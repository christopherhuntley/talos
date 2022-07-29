#!/usr/bin/env python
# coding: utf-8

# # IRS 990 Data ETL
# **This notebook includes code needed to populate a data lake with IRS 990 data. Note: it rebuilds the data lake from scratch each time.**
# 
# ## Bulk Downloads from IRS
# 
# This is a short BASH script to download all 990 xml files to date. 
# - Downloads are incremental, with "no clobber" settings used to avoid redundant downloads
# - The `$CYEAR` variable represents the current year in XXXX format 
# - The `$DEST` variable is a directory path for storing the files
# - TODO: Allow overrides from environment variables

# In[ ]:


get_ipython().run_cell_magic('bash', '', 'CYEAR=`date +"%Y"`\nDEST=./990data/raw/\nfor y in $(seq 2015 $CYEAR); do\n    PART=1\n    COMPLETE=0\n    until [ $COMPLETE -eq 1 ]; do\n        URL="https://apps.irs.gov/pub/epostcard/990/xml/${y}/download990xml_${y}_${PART}.zip"\n        wget -q -N $URL -P $DEST\n        if head $DEST/download990xml_${y}_${PART}.zip | grep -q html; then\n            COMPLETE=1\n            rm -f $DEST/download990xml_${y}_${PART}.zip\n        else\n            echo "Updating/Downloading $URL"\n        fi\n        ((PART++))\n    done\ndone\n')


# ## Data ETL
# 
# This is Python code to 
# - extract (parse) data from zipped-XML files; the output is a data tree (`tree_data`)
# - transform the data to suit various potential uses and schema (tabular, hierarchical, etc.) 
# - load (export) the data to destinations by file format and year(csv files, json files, relational DBs)
# 
# The process is somewhat monolithic:
# - rebuilds the entire lake from scratch
# - handles all data formats, one at a time

# In[ ]:


from pathlib import Path
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import csv
import json

# -------------- Constants / Environment Vars-----------------------------
# Namespace for XML parsing
ns = {'':'http://www.irs.gov/efile'}

# Locations of the data directories
data_dir = Path('./990data')
raw_zips_dir = data_dir / 'raw'
json_dir = data_dir / 'json'
csv_dir = data_dir / 'csv'
tmp_dir = data_dir / 'tmp'

# -------------------- Utilities -----------------------------------------

def safe_text(elements):
    
    '''A text extraction utility for ElementTree(s)''' 
    
    if elements is None:
        return ''
    elif type(elements) == ET.Element: 
        # singular
        return elements.text
    else:
        # plural
        return ' '.join([e.text for e in elements]) 

def coalesce(*values):
    
    """Return the first non-None value or None if all values are None"""
    # src: https://gist.githubusercontent.com/onelharrison/37a1d153246a5e1d2336c444f05522a6/raw/3075d414eee9d45a6558513bcd33f4a266b57e77/coalesce.py
    
    return next((v for v in values if v is not None), None)

# ----------- XML Parser functions for various parts of the return --------------- 
        
def parse_return(root,fname):
    
    '''Packages an XML return's header data as a dictionary'''
    
    fields = {}
    
    fields['src_fname'] = fname
    fields['return_type'] = coalesce(
        safe_text(root.find('.//ReturnTypeCd',ns)),
        safe_text(root.find('.//ReturnType',ns))
    )
    fields['ein'] = safe_text(root.find('.//Filer/EIN',ns))
    fields['business_name'] = coalesce(
        safe_text(root.find('.//Filer/BusinessName/*',ns)),
        safe_text(root.find('.//Filer/Name/*',ns))
    )
    fields['business_address'] = safe_text(root.findall('.//Filer/USAddress/*',ns))
    fields['preparer_firm'] = coalesce(
        safe_text(root.find('.//PreparerFirmName/*',ns)),
        safe_text(root.find('.//PreparerFirmBusinessName/*',ns))
    )
    fields['preparer_address'] = coalesce(
        safe_text(root.findall('.//PreparerUSAddress/*',ns)),
        safe_text(root.findall('.//PreparerFirmUSAddress/*',ns))
    )
    
    fields['tax_year']= coalesce(
        safe_text(root.find('.//TaxYr',ns)), 
        safe_text(root.find('.//TaxYear',ns))
    )
    return fields

def parse_officers(root,fname):
    
    '''Packages data about company officers as a list of dictionaries'''
    
    records = []
    
    # officers listed in data
    officers = root.findall('.//OfficerDirTrstKeyEmplGrp',ns)
    officers += root.findall('.//OfficerDirectorTrusteeEmplGrp',ns)
    officers += root.findall('.//OfcrDirTrusteesOrKeyEmployee',ns)
    officers += root.findall('.//Form990PartVIISectionAGrp',ns)
    
    for officer in officers:
        fields = {
            'name': coalesce(
                safe_text(officer.find('PersonNm',ns)),
                safe_text(officer.findall('.//BusinessName/*',ns)),
                safe_text(officer.find('PersonName',ns))
            ),
            'title': coalesce(
                safe_text(officer.find('TitleTxt',ns)),
                safe_text(officer.find('Title',ns))
            ),
            'address': safe_text(officer.findall('.//USAddress/*',ns))
        }
        records += [fields]
    
    # officer listed in header; only use if no other records found
    if not records:
        officer = root.find('.//BusinessOfficerGrp',ns)
        
        if officer == None:
            officer = root.find('.//Officer',ns)
            
        fields = {
            'name': coalesce(
                safe_text(officer.find('PersonNm',ns)),
                safe_text(officer.find('Name',ns))
            ),
            'title': coalesce(
                safe_text(officer.find('PersonTitleTxt',ns)),
                safe_text(officer.find('Title',ns))
            ),
            'address': ''
        }
        records += [fields]
    
    return records

def parse_grants(root,fname):
    
    '''Packages data about grants and contributions as a list of dictionaries'''
    
    records = []
    
    grants = root.findall('.//GrantOrContributionPdDurYrGrp',ns)
    for grant in grants:
        fields = {
            'recipient_name': coalesce(
                safe_text(grant.find('RecipientPersonNm',ns)), 
                safe_text(grant.findall('.//RecipientBusinessName/*',ns))
            ),
            'recipient_address': safe_text(grant.findall('.//RecipientUSAddress/*',ns)),
            'purpose': safe_text(grant.find('.//GrantOrContributionPurposeTxt',ns)),
            'amount': safe_text(grant.find('Amt',ns))
        }
        records += [fields]
        
    return records


def return_tree(root,fname): 
    tree = parse_return(root,fname)
    tree['officers'] = parse_officers(root,fname)
    tree['grants'] = parse_grants(root,fname)
    
    return tree

# ----------- EXPORT Functions for various formats ---------------------

def export_to_csv(data_tree,year,part,zipmode="w"):
    
    '''Exports a data tree to keyed csv files'''
    
    returns = list(data_tree)
    officers = []
    grants = []
    for r in returns:
        for o in r['officers']:
            o['src_fname'] = r['src_fname'] 
            officers += [o]
        
        for g in r['grants']:
            g['src_fname'] = r['src_fname'] 
            grants += [g]
            
    # generate returns.csv
    df = pd.DataFrame(returns)
    del df['officers']
    del df['grants']
    df.to_csv("tmp/returns.csv",mode="w",index=False)
    
    # generate officers.csv
    df = pd.DataFrame(officers)
    df.to_csv("tmp/officers.csv",mode="w",index=False)
    
    # generate grants.csv
    df = pd.DataFrame(grants)
    df.to_csv("tmp/grants.csv",mode="w",index=False)
    
    # export to zipfile
    fname = "IRS990_csv_" + str(year)+ "_part_"+ str(part) + ".zip"
    with zipfile.ZipFile(csv_dir / fname, mode=zipmode, compression = zipfile.ZIP_DEFLATED) as outzip:
        outzip.write("tmp/returns.csv")
        outzip.write("tmp/officers.csv")
        outzip.write("tmp/grants.csv")
    

def export_to_json(data_tree,year,part, zipmode="w"):
    
    '''Exports a data tree as a json file'''
    
    with open("tmp/returns.json",'w') as jf:
        json.dump(data_tree,jf)
    
    fname = "IRS990_json_" + str(year)+ "_part_"+ str(part) + ".zip"
    with zipfile.ZipFile(json_dir / fname, mode=zipmode, compression = zipfile.ZIP_DEFLATED) as outzip:
        outzip.write("tmp/returns.json")
   

# -------------------- Main code --------------------------------
# process each zipped xml file in the raw downloads directory
for zf_path in sorted(list(raw_zips_dir.glob('./*.zip'))):
    
    # extract the year from the zf_path
    year = zf_path.name.split("_")[1]
    part = zf_path.name.split("_")[2].split(".")[0]
 
    print(zf_path, year)
    
    zf = zipfile.ZipFile(zf_path, 'r')
    
    # processes each xml document in the zip; builds up a data tree
    data_tree = []
    for i,fname in enumerate(zf.namelist()):
        if (i % 1000 == 0):
            print(i, fname)
        
        # extract the xml into a live ET tree
        with zf.open(fname) as f:
            xml_tree = ET.parse(f)
            xml_root = xml_tree.getroot()

            data_tree += [return_tree(xml_root,fname)]
    
    # exports
    export_to_csv(data_tree, year,part)
    export_to_json(data_tree, year,part)
      
    
        
        
        
        


# In[ ]:





# In[ ]:




