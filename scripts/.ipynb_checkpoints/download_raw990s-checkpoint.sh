CYEAR=`date +"%Y"`
DEST=../990data/raw/
for y in $(seq 2015 $CYEAR); do
    PART=1
    COMPLETE=0
    until [ $COMPLETE -eq 1 ]; do
        URL="https://apps.irs.gov/pub/epostcard/990/xml/${y}/download990xml_${y}_${PART}.zip"
        wget -N $URL -P $DEST
        if head $DEST/download990xml_${y}_${PART}.zip | grep -q html; then
            COMPLETE=1
            rm -f $DEST/download990xml_${y}_${PART}.zip
        fi
        ((PART++))
    done
done


