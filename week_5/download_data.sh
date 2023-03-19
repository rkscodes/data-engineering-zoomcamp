set -e

TAXI_TYPE=$1
YEAR=$2


URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for MONTH in {1..12}; do
    # echo $MONTH
    FMONTH=$(printf "%02d" ${MONTH})
    # echo $FMONTH
    URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
    echo $URL
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    echo "downloading $URL to $LOCAL_PATH"
    mkdir -p ${LOCAL_PREFIX}
    wget $URL -O ${LOCAL_PATH}
done