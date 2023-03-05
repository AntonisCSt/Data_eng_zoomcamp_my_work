set -e

BASIC_ADRESS="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

TAXI_TYPE=$1 #"yellow"
YEAR=$2 #2020

for MONTH in {1..12..1}
do
  FMONTH=`printf "%02d" ${MONTH}`
  URL="${BASIC_ADRESS}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}
done
