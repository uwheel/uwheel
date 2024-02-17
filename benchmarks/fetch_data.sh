#!/bin/bash

mkdir -p data

urls=("https://s3.amazonaws.com/tripdata/201808-citibike-tripdata.csv.zip" "https://s3.amazonaws.com/tripdata/201809-citibike-tripdata.csv.zip" "https://s3.amazonaws.com/tripdata/201810-citibike-tripdata.csv.zip" "https://s3.amazonaws.com/tripdata/201811-citibike-tripdata.csv.zip" "https://s3.amazonaws.com/tripdata/201812-citibike-tripdata.csv.zip")

for url in "${urls[@]}"; do
    file=`basename "$url"`
    wget "$url" -O data/"$file"
    unzip data/"$file" -d data/
    rm data/"$file"
done

output_file="data/citibike-tripdata.csv"
input_files=("data/201808-citibike-tripdata.csv" "data/201809-citibike-tripdata.csv" "data/201810-citibike-tripdata.csv" "data/201811-citibike-tripdata.csv" "data/201812-citibike-tripdata.csv")

touch data/citibike-tripdata.csv

head -n 1 "${input_files[0]}" > "$output_file"


for input_file in "${input_files[@]}"; do
    tail -n +2 "$input_file" >> "$output_file"
done

echo "Combined citibike trip CSV files into '$output_file'"

## DEBS12

echo "Preparing DEBS12 Dataset"

debs_url="https://zenodo.org/records/8120787/files/DEBS-Data.tar.gz"
wget "$debs_url" -P data/
tar -xvf data/DEBS-Data.tar.gz -C data/
rm data/DEBS-Data.tar.gz
mv data/DEBS-Data/DEBS2012-ChallengeData/allData.txt data/debs12.csv
echo "Finished preparing DEBS12 Dataset"