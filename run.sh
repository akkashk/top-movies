#!/bin/bash
if [ "$#" -ne 3 ]; then
    echo "Script expects exactly three parameters: <path_to_IMDB.zip> <path_to_wiki.gz> <connection_uri>"
    exit 1
fi


OUT_DIR="topmovies_inputs"
mkdir $OUT_DIR

echo "Unzipping IMDB dataset"
unzip "$1" -d $OUT_DIR

echo "Unzipping Wikipedia dataset"
gunzip -dkc "$2" > $OUT_DIR/wiki.xml

echo "Calculating ratio"
./top-movies ratio $OUT_DIR/movies_metadata.csv

echo "Matching movies"
./top-movies match $OUT_DIR/wiki.xml $OUT_DIR/movies_metadata.csv $OUT_DIR/credits.csv

echo "Combining data"
./top-movies combine $OUT_DIR/movies_metadata.csv output_ratio.csv output_matching.csv $OUT_DIR/ratings.csv

echo "Loading to Postgres"
./top-movies load output_combine.csv "$3"

echo "Cleaning up"
rm output_ratio.csv output_matching.csv output_combine.csv
rm -rf $OUT_DIR