#!/bin/bash

usage="$0 <spider_name> <output filename> <output filetype>\n"
example="$0 cisco cisco.csv csv\n"

#[[ $# -ne 3 ]] && echo -e "\n$usage\n$example\n" && exit 1
spider_name="$1"
file="$2"
file_type="$3"
properties_file="$4"

# Source the property file for pre-defined variables

[[ ! -s "$properties_file" ]] && echo -e "\nError: scrapy.properties file does not exist. Exiting...\n" && exit 1
. "$properties_file"

export PYTHONPATH="${PYTHONPATH}:${BLZ_PROJ_DIR}/"

spider_location="${BLZ_BOT_DIR}/${spider_name}/src"
log_file="${BLZ_LOG_DIR}/${spider_name}.log"

[[ ! -d $spider_location ]] && echo -e "\nError: Spider directory $spider_location does not exist. Exiting...\n" && exit 1

cd $spider_location

# parameters after -a are user-defined (not scrapy native), these are being used by Blazent's implementation BlzDataExporter pipeline
scrapy crawl $spider_name --logfile "$log_file" --loglevel "$BLZ_LOG_LEVEL" \
                          -a DATA_OUTPUT_DIR="$BLZ_DATA_OUTPUT_DIR" \
                          -a DATA_FORMAT="$file_type" \
                          -a ERROR_OUTPUT_DIR="$BLZ_ERROR_OUTPUT_DIR" \
                          -a FILE_TIMESTAMP_FORMAT="$FILE_TIMESTAMP_FORMAT"
