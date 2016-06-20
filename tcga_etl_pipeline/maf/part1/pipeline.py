#!/usr/bin/env python

# Copyright 2015, Institute for Systems Biology.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Script to parse MAF file
"""
from bigquery_etl.extract.utils import convert_file_to_dataframe
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
from bigquery_etl.transform.tools import *
import json
from gcloud import storage
from bigquery_etl.execution import process_manager
import os.path
from pandas import ExcelWriter
import extract
import time
import transform
import sys
from bigquery_etl.utils.logging_manager import configure_logging

#----------------------------------
# Config
#----------------------------------
# parse the config file
config = json.load(open(sys.argv[1]))
project_id = config['project_id']
bucket_name = config['buckets']['open']

# get required headers/columns from the file
# we need to preserve the order too
# IMPORTANT: notice the lowercase column heading names.
#   Through out the script we use lowecase heading names
oncotator_columns = [line.rstrip('\n').lower() for line in config['maf']['oncotator_input_columns']]
oncotator_input_files_dest = config['maf']['oncotator_input_files_dest']

#-----------------------------------
# Extract
#----------------------------------- 
data_library = extract.search_files(config)
# log all files found
writer = ExcelWriter('maf_part1.log.xlsx')
data_library.to_excel(writer, "maf_files")
writer.save()

#-----------------------------------------------------
# Execution 
#------------------------------------------------------
log_filename = 'etl_maf_part1.log'
log_name = 'etl_maf_part1.log'
log = configure_logging(log_name, log_filename)
log.info('start maf part1 pipeline')
pm = process_manager.ProcessManager(max_workers=20, db='maf1.db', table='task_queue_status', log=log)
for index, row in data_library.iterrows():
    inputfilename = row['filename']
    outputfilename =  oncotator_input_files_dest + row['unique_filename'].replace(".maf", ".txt")
    # transform
    future = pm.submit(transform.generate_oncotator_inputfiles, project_id, bucket_name, inputfilename, outputfilename, oncotator_columns)
    time.sleep(0.2)
pm.start()
log.info('finished maf part1 pipeline')

