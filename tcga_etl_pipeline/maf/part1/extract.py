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

def search_files(config):

    # project id
    project_id = config['project_id']
    # bucket name 
    bucket_name = config['buckets']['open']
   
    # connect to google cloud storage 
    gcs = GcsConnector(project_id, bucket_name)

    #---------------------    
    # search for MAF files
    #---------------------
    maf_file = re.compile("^.*.maf$")
    # search only these tumor types defined in config files
    search_tumors = ["tcga/" + d.lower() for d in config['all_tumor_types']]
    data_library = gcs.search_files(search_patterns=['.maf'], regex_search_pattern=maf_file, prefixes=search_tumors)
    data_library['basefilename'] = map(lambda x: os.path.splitext(os.path.basename(x))[0], data_library['filename']) 
    data_library['unique_filename'] = format_dupe_values(data_library['basefilename'])

    return data_library

if __name__ == '__main__':
    print search_files(json.load(open(sys.argv[1])))

