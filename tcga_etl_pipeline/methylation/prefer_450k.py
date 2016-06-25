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
import json
import sys
import methylation.extract
from bigquery_etl.extract.gcloud_wrapper import GcsConnector

def main(configfilename):
    """
    Prefer 450K files over 270k (delete transformed 270K files)
    """
    with open(configfilename) as configfile:
        config = json.load(configfile)

    print '*'*30 + "\nExtract - methylation"
    data_library = methylation.extract.identify_data(config)
    hiseq_aliquots = data_library.query('Platform == "HumanMethylation450"')['AliquotBarcode'].drop_duplicates().values.tolist()
    IlluminaGA_df = data_library.query('Platform == "HumanMethylation27"')
    IlluminaHiSeq_GA_df =  IlluminaGA_df[ (IlluminaGA_df['AliquotBarcode'].isin(hiseq_aliquots))]

    project_id = config['project_id']
    bucket_name = config['buckets']['open']
    gcs = GcsConnector(project_id, bucket_name)
    for _, row in IlluminaHiSeq_GA_df.iterrows():
        blob = row.to_dict()['OutDatafileNameKey']
        gcs.delete_blob(blob)

if __name__ == '__main__':
    main(sys.argv[1])
