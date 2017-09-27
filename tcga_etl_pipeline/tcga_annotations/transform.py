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

"""Script to parse annotations file
https://tcga-data.nci.nih.gov/annotations/resources/searchannotations/json
To run: python load.py config_file
"""

from urllib2 import Request, urlopen
import json
from bigquery_etl.extract.gcloud_wrapper import GcsConnector
import sys
from bigquery_etl.transform.tools import normalize_json, cleanup_dataframe
import pandas as pd

def parse_annotations(config):
    """
    Download using annotation file using urllib2.Request
    Convert the file into a dataframe
    """
   # connect to google.cloud wrapper
    project_id = config['project_id']
    bucket_name = config['buckets']['open']
    gcs = GcsConnector(project_id, bucket_name)

    # use Request to get the json data
    print 'Downloading the TCGA annotations file'
    request = Request(config['tcga_annotations']['input_file'])
    response = urlopen(request)
    elevations = response.read()
    data = json.loads(elevations)

    all_annotations = []
    for annotation in data['dccAnnotation']:
        annotation = normalize_json(annotation)
        if annotation["items_item"].startswith('TCGA-') and len(annotation["items_item"]) >= 12:
            item = annotation["items_item"]
            annotation['ParticipantBarcode'] = "-".join(item.split("-")[0:3])
            if (len(annotation["items_item"]) > 12 and
                    annotation["items_itemType_itemTypeName"] != "Patient"):
                annotation['SampleBarcode'] = "-".join(item.split("-")[0:4])
        if annotation["items_itemType_itemTypeName"] == "Aliquot":
            aliquot = annotation["items_item"]
            if aliquot.startswith('TCGA-') and len(aliquot) == 28:
                annotation['AliquotBarcode'] = aliquot

        all_annotations.append(annotation)

    #------------
    # transform
    #------------
    data_df = pd.DataFrame(all_annotations)
    # clean up the dataframe to upload to BigQuery
    data_df = cleanup_dataframe(data_df)
    # rename columns
    data_df = rename_columns(data_df)
    print data_df

    #----------
    # upload
    #----------
    # upload the contents of the dataframe in njson format
    print "Uploading the njson file"
    status = gcs.convert_df_to_njson_and_upload(data_df, config['tcga_annotations']['output_file'])
    print status

def rename_columns(data_df):
    """
    Rename column names
    """
    replace_column_names = {
        'annotationCategory_annotationClassification_annotationClassificationName':
            'annotationClassification',
        'annotationCategory_categoryId': 'annotationCategoryId',
        'annotationCategory_categoryName': 'annotationCategoryName',
        'id': 'annotationId',
        'items_disease_abbreviation':  'Study',
        'items_item': 'itemBarcode',
        'items_itemType_itemTypeName': 'itemTypeName',
        'notes_noteText': 'annotationNoteText',
        'notes_dateAdded': 'dateAdded',
        'notes_dateEdited': 'dateEdited'
    }
    data_df.columns = [replace_column_names[x] if x in replace_column_names\
                                               else x for x in data_df.columns]
    return data_df


if __name__ == '__main__':
    parse_annotations(json.load(open(sys.argv[1])))


