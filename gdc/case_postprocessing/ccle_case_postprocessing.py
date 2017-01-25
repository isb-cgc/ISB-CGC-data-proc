'''
Created on Jan 18, 2017
does post processing for the CCLE_metadata_clinical nad CCLE_metadata_biospeicmen.
this involves copying over the columns dervied from the CCLE metadata spreadsheet

Copyright 2015, Institute for Systems Biology.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

@author: michael
'''
from gdc.util.gdc_util import update_cloudsql_from_bigquery

def postprocess(config, project_name, log):
    try:
        log.info('\tstart postprocess for %s' % (project_name))
        postproc_config = config['CCLE']['process_cases']['postproc_case']
        cloudsql_table = postproc_config['postproc_cloudsql_table']
        update_cloudsql_from_bigquery(config, postproc_config, project_name, cloudsql_table, log)
        log.info('\tdone postprocess for %s' % (project_name))
    except:
        log.exception('problem postprocessing %s' % (project_name))

