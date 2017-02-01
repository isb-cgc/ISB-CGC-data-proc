'''
Created on Jan 18, 2017
does post processing for the TCGA_metadata_clinical nad TCGA_metadata_biospeicmen.
this involves copying over the columns dervied from the bio xml file elements

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
from gdc.model.isbcgc_cloudsql_tcga_model import ISBCGC_database_helper
from util import order4insert

def postprocess(config, project_name, endpt_type, log):
        log.info('\tstart postprocess clinical for %s' % (project_name))
        postproc_config = config['TCGA']['process_cases']['postproc_case']
        cloudsql_table = postproc_config['postproc_cloudsql_table']
        update_cloudsql_from_bigquery(config, postproc_config, project_name, cloudsql_table, log)
        log.info('\tdone postprocess clinical for %s' % (project_name))

        log.info('\tstart postprocess biospecimen for %s' % (project_name))
        postproc_config = config['TCGA']['process_cases']['postproc_biospecimen']
        cloudsql_table = postproc_config['postproc_cloudsql_table']
        update_cloudsql_from_bigquery(config, postproc_config, project_name, cloudsql_table, log)
        log.info('\tdone postprocess biospecimen for %s' % (project_name))

        log.info('\tstart populating samples for %s' % (project_name))
        stmts = config['TCGA']['populate_samples']['stmts']
        for stmt in stmts:
            ISBCGC_database_helper.update(config, stmt % (project_name, endpt_type), log, [[]])
        log.info('\tdone populating samples for %s' % (project_name))

        log.info('\tstart populating attrs for %s' % (project_name))
        attrrows = config['TCGA']['populate_samples']['attr_rows']
        dbrows = order4insert(config['TCGA']['populate_samples']['attr_order'], ISBCGC_database_helper.field_names('TCGA_metadata_attrs'), attrrows)
        ISBCGC_database_helper.column_insert(config, dbrows, 'TCGA_metadata_attrs', ISBCGC_database_helper.field_names('TCGA_metadata_attrs'), log)
        log.info('\tdone populating attrs for %s' % (project_name))
