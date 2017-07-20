'''
a wrapper to google cloud sql.

the MySQLdb module is thread safe but the connections to the database are not.  so the
recommendation is that each thread have an independent connection.  currently, each
database access will use its own connection and at the end of the method, close it.
if this becomes expensive, timewise, a mapping of thread to connection can be utilized.

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
'''
from collections import OrderedDict

import isbcgc_cloudsql_model
import gdc.model.isbcgc_cloudsql_annotation_model
import gdc.model.isbcgc_cloudsql_ccle_model
import gdc.model.isbcgc_cloudsql_target_model
import gdc.model.isbcgc_cloudsql_tcga_model

class ISBCGC_database_helper(isbcgc_cloudsql_model.ISBCGC_database_helper):
    """
    this class manages the cloud sql metadata upload
    """
    metadata_program = {
        'table_name': 'metadata_program',
        'primary_key_name': 'metadata_program_id',
        'columns': [
            ['program_gdc_id', 'VARCHAR(36)', 'NOT NULL'],
            ['program_name', 'VARCHAR(80)', 'NOT NULL'],
            ['dbgap_accession_number', 'VARCHAR(9)', 'NOT NULL'],
            ['endpoint_type', 'VARCHAR(8)', 'NOT NULL']
        ],
#         'natural_key_cols': [
#             'case_barcode'
#         ],
        'indices_defs': [
            ['program_gdc_id'],
            ['program_name']
        ]
    }
    
    metadata_tables = OrderedDict(
        [
            ('metadata_program', metadata_program)
        ]
    )

    @classmethod
    def initialize(cls, config, log):
        cls.metadata_tables.update(gdc.model.isbcgc_cloudsql_tcga_model.ISBCGC_database_helper.metadata_tables)
        cls.metadata_tables.update(gdc.model.isbcgc_cloudsql_target_model.ISBCGC_database_helper.metadata_tables)
        cls.metadata_tables.update(gdc.model.isbcgc_cloudsql_ccle_model.ISBCGC_database_helper.metadata_tables)
        cls.metadata_tables.update(gdc.model.isbcgc_cloudsql_annotation_model.ISBCGC_database_helper.metadata_tables)
        cls.setup_tables(config, log)

