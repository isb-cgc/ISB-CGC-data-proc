"""Script to do a quick describe on the dataframe 
    Dataframe created by the results from the query to Biquery
"""

import argparse
import pandas as pd
import numpy as np
from pandas import ExcelWriter
from bigquery_etl.utils import convert_gbq_to_df

def main(project_id, dataset_id, table_names):

    # create a excel writer instance
    writer = ExcelWriter('bq_sanity_check_reports.xlsx')

    for table_name in table_names.split(";"):
        query = 'SELECT * FROM {0}.{1}'.format(dataset_id, table_name)
        
        df = convert_gbq_to_df.run(project_id, query)
    
        #Summarizing data
        df_stats = df.describe(include='all').transpose()
    
        # write to an excel sheet
        df_stats.to_excel(writer, sheet_name=table_name)
        writer.save()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project_id', help='Your Google Cloud project ID.')
    parser.add_argument('dataset_id', help='BigQuery Dataset ID.')
    parser.add_argument('table_names', 
         help='BigQuery Table Names. Multiple table names must be semi-colon separated.')

    args = parser.parse_args()
    main(
        args.project_id,
        args.dataset_id,
        args.table_names
        )

