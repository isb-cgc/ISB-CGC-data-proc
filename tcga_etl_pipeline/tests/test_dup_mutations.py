'''
Created on May 17, 2016

class to test removing duplicate mutations in maf files.  expects to use the files_test.txt as input and the
two test files:
    tests/test_files/pancan.merged.maf.filters_and_centersTCGA-GL-A9DD-01Aonly.oncotator_output: has 167 rows
        all based on tumor sample TCGA-GL-A9DD-01.  there are 127 base rows with tumor aliquot TCGA-GL-A9DD-01A-11D-A36X-10
        and two normal aliquots: one for blood, TCGA-GL-A9DD-10A-01D-A370-10, with 80 rows and one for tissue, 
        TCGA-GL-A9DD-11A-01D-A370-10, with 47 rows.  the tissue sample rows should bve removed in favor of the blood.
        an additioanl 20 rows are based on tumor aliquot TCGA-GL-A9DD-10A-01W-A370-10, these should be removed in favor 
        of the TCGA-GL-A9DD-10A-01D-A370-10.
        an additional 20 rows are based on normal aliquots TCGA-GL-A9DD-10A-01G-A370-10 or TCGA-GL-A9DD-11A-01G-A370-10.
        these should similarly be removed.  the test results should show the following:
            [INFO 2016-06-09 09:15:23,619 test_maf_part2.log]      calling check_duplicates to collapse aliquots with 167 rows
            [INFO 2016-06-09 09:15:23,619 test_maf_part2.log]          check for duplicate tumor barcodes.  dataframe size: 167
            [INFO 2016-06-09 09:15:23,619 test_maf_part2.log]          finished check for duplicate tumor barcodes.  dataframe size: 147
            [INFO 2016-06-09 09:15:23,619 test_maf_part2.log]          check for duplicate normal barcodes.  dataframe size: 147
            [INFO 2016-06-09 09:15:23,621 test_maf_part2.log]          finished check for duplicate normal barcodes.  dataframe size: 127
            [INFO 2016-06-09 09:15:23,621 test_maf_part2.log]          check for paired blood/tissue normal barcodes.  dataframe size: 127
            [INFO 2016-06-09 09:15:23,621 test_maf_part2.log]          finished check for paired blood/tissue normal barcodes.  dataframe size: 80
            [INFO 2016-06-09 09:15:23,621 test_maf_part2.log]      finished check_duplicates to collapse aliquots with 80 rows
            [INFO 2016-06-09 09:15:23,621 test_maf_part2.log]      consolidate the centers for duplicate mutations into list
            [INFO 2016-06-09 09:15:23,730 test_maf_part2.log]      finished consolidating centers for duplicate mutations
            [INFO 2016-06-09 09:15:23,730 test_maf_part2.log]      calling remove_duplicates to collapse mutations with 80 rows
            [INFO 2016-06-09 09:15:23,746 test_maf_part2.log]      finished remove_duplicates to collapse mutations with 80 rows
            [INFO 2016-06-09 09:15:23,746 test_maf_part2.log]  writing 80 rows
        
    tests/test_files/pancan.merged.maf.filters_and_centersTCGA-GL-A9DD-01A.oncotator_output: 
        has all the rows of the above file in additional to several additional rows.  the tests results should show the following
            [INFO 2016-06-09 08:26:48,040 test_maf_part2.log]      calling check_duplicates to collapse aliquots with 2040 rows
            [INFO 2016-06-09 08:26:48,040 test_maf_part2.log]          check for duplicate tumor barcodes.  dataframe size: 2040
            [INFO 2016-06-09 08:26:48,056 test_maf_part2.log]          finished check for duplicate tumor barcodes.  dataframe size: 2020
            [INFO 2016-06-09 08:26:48,056 test_maf_part2.log]          check for duplicate normal barcodes.  dataframe size: 2020
            [INFO 2016-06-09 08:26:48,104 test_maf_part2.log]          finished check for duplicate normal barcodes.  dataframe size: 2000
            [INFO 2016-06-09 08:26:48,104 test_maf_part2.log]          check for paired blood/tissue normal barcodes.  dataframe size: 2000
            [INFO 2016-06-09 08:26:48,151 test_maf_part2.log]          finished check for paired blood/tissue normal barcodes.  dataframe size: 1410
            [INFO 2016-06-09 08:26:48,151 test_maf_part2.log]      finished check_duplicates to collapse aliquots with 1410 rows
            [INFO 2016-06-09 08:26:48,151 test_maf_part2.log]      consolidate the centers for duplicate mutations into list
            [INFO 2016-06-09 08:26:48,915 test_maf_part2.log]      finished consolidating centers for duplicate mutations
            [INFO 2016-06-09 08:26:48,915 test_maf_part2.log]      calling remove_duplicates to collapse mutations with 1410 rows
            [INFO 2016-06-09 08:26:48,937 test_maf_part2.log]      finished remove_duplicates to collapse mutations with 1410 rows
            [INFO 2016-06-09 08:26:48,937 test_maf_part2.log]  writing 1410 rows

    running both together should produce the following output:
            [INFO 2016-06-09 09:38:45,355 test_maf_part2.log]      calling check_duplicates to collapse aliquots with 2207 rows
            [INFO 2016-06-09 09:38:45,355 test_maf_part2.log]          check for duplicate tumor barcodes.  dataframe size: 2207
            [INFO 2016-06-09 09:38:45,371 test_maf_part2.log]          finished check for duplicate tumor barcodes.  dataframe size: 2167
            [INFO 2016-06-09 09:38:45,387 test_maf_part2.log]          check for duplicate normal barcodes.  dataframe size: 2167
            [INFO 2016-06-09 09:38:45,417 test_maf_part2.log]          finished check for duplicate normal barcodes.  dataframe size: 2127
            [INFO 2016-06-09 09:38:45,417 test_maf_part2.log]          check for paired blood/tissue normal barcodes.  dataframe size: 2127
            [INFO 2016-06-09 09:38:45,464 test_maf_part2.log]          finished check for paired blood/tissue normal barcodes.  dataframe size: 1490
            [INFO 2016-06-09 09:38:45,464 test_maf_part2.log]      finished check_duplicates to collapse aliquots with 1490 rows
            [INFO 2016-06-09 09:38:45,464 test_maf_part2.log]      consolidate the centers for duplicate mutations into list
                ... (logging lines for centers)
            [INFO 2016-06-09 09:38:46,292 test_maf_part2.log]      finished consolidating centers for duplicate mutations
            [INFO 2016-06-09 09:38:46,292 test_maf_part2.log]      calling remove_duplicates to collapse mutations with 1490 rows
            [DEBUG 2016-06-09 09:38:46,292 bigquery_etl.transform.tools]  Found duplicate rows
            [DEBUG 2016-06-09 09:38:46,292 bigquery_etl.transform.tools]  Deleted
            [INFO 2016-06-09 09:38:46,325 test_maf_part2.log]      finished remove_duplicates to collapse mutations with 1410 rows
            [INFO 2016-06-09 09:38:46,325 test_maf_part2.log]  writing 1410 rows

@author: michael
'''
#### -*- coding: utf-8 -*-
import sys
import time
import pandas as pd
import json
from cStringIO import StringIO
import check_duplicates
from bigquery_etl.utils.logging_manager import configure_logging
from bigquery_etl.extract.utils import convert_file_to_dataframe
from bigquery_etl.transform.tools import cleanup_dataframe, remove_duplicates
from bigquery_etl.execution import process_manager

log_filename = 'test_maf_part2.log'
log_name = 'test_maf_part2.log'
log = configure_logging(log_name, log_filename)
#--------------------------------------
# Format Oncotator output before merging
#--------------------------------------
def format_oncotator_columns(df):
    
    df.columns = map(lambda x: x.replace('1000', '_1000'), df.columns)
    df.columns = map(lambda x: x.replace('gencode', 'GENCODE'), df.columns)
    df.columns = map(lambda x: '_'.join([''.join(i[0].upper() + i[1:]) for i in x.split('_')]), df.columns)

    # adjust columns
    replace_columns = { 
         'Tumor_Sample_Barcode' : 'Tumor_AliquotBarcode'
        ,'Matched_Norm_Sample_Barcode' : 'Normal_AliquotBarcode'
        ,'Match_Norm_Seq_Allele1' : 'Normal_Seq_Allele1'
        ,'Match_Norm_Seq_Allele2' : 'Normal_Seq_Allele2'  
        ,'Match_Norm_Validation_Allele1' : 'Normal_Validation_Allele1'
        ,'Match_Norm_Validation_Allele2' : 'Normal_Validation_Allele2'
        ,'Gc_Content' : 'GC_Content'
        ,'CDNA_Change' : 'cDNA_Change'
    }


    for i in replace_columns:
        df.columns = map(lambda x: x.replace(i, replace_columns[i]), df.columns)

    return df

#------------------------------------------
# this adds news columns and does a few checks on the columns
#------------------------------------------
def add_columns(df, sample_code2letter, study):
    ## Add new columns
    df['Tumor_SampleBarcode'] = df['Tumor_AliquotBarcode'].map(lambda x: '-'.join(x.split('-')[0:4]))
    df['Tumor_ParticipantBarcode'] = df['Tumor_AliquotBarcode'].map(lambda x: '-'.join(x.split('-')[0:3]))
    df['Tumor_SampleTypeLetterCode'] = df['Tumor_AliquotBarcode'].map(lambda x: sample_code2letter[x.split('-')[3][0:2]])

    df['Normal_SampleBarcode'] = df['Normal_AliquotBarcode'].map(lambda x: '-'.join(x.split('-')[0:4]))
    df['Normal_ParticipantBarcode'] = df['Normal_AliquotBarcode'].map(lambda x: '-'.join(x.split('-')[0:3]))
    df['Normal_SampleTypeLetterCode'] = df['Normal_AliquotBarcode'].map(lambda x: sample_code2letter[x.split('-')[3][0:2]])
    df['Center'] = df['Center'].map(lambda x: ';'.join(sorted(x.split(';'))) if ';' in x else x)
    df['Study'] = study
    df['NCBI_Build'] = 37

    # ---------------------------------------------
    # Checks
    # ---------------------------------------------

    # check patient_id
    tumor_patient_id_bool = (df['Tumor_ParticipantBarcode'] == df['Normal_ParticipantBarcode'])
    df = df[tumor_patient_id_bool]
    if not df[~tumor_patient_id_bool].empty:
       #print df[~tumor_patient_id_bool].to_dict()
       log.error('ERROR: did not find all tumors paired with normal samples')
       raise ValueError('ERROR: did not find all tumors paired with normal samples')

    # tumor barcode 14th character must be 0
    tumor_sample_codes = map(lambda x: x.split('-')[3][0], df['Tumor_AliquotBarcode'])
    if '0' not in tumor_sample_codes and len(tumor_sample_codes) > 0:
       log.error('ERROR: tumor barcode 14th character must be 0')
       raise ValueError('ERROR: tumor barcode 14th character must be 0')

    # normal barcode 14th character must be 1
    norm_sample_codes = map(lambda x: x.split('-')[3][0], df['Normal_AliquotBarcode'])
    if '1' not in norm_sample_codes  and len(norm_sample_codes) > 0:
       log.error('ERROR: normal barcode 14th character must be 1')
       raise ValueError('ERROR: normal barcode 14th character must be 1')

    df['ParticipantBarcode'] = df['Tumor_ParticipantBarcode']
    del df['Tumor_ParticipantBarcode']
    del df['Normal_ParticipantBarcode']

    return df



#----------------------------------------
# this is the main function to process oncotator MAF files
# 1. this merges the different files by disease type
# 2. selects the columns to load into BigQuery
# 3. format the oncotator columns
# 4. adds new columns
# 5. removes any duplicate aliqouts
#----------------------------------------
def process_oncotator_output(project_id, bucket_name, data_library, bq_columns, sample_code2letter, oncotator_object_path):
    study = data_library['Study'].iloc[0]

    # this needed to stop pandas from converting them to FLOAT
    dtype = {
        "Transcript_Exon" : "object"
       ,"NCBI_Build" : "object"
       ,"COSMIC_Total_Alterations_In_Gene" : "object"
       ,"CCLE_ONCOMAP_Total_Mutations_In_Gene" : "object"
       ,"HGNC_HGNC_ID" : "object"
       ,"UniProt_AApos" : "object"
       ,"Transcript_Position" : "object"
       ,"HGNC_OMIM_ID_Supplied_By_NCBI" : "object"
      }


    file_count = 0
   
    # create an empty dataframe. we use this to merge dataframe
    disease_bigdata_df = pd.DataFrame()

    # iterate over the selected files
    for oncotator_file in data_library['filename']:
        file_count+= 1

        log.info('-'*10 + "{0}: Processing file {1}".format(file_count, oncotator_file) + '-'*10)

        try:
            # covert the file to a dataframe
            filename = oncotator_object_path + oncotator_file
            with open(filename) as infile:
                filestring = StringIO(infile.read())
            df = convert_file_to_dataframe(filestring)
            try:
                df = cleanup_dataframe(df)
            except RuntimeError as re:
                log.warning('%s: problem cleaning dataframe for %s: %s' % (study, filename, re))
        except Exception as e:
            print e
            raise
           
        if df.empty:
            log.debug('empty dataframe for file: ' + str(oncotator_file))
            continue
        #------------------------------
        # different operations on the frame
        #------------------------------
        # get only the required BigQuery columns
        df = df[bq_columns]
       
        # format oncotator columns; name changes etc 
        df = format_oncotator_columns(df)

        # add new columns
        df = add_columns(df, sample_code2letter, study)

        disease_bigdata_df = disease_bigdata_df.append(df, ignore_index = True)
            
        log.info('-'*10 + "{0}: Finished file {1}. rows: {2}".format(file_count, oncotator_file, len(df)) + '-'*10)

    # this is a merged dataframe
    if not disease_bigdata_df.empty:
        
        # remove duplicates; various rules; see check duplicates)
        
        log.info('\tcalling check_duplicates to collapse aliquots with %s rows' % (len(disease_bigdata_df)))
        disease_bigdata_df = check_duplicates.remove_maf_duplicates(disease_bigdata_df, sample_code2letter, log)
        log.info('\tfinished check_duplicates to collapse aliquots with %s rows' % (len(disease_bigdata_df)))

        # enforce unique mutation--previous
        # unique_mutation = ['Chromosome', 'Start_Position', 'End_Position', 'Tumor_Seq_Allele1', 'Tumor_Seq_Allele2', 'Tumor_AliquotBarcode']
        # enforce unique mutation
        unique_mutation = ['Hugo_Symbol', 'Entrez_Gene_Id', 'Chromosome', 'Start_Position', 'End_Position', 'Reference_Allele', 'Tumor_Seq_Allele1', 'Tumor_Seq_Allele2',
                  'Tumor_AliquotBarcode']
        # merge mutations from multiple centers
        log.info('\tconsolidate the centers for duplicate mutations into list')
        seencenters = set()
        def concatcenters(df_group):
            if len(df_group) > 1:
                centers = list(set(df_group['Center'].tolist()))
                uniquecenters = set()
                delim = config['maf']['center_delim']
                for center in centers:
                    fields = center.split(delim)
                    for field in fields:
                        uniquecenters.add(field)
                sortedunique = delim.join(sorted(list(uniquecenters)))
                df_group.loc[:,'Center'] = sortedunique
                if sortedunique not in seencenters:
                    log.info('unique centers: %s' % sortedunique)
                    seencenters.add(sortedunique)
            return df_group

        disease_bigdata_df = disease_bigdata_df.groupby(unique_mutation).apply(concatcenters)
        log.info('\tfinished consolidating centers for duplicate mutations')

        # enforce unique mutation
        log.info('\tcalling remove_duplicates to collapse mutations with %s rows' % (len(disease_bigdata_df)))
        disease_bigdata_df = remove_duplicates(disease_bigdata_df, unique_mutation)
        log.info('\tfinished remove_duplicates to collapse mutations with %s rows' % (len(disease_bigdata_df)))

        # convert the disease_bigdata_df to new-line JSON and the upload the file
        file_to_upload = StringIO()

        log.info('writing %s rows' % (len(disease_bigdata_df)))
        for _, rec in disease_bigdata_df.iterrows():
            file_to_upload.write(rec.convert_objects(convert_numeric=False).to_json() + "\n")
        file_to_upload.seek(0)
        with open(oncotator_object_path + "{0}.json".format(study), 'w') as outfile:
            outfile.write(file_to_upload.getvalue())
    else:
        log.warning('Empty dataframe for %s in %s!' % (oncotator_file, study))
    return True

if __name__ == '__main__':
    log.info('start maf part2 pipeline')
    config = json.load(open(sys.argv[1]))
  
    project_id = config['project_id']
    bucket_name = config['buckets']['open']
    sample_code2letter = config['sample_code2letter']
 
    # get disease_codes/studies( TODO this must be changed to get the disease code from the file name)
    df = convert_file_to_dataframe(open(sys.argv[2]))
    df = cleanup_dataframe(df)
    studies = list(set(df['Study'].tolist()))

    # get bq columns ( this allows the user to select the columns
    # , without worrying about the index, case-sensitivenes etc
    selected_columns = pd.read_table(sys.argv[3], names=['bq_columns'])
    transposed = selected_columns.T
    transposed.columns = transposed.loc['bq_columns']
    transposed = cleanup_dataframe(transposed)
    bq_columns = transposed.columns.values

    # submit threads by disease  code
    pm = process_manager.ProcessManager(max_workers=33, db='maf.db', table='task_queue_status', log=log)
    for idx, df_group in df.groupby(['Study']):
        #future = pm.submit(process_oncotator_output, project_id, bucket_name, df_group, bq_columns, sample_code2letter)
        process_oncotator_output( project_id, bucket_name, df_group, bq_columns, sample_code2letter, config['maf']['oncotator_object_path'])
        time.sleep(0.2)
    pm.start()
    log.info('finished maf part2 pipeline')
