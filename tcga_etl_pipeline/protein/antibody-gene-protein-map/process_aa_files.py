import sys
import re
import json
import pandas as pd
from pandas import ExcelWriter
import numpy as np
import os.path
from os.path import basename

from bigquery_etl.extract.gcloud_wrapper import GcsConnector

def download_antibody_annotation_files(config):
    object_key_template = config['maf']['aa_object_key_template']
    aa_file_dir = config['maf']['aa_file_dir']
    gcs = GcsConnector(config['project_id'], config['buckets']['open'])
    studies = config['all_tumor_types']
    
    if not os.path.isdir(aa_file_dir):
        os.makedirs(aa_file_dir)
    for study in studies:
        keypath = object_key_template % (study.lower(), study.upper())
        tmpfile = gcs.download_blob_to_file(keypath)
        with open(aa_file_dir + keypath[keypath.rindex('/'):], 'w') as outfile:
            outfile.write(tmpfile.getvalue())


def process_antibody_annotation_files(config):
    # Get protein name from the 'composite element ref', get only for the missing one/ problematic ones
    antibodySource = ['M', 'R', 'G']
    validationStatus = ['V', 'C', 'NA', 'E', 'QC']
    substrings = ["-".join(["", ads, vds + ".*"]) for ads in antibodySource for vds in validationStatus]
    protein_substrings = re.compile("|".join(substrings))
    
    # collect gene and protein information for missing one
    aa_file_dir = config['maf']['aa_file_dir']
    for files in os.listdir(aa_file_dir):
        for aafile in files:
            a = re.compile("^.*.antibody_annotation.txt$")
            if a.match(aafile):
                filename = os.path.join(aa_file_dir, aafile)
                print "-" * 30
                print basename(filename)
                data_df = pd.read_csv(filename, delimiter='\t', header=0, keep_default_na=False)
                column_names = list(data_df.columns.values)
            # fix antibody_validation_status column, this can be a dict in case there are more than one substitutions
                column_names = map(lambda x:x.replace('Val.Stat', 'antibody_validation_status'), column_names)
                column_names = map(lambda x:x.replace(" ", "_"), column_names)
                pattern = re.compile(r"\.+")
                column_names = map(lambda x:pattern.sub("_", x), column_names)
                column_names = map(lambda x:x.strip("_"), column_names)
                column_names = map(lambda x:x.lower(), column_names)
                data_df.columns = column_names
            # Fix minor data inconsistencies in the antibody_validation_status values
                if 'antibody_validation_status' in column_names:
                    data_df['antibody_validation_status'].replace({'UsewithCaution':'Use with Caution', 'UnderEvaluation':'Under Evaluation'}, inplace=True)
                else:
                    print "No antibody_validation_status in file"
                # Check if the file has composite_element_ref, else create one
                if 'composite_element_ref' not in column_names:
                    print "The file doesnt have a composite_element_ref" % filename
                    data_df['antibody_origin_abbv'] = map(lambda x:x[0], data_df['antibody_origin'])
                    data_df['composite_element_ref'] = data_df['protein_name'] + "-" + data_df['antibody_origin_abbv'] + "-" + data_df['antibody_validation_status']
                    del data_df['antibody_origin_abbv']
                # get protein name from the 'composite column ref'
                if 'protein_name' not in column_names:
                    data_df['protein_name'] = map(lambda x:protein_substrings.sub("", x), data_df['composite_element_ref'])
                # strip each value of the empty space
                data_df['gene_name'] = map(lambda x:x.strip(), data_df['gene_name'])
                data_df['protein_name'] = map(lambda x:x.strip(), data_df['protein_name'])
                # create corrected Antibody Annotation files
                corrected_aa_files_dir = config['maf']['corrected_aa_file_dir']
                if not os.path.exists(corrected_aa_files_dir):
                    os.makedirs(corrected_aa_files_dir)
                data_df.to_csv(corrected_aa_files_dir + aafile, sep='\t', index=False)

#------------------------------------------
# Get HGNC approved, alias, previous symbols
#------------------------------------------
def parse_hgnc(hgnc_file):
    hgnc_json_data=open(hgnc_file)
    data = json.load(hgnc_json_data)
    
    row_list = []
    for hgnc in data["response"]['docs']:
        if 'alias_symbol' not in hgnc:
            hgnc['alias_symbol'] = ""
    
        if 'prev_symbol' not in hgnc:
            hgnc['prev_symbol'] = ""
    
        # store symbol, alias, previous symbol
        row_list.append(
                         {
                           'symbol': hgnc['symbol']
                          ,'alias_symbol':";".join(hgnc['alias_symbol'])
                          ,'prev_symbol':";".join(hgnc['prev_symbol'])
                          }
                      )
    # create a dataframe with HGNC information; use this to query information about HGNC genes
    hgnc_df = pd.DataFrame(row_list)
    return hgnc_df

#-------------------------------------------------
# HGNC validation
    ## - Algorithm - ##
    #1. Check if the gene name is avaliable in the HGNC symbols, if yes - Approved, if no - Step 2
    #2. Check if the gene name is avaliable in the HGNC Alias symbols, if yes - Approved, if no - Step 3
    #3. Check if the gene name is avaliable in the HGNC previous symbols, if yes - Replace with the HGNC symbol, if no - Step 4
    #4. Not found, status update to "not_found". Gene not changed.
#------------------------------------------------
def hgnc_validation(gene_name, hgnc_df):
    if not hgnc_df[hgnc_df.symbol == gene_name].empty:
        val_status = "approved"
        val_gene = gene_name
        additional_notes = ''
    else:
        alias_df = hgnc_df[pd.Series([gene_name in x for  x in hgnc_df.alias_symbol.str.split(";")])]
        if not alias_df.empty:
            val_status = "alias" 
            val_gene = gene_name
            additional_notes = 'alias_gene_symbol'
        else:
            prev_df = hgnc_df[pd.Series([gene_name in x for  x in hgnc_df.prev_symbol.str.split(";")])]
            if not prev_df.empty:
                val_status = "prev"
                val_gene = str(prev_df.symbol.values[0])
                additional_notes = 'prev_gene_symbol_replaced_by_' + str(val_gene)
            else:
                val_status = "not_found"
                val_gene = gene_name
                additional_notes = 'dead_end_gene_not_found_in_HGNC' 
    return (val_status, val_gene, additional_notes)


def get_variants_of_name(name1, name_list):
    allnames = []
    for name2 in name_list:
        name_org2 = name2
    
        replace_column_strings= {
               " " : "" # blank space to underscore
             , "-" : "" # dashes to underscore (BQ doesnt allow dashes)
             , ")" : ""  # delete closing bracket
             , "(" : "" # opening bracket to underscore
             , "+" : "" # plus to underscore
             , "_" : ""
          }
    
        # replace all patterns from above
        for repl in replace_column_strings:
            name2 = name2.replace(repl, replace_column_strings[repl]).lower().strip()
            name1 = name1.replace(repl, replace_column_strings[repl]).lower().strip()
        if name2 == name1:
            allnames.append(name_org2)
    
    if len(set(allnames)) > 1:
        return list(set(allnames))
    else:
        return np.nan


def rank_dataframe(data_library):
    for idx, row in data_library.iterrows():
        record = row.to_dict()
        
        rank = 100
        notes = ''
        if int(record['num_genes']) != 1:
            rank = rank - 1
            notes = notes + 'num_genes not 1;'
        if int(record['num_proteins']) != 1:
            rank = rank - 1
            notes = notes + 'num_proteins not 1;'
        if 'not_found' in record['HGNC_validation_status']:
            rank = rank - 1
            notes = notes + 'gene not_found in HGNC;'
        if 'prev' in record['HGNC_validation_status']:
            rank = rank - 1
            notes = notes + 'prev gene in HGNC;'
        if 'alias' in record['HGNC_validation_status']:
            rank = rank - 1
            notes = notes + 'alias gene in HGNC;'
        if not isinstance(record['other_protein_names'], float):
            rank = rank - 2
            notes = notes + 'found other potential protein names;'
        if not isinstance(record['other_gene_names'], float):
            rank = rank - 2
            notes = notes + 'found other potential gene names;'
        data_library.loc[idx,'row_rank'] = rank
        data_library.loc[idx,'notes'] = notes
    return data_library


def get_antibody_gene_protein_map(config):
    antibody_to_protein_map = {}
    antibody_to_gene_map = {}
    
    # collect gene and protein information for missing one
    corrected_aa_files_dir = config['maf']['corrected_aa_file_dir']
    for files in os.listdir(corrected_aa_files_dir):
        for aafile in files:
            a = re.compile("^.*.antibody_annotation.txt$")
            if a.match(aafile):
                filename = os.path.join(corrected_aa_files_dir, aafile)
    
                # convert the file into a dataframe
                data_df  = pd.read_csv(filename, delimiter='\t', header=0, keep_default_na=False)
    
                # collect information to create a map
                for _, row in data_df.iterrows():
                    record = row.to_dict()
    
                    if not antibody_to_protein_map.get('composite_element_ref'):
                        antibody_to_protein_map[record['composite_element_ref']] =  []
                    antibody_to_protein_map[record['composite_element_ref']].append(record['protein_name'].strip())
    
                    if not antibody_to_gene_map.get('composite_element_ref'):
                        antibody_to_gene_map[record['composite_element_ref']] =  []
                    antibody_to_gene_map[record['composite_element_ref']].append(record['gene_name'].strip())
    
    return antibody_to_gene_map, antibody_to_protein_map


def fix_gene_protein_inconsistencies(config, hgnc_df_filename):
    # create a excel spreadsheet with the HGNC and antibody-gene-p map
    corrected_aa_files_dir = config['maf']['corrected_aa_file_dir']
    writer = ExcelWriter(corrected_aa_files_dir + 'antibody-gene-protein-map.xlsx')

    antibody_to_gene_map, antibody_to_protein_map = get_antibody_gene_protein_map(config)

    # create a dataframe to work with
    aa_map = []
    for i in antibody_to_gene_map:
        num_gene_names  = len(filter(len, antibody_to_gene_map[i]))
        num_protein_names  = len(filter(len, antibody_to_protein_map[i]))
        aa_map.append({
                       'composite_element_ref': i
                      ,'gene_name': antibody_to_gene_map[i]
                      ,'protein_name': antibody_to_protein_map[i]
                      ,'num_genes' : num_gene_names
                      ,'num_proteins': num_protein_names
                    })
    
    data_library =  pd.DataFrame(aa_map)

    # --------------------------part 1--------------------------------------
    ## check other potential protein and gene names    
    protein_lists = data_library['protein_name'].tolist()
    gene_lists = data_library['gene_name'].tolist()
    
    protein_names = [item for i in protein_lists for item in i]
    gene_names = [item for i in gene_lists for item in i]
    
    data_library.loc[:, 'other_protein_names'] = data_library['protein_name'].map(lambda x: get_variants_of_name(x[0], protein_names))
    data_library.loc[:, 'other_gene_names'] = data_library['gene_name'].map(lambda x: get_variants_of_name(x[0], gene_names))
    data_library.loc[:, 'final_curated_protein'] = data_library['protein_name']
   
    #--------------------------part 2 ----------------------------------------
    # HGNC validation 
    hgnc_df = parse_hgnc(hgnc_df_filename)
    hgnc_df.to_excel(writer,'HGNC_validated_genes')
    writer.save()

    # this is an hack if we find multiple genes 
    for idx, row in data_library.iterrows():
        record = row.to_dict()
    
        all_val_statuses = []
        all_val_genes = []
        additional_notes = ''
        for genelist in record['gene_name']:
            ind_val_status = []
            ind_val_gene = []
            ind_val_notes = []
            for gene in genelist.split():
                val_status, val_gene, additional_notes = hgnc_validation(gene, hgnc_df)
                ind_val_status.append(val_status)
                ind_val_gene.append(val_gene)
                ind_val_notes.append(additional_notes)
            all_val_statuses.append(" ".join(ind_val_status))
            all_val_genes.append(" ".join(ind_val_gene))
            additional_notes = ";".join(list(set(ind_val_notes)))

        data_library.loc[idx,'HGNC_validation_status'] = all_val_statuses
        data_library.loc[idx,'final_curated_gene'] = all_val_genes
        additional_notes = additional_notes.strip()
        if additional_notes:
            data_library.loc[idx,'additional_notes'] = additional_notes

    # -----------------Rank the dataframe----------------------------------#    
    # rank the data frame 
    data_library = rank_dataframe(data_library)
    data_library = data_library.sort(['row_rank'], ascending=[1])
    col_order = ['composite_element_ref', 'num_genes', 'num_proteins', 'gene_name', 'protein_name', 'HGNC_validation_status', 'other_protein_names', 'other_gene_names', 'final_curated_gene', 'final_curated_protein', 'row_rank', 'notes', 'additional_notes'] 
    data_library.to_excel(writer,'antibody-gene-protein-map', index=False, columns= col_order)
    writer.save()
    
def main(configfilename):
    # go through the AA files and create corrected files. 
    with open(configfilename) as configfile:
        config = json.load(configfile)
    download_antibody_annotation_files(config)
    process_antibody_annotation_files(config)
    fix_gene_protein_inconsistencies(config) 

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
