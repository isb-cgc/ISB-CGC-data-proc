import sys
import re
import json
import os
import pandas as pd
import glob
import os.path
from os.path import basename
from pandas import ExcelWriter
from collections import defaultdict
import roman_to_int
import numpy as np
import math

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


def get_antibody_gene_protein_map(rootdir1):
    antibody_to_protein_map = {}
    antibody_to_gene_map = {}
    
    # collect gene and protein information for missing one
    for subdir1, dirs1, files1 in os.walk(rootdir1):
       for file in files1:
          a = re.compile("^.*.antibody_annotation.txt$")
          if a.match(file):
             filename = os.path.join(subdir1, file)
    
             # convert the file into a dataframe
             data_df  = pd.read_csv(filename, delimiter='\t', header=0, keep_default_na=False)
    
             # collect information to create a map
             for idx, row in data_df.iterrows():
                record = row.to_dict()
    
                if not antibody_to_protein_map.get('composite_element_ref'):
                    antibody_to_protein_map[record['composite_element_ref']] =  []
                antibody_to_protein_map[record['composite_element_ref']].append(record['protein_name'].strip())
    
                if not antibody_to_gene_map.get('composite_element_ref'):
                    antibody_to_gene_map[record['composite_element_ref']] =  []
                antibody_to_gene_map[record['composite_element_ref']].append(record['gene_name'].strip())
    
    return antibody_to_gene_map, antibody_to_protein_map


def main():
    # create a excel spreadsheet with the HGNC and antibody-gene-p map
    writer = ExcelWriter('antibody-gene-protein-map.xlsx')

    rootdir1 = 'corrected_aa_files/'
    antibody_to_gene_map, antibody_to_protein_map = get_antibody_gene_protein_map(rootdir1)

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
    hgnc_df = parse_hgnc(sys.argv[1])
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
    
   
if __name__ == "__main__":
    main() 
