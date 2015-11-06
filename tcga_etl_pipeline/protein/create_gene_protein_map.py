import tarfile,os
import sys
import time
from lxml import etree
import re
import json
import os
import gcloud.storage
import hashlib
from StringIO import StringIO
import pandas as pd
import glob
import os.path
from os.path import basename
from collections import Counter
import operator
# requires atleast pandas-0.16.1
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../util'))
if not path in sys.path:
    sys.path.append(path)
del path
import util

#-------------------------------------------------
# HGNC validation
   ## - Algorithm - ##
   #1. Check if the gene name is avaliable in the HGNC symbols, if yes - Approved, if no - Step 2
   #2. Check if the gene name is avaliable in the HGNC Alias symbols, if yes - Approved, if no - Step 3
   #3. Check if the gene name is avaliable in the HGNC previous symbols, if yes - Replace with the HGNC symbol, if no - Step 4
   #4. Not found, status update to "not_found". Gene not changed. 
#------------------------------------------------
def hgnc_validation(gene_counter, hgnc_df):
   validation = []
   val_genes = []
   for gene_name in gene_counter:
      genes = gene_name.split()
      if len(genes) > 1:
         # NOTE this function inside function
         r, x = hgnc_validation(genes, hgnc_df)
         validation.append(" ".join(r))
         val_genes.append(" ".join(x))
         continue

      if not hgnc_df[hgnc_df.symbol == gene_name].empty:
         validation.append("approved")
         val_genes.append(gene_name)
      else:
         alias_df = hgnc_df[pd.Series([gene_name in x for  x in hgnc_df.alias_symbol.str.split(";")])]
         if not alias_df.empty:
            validation.append("alias" )
            val_genes.append(gene_name)
         else:
            prev_df = hgnc_df[pd.Series([gene_name in x for  x in hgnc_df.prev_symbol.str.split(";")])]
            if not prev_df.empty:
               validation.append("prev")
               val_genes.append(prev_df.symbol.values[0])
            else:
               validation.append("not_found")
               val_genes.append(gene_name)
   return validation, val_genes


def parse_hgnc(hgnc_file):
    #------------------------------------------
    # Get HGNC approved, alias, previous symbols
    #------------------------------------------
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

hgnc_df = parse_hgnc(sys.argv[1])
#---------------------------------------------------------------------

#--------------------------Part 2-----------------------------------#

# Get protein name from the 'composite element ref', get only for the missing one/ problematic ones
antibodySource = ['M', 'R', 'G']
validationStatus = [ 'V', 'C', 'NA', 'E']
substrings = ["-".join(["", ads, vds + ".*"]) for ads in antibodySource for vds in validationStatus]
protein_substrings = re.compile("|".join(substrings))

# search for methylation files
protein_file = re.compile("^.*antibody_annotation.txt$")
search_diseases = ["tcga/" + d for d in disease_codes]
data_library = etl.search_files(search_patterns=['HumanMethylation', 'TCGA-'], regex_search_pattern=protein_file, prefixes=search_diseases)

#@!!!# go through the AA files and create corrected files. Also, create a antibody- gene map
#@!!!rootdir1 = '/mnt/datadisk-3/tcga-data-from-bucket-protein/'
#@!!!
#@!!!antibody_to_protein_map = {}
#@!!!gene_inconsistencies = {}
#@!!!protein_inconsistencies = {}
#@!!!
#@!!!
#@!!!# collect gene and protein information for missing one
#@!!!for subdir1, dirs1, files1 in os.walk(rootdir1):
#@!!!   for file in files1:
#@!!!      a = re.compile("^.*.antibody_annotation.txt$")
#@!!!      if a.match(file):
#@!!!         filename = os.path.join(subdir1, file)
#@!!!
#@!!!         data_df  = pd.read_csv(filename, delimiter='\t', header=0, keep_default_na=False)
#@!!!         column_names = list(data_df.columns.values)
#@!!!
#@!!!         # fix antibody_validation_status column, this can be a dict in case there are more than one substitutions
#@!!!         column_names = map(lambda x: x.replace('Val.Stat', 'antibody_validation_status'), column_names)
#@!!!         column_names = map(lambda x: x.replace (" ", "_"), column_names)
#@!!!         pattern = re.compile(r"\.+")
#@!!!         column_names = map(lambda x: pattern.sub("_", x), column_names)         
#@!!!         column_names = map(lambda x: x.strip("_"), column_names)
#@!!!         column_names = map(lambda x: x.lower(), column_names)
#@!!!         
#@!!!         data_df.columns = column_names
#@!!!
#@!!!         # Fix minor data inconsistencies in the antibody_validation_status values
#@!!!         if 'antibody_validation_status' in column_names:
#@!!!            data_df['antibody_validation_status'].replace({'UsewithCaution': 'Use with Caution', 'UnderEvaluation': 'Under Evaluation'}, inplace=True)
#@!!!         
#@!!!         # LUAD doesnt have composite_element_ref
#@!!!         if 'composite_element_ref' not in column_names:
#@!!!            data_df['antibody_origin_abbv'] = map(lambda x: x[0], data_df['antibody_origin'])
#@!!!            data_df['composite_element_ref'] = data_df['protein_name'] + "-" +  data_df['antibody_origin_abbv'] + "-" + data_df['antibody_validation_status']
#@!!!            del data_df['antibody_origin_abbv']
#@!!!      
#@!!!         # get protein name from the 'composite column ref' 
#@!!!         if 'protein_name' not in column_names:  
#@!!!            data_df['protein_name'] = map(lambda x: protein_substrings.sub("", x), data_df['composite_element_ref'])
#@!!!
#@!!!         # strip each value of the empty space
#@!!!         data_df['gene_name'] = map(lambda x: x.strip(), data_df['gene_name']) 
#@!!!         data_df['protein_name'] = map(lambda x: x.strip(), data_df['protein_name'])
#@!!!
#@!!!         # collect information to create a map
#@!!!         for idx, row in data_df.iterrows():
#@!!!            record = row.to_dict()
#@!!!            if record['composite_element_ref'] in antibody_to_protein_map: 
#@!!!               gene_inconsistencies[record['composite_element_ref']].append(record['gene_name'])
#@!!!               protein_inconsistencies[record['composite_element_ref']].append(record['protein_name'])
#@!!!
#@!!!            else:
#@!!!               antibody_to_protein_map[record['composite_element_ref']] = {'gene_name' :  record['gene_name'], 'protein_name': record['protein_name']}
#@!!!               gene_inconsistencies[record['composite_element_ref']] = []
#@!!!               protein_inconsistencies[record['composite_element_ref']] = []
#@!!!               gene_inconsistencies[record['composite_element_ref']].append(record['gene_name'])
#@!!!               protein_inconsistencies[record['composite_element_ref']].append(record['protein_name'])
#@!!!
#@!!!         # create corrected Antibody Annotation files
#@!!!         corrected_aa_files_dir = "corrected_aa_files/"
#@!!!         if not os.path.exists(corrected_aa_files_dir):
#@!!!            os.makedirs(corrected_aa_files_dir)  
#@!!!         data_df.to_csv(corrected_aa_files_dir + file, sep='\t', index=False)  
#@!!!
#@!!!
#@!!!
#@!!!#----------------------------------------------
#@!!!# create a map
#@!!!#----------------------------------------------
#@!!!output_list = []
#@!!!for i in gene_inconsistencies:
#@!!!   gene_counter = set(gene_inconsistencies[i])
#@!!!   protein_counter = set(protein_inconsistencies[i])
#@!!!  
#@!!!   # HGNC validation 
#@!!!   validation, val_genes = hgnc_validation(gene_counter, hgnc_df)
#@!!!   
#@!!!   # small hack to select the best gene; select the best gene
#@!!!   validation_rank = ["approved", "alias", "prev", "not_found"] 
#@!!!   final_val_gene = None
#@!!!   final_val_protein = None
#@!!!   try:
#@!!!      try:
#@!!!         final_picked_gene = val_genes[validation.index("approved approved approved")]
#@!!!      except:
#@!!!         try:
#@!!!            final_picked_gene = val_genes[validation.index("approved approved")]
#@!!!         except:
#@!!!            final_picked_gene = val_genes[validation.index("approved")]
#@!!!   
#@!!!   except:
#@!!!      try:
#@!!!         final_picked_gene = val_genes[validation.index("alias")]
#@!!!      except:
#@!!!         try:
#@!!!            final_picked_gene = val_genes[validation.index("prev")]
#@!!!            final_val_gene = "substituted_prev_with_recent"
#@!!!         except:
#@!!!            final_picked_gene = " ".join(val_genes)
#@!!!            final_val_gene = "dead_end_not_found_in_HGNC"
#@!!!
#@!!!
#@!!!   # select the best protein
#@!!!   if len(protein_counter) > 1:
#@!!!     final_picked_protein = protein_substrings.sub("", i)
#@!!!     final_val_protein = "parsed_protein_name_from_composite_ref"
#@!!!   else:
#@!!!     final_picked_protein = list(protein_counter)[0]
#@!!! 
#@!!!   output_list.append(
#@!!!                   {
#@!!!                     'gene_counter': len(gene_counter)
#@!!!                    ,'protein_counter': len(protein_counter)
#@!!!                    ,'composite_element_ref': i
#@!!!                    ,'genes_found': ";".join(gene_counter)
#@!!!                    ,'gene_hgnc_validation' :  ";".join(validation)
#@!!!                    ,'final_gene_name' : final_picked_gene
#@!!!                    ,'final_gene_status' : final_val_gene
#@!!!                    ,'proteins_found': ";".join(protein_counter)
#@!!!                    ,'final_protein_name' : final_picked_protein
#@!!!                    ,'final_protein_status' : final_val_protein
#@!!!                    }
#@!!!                )
#@!!!
#@!!!   output_df = pd.DataFrame(output_list) 
#@!!!   col_order = ['composite_element_ref', 'genes_found', 'proteins_found', 'gene_counter',  'protein_counter', 'gene_hgnc_validation', 'final_gene_name', 'final_protein_name' , 'final_gene_status', 'final_protein_status']
#@!!!   output_df.sort(['final_gene_status', 'final_protein_status'], ascending=[0, 0],inplace = True)
#@!!!   # create a antibody -gene-protein map
#@!!!   output_df.to_csv("gene_protein_map.xls", sep='\t', index=False, columns= col_order) 
#@!!!
#@!!!
