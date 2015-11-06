import sys
import re
import json
import os
import pandas as pd
import glob
import os.path
from os.path import basename
from pandas import ExcelWriter

# go through the AA files and create corrected files. 
rootdir = '/home/srauser/isb-cgc-scripts/data-prototyping/bigquery_etl/protein/antibody-gene-protein-map/aa_files'

# Get protein name from the 'composite element ref', get only for the missing one/ problematic ones
antibodySource = ['M', 'R', 'G']
validationStatus = [ 'V', 'C', 'NA', 'E', 'QC']
substrings = ["-".join(["", ads, vds + ".*"]) for ads in antibodySource for vds in validationStatus]
protein_substrings = re.compile("|".join(substrings))


# collect gene and protein information for missing one
for subdir1, dirs1, files1 in os.walk(rootdir):
   for file in files1:
      a = re.compile("^.*.antibody_annotation.txt$")
      if a.match(file):
         filename = os.path.join(subdir1, file)
         print "-"*30
         print basename(filename)
         
         data_df  = pd.read_csv(filename, delimiter='\t', header=0, keep_default_na=False)
         column_names = list(data_df.columns.values)
         
         # fix antibody_validation_status column, this can be a dict in case there are more than one substitutions
         column_names = map(lambda x: x.replace('Val.Stat', 'antibody_validation_status'), column_names)
         column_names = map(lambda x: x.replace (" ", "_"), column_names)
         pattern = re.compile(r"\.+")
         column_names = map(lambda x: pattern.sub("_", x), column_names)         
         column_names = map(lambda x: x.strip("_"), column_names)
         column_names = map(lambda x: x.lower(), column_names)
         
         data_df.columns = column_names

         # Fix minor data inconsistencies in the antibody_validation_status values
         if 'antibody_validation_status' in column_names:
            data_df['antibody_validation_status'].replace({'UsewithCaution': 'Use with Caution', 'UnderEvaluation': 'Under Evaluation'}, inplace=True)
         else:
            print "No antibody_validation_status in file"
 
         # Check if the file has composite_element_ref, else create one
         if 'composite_element_ref' not in column_names:
            print "The file doesnt have a composite_element_ref" % filename 
            data_df['antibody_origin_abbv'] = map(lambda x: x[0], data_df['antibody_origin'])
            data_df['composite_element_ref'] = data_df['protein_name'] + "-" +  data_df['antibody_origin_abbv'] + "-" + data_df['antibody_validation_status']
            del data_df['antibody_origin_abbv']
                 
 
         # get protein name from the 'composite column ref' 
         if 'protein_name' not in column_names:  
            data_df['protein_name'] = map(lambda x: protein_substrings.sub("", x), data_df['composite_element_ref'])

         # strip each value of the empty space
         data_df['gene_name'] = map(lambda x: x.strip(), data_df['gene_name']) 
         data_df['protein_name'] = map(lambda x: x.strip(), data_df['protein_name'])

         # create corrected Antibody Annotation files
         corrected_aa_files_dir = "corrected_aa_files/"
         if not os.path.exists(corrected_aa_files_dir):
            os.makedirs(corrected_aa_files_dir)  
         data_df.to_csv(corrected_aa_files_dir + file, sep='\t', index=False)  

