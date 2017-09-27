import tarfile,os
import sys
import time
from lxml import etree
import re
import json
import os
import google.cloud.storage
import hashlib
from StringIO import StringIO
import pandas as pd
import glob
import os.path
from os.path import basename
from collections import Counter
import operator
from pandas import ExcelWriter


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


#------------------------------------------
# Get HGNC approved, alias, previous symbols
#------------------------------------------
hgnc_json_data=open(sys.argv[1])
data = json.load(hgnc_json_data)

row_list = []
for hgnc in data["response"]['docs']:

#   print hgnc
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
writer = ExcelWriter('protein_log.xlsx')
hgnc_df.to_excel(writer,'HGNC_validated_genes')
writer.save()

