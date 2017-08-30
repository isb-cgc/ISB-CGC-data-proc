import os
import sys
import re
import hashlib
import json
from cStringIO import StringIO
import pandas as pd
import logging
from HTMLParser import HTMLParser
import datetime
import os.path
from google.cloud import storage
from lxml import etree
from collections import Counter


#--------------------------------------
# set default bucket
#--------------------------------------
storage.set_default_bucket("isb-cgc")
storage_conn = storage.get_connection()
storage.set_default_connection(storage_conn)


all_elements = {}

#--------------------------------------
# get the bucket contents
#--------------------------------------
bucket = storage.get_bucket('isb-cgc-open')

for k in bucket.list_blobs(prefix="tcga/"):

   if '.xml' in k.name and 'clinical' in k.name:
      print k.name
      disease_type = k.name.split("/")[1]

      maf_data = StringIO()
      k.download_to_file(maf_data)
      maf_data.seek(0)

      tree = etree.parse(maf_data)
      root = tree.getroot() #this is the root; we can use it to find elements
      blank_elements = re.compile("^\\n\s*$")

      admin_element = root.findall('.//*/[@procurement_status="Completed"]', namespaces=root.nsmap)
      
      maf_data.close()

      # ------------------------------------
      unique_elements = {}
      for i in admin_element:
         unique_elements[i.tag.split("}")[1]] = 1
  
      for j in unique_elements:
         if disease_type in all_elements:
            all_elements[disease_type].append(j)
         else:
            all_elements[disease_type] = []
            all_elements[disease_type].append(j)

for dt in all_elements:
   c = dict(Counter(all_elements[dt]))
   df = pd.DataFrame(c.items(), columns=["item", "counts"])
   df = df.sort(['counts'], ascending=[False])
   df_stringIO =  df.to_csv(sep='\t', index=False)

   # upload the file
   upload_bucket = storage.get_bucket('ptone-experiments')
   upload_blob = storage.blob.Blob('working-files/clinical_metadata/' + dt + ".counts.txt", bucket=upload_bucket)
   upload_blob.upload_from_string(df_stringIO)

#for dt in all_elements:
#   c = dict(Counter(all_elements[dt]))
#   df = pd.DataFrame(c.items(), columns=["item", "counts"])
#   for ele in df[df.counts >= round(int(df.counts.quantile(.70)))]['item']:
#      print ele
#


