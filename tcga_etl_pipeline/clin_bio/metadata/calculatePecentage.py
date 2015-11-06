import numpy as np
import os
import sys
import gcloud.storage
import re
import hashlib
import json
from cStringIO import StringIO
import pandas as pd
import datetime
import os.path
from gcloud import storage
from collections import Counter

def frange(start, stop, step):
    i = start
    while i < stop:
        yield i
        i += step

#--------------------------------------
# set default bucket
#--------------------------------------
storage.set_default_bucket("isb-cgc")
storage_conn = storage.get_connection()
storage.set_default_connection(storage_conn)

all_elements = {}

df2 = pd.DataFrame()
#--------------------------------------
# get the bucket contents
#--------------------------------------
bucket = storage.get_bucket('ptone-experiments')

for k in bucket.list_blobs(prefix="working-files/clinical_metadata/"):

   if 'counts.txt' in k.name:
      disease_type = k.name.split("/")[2].split(".")[0]

      data = StringIO()
      k.download_to_file(data)
      data.seek(0)

      df = pd.read_csv(data, sep="\t")
      
      df['disease_type'] = disease_type

      if df2.empty:
         df2 = df
         continue

      frames = [df, df2]

      result = pd.concat(frames)
      df2 = result
      data.close()

# get present selected metadata 
clinical_metadata = [x.strip() for x in open(sys.argv[1], "r")]

# get all disease types
disease_types = list(set(result["disease_type"].tolist()))

for x in range(10,60, 10):
   all_elements = []
   #print "--"*20 ,x ,"%", "--"*20
   for dt in disease_types:
      df = result[result.disease_type == str(dt)]

      # number of patients is always the max number in the counts
      number_of_patients = df.counts.max()

      # lets round this 
      # x is 10, 20, 30 ..50
      cutoff = number_of_patients - ((number_of_patients * x ) /100)
      
      df1 = df[df.counts >= cutoff]
      eles = list(set(df1["item"].tolist()))
      all_elements = all_elements + eles


   print x, "\n" ,"--"*20
   print "\n".join(list(set(all_elements)))
#   print len(list(set(all_elements)))
sys.exit()
