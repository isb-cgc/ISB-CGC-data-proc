#!/usr/bin/env python

# Copyright 2015, Institute for Systems Biology.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import time
import pandas as pd
import numpy as np
import json
from pandas.io import gbq
from gcloud import storage
from cStringIO import StringIO
import re
from os.path import basename
from collections import OrderedDict


def select_aliquots(duplicate_aliquots):

   d = OrderedDict([
                      ('13:15', ['10'])
                    , ('19:20', ['D'])
                    , ('26:28', ['01', '08', '14', '09', '21', '30', '10', '12', '13', '31', '18', '25'])
                   ])

   for k, v in d.items():
      ki = k.strip().split(':')

      for y in v:
         aliquots_to_be_deleted = []
         for x in duplicate_aliquots:

#         for y in v:
            string = str(x[int(ki[0]):int(ki[1])])
            #print string, str(y)
            if string == str(y):
               aliquots_to_be_deleted.append(x)
         if len(aliquots_to_be_deleted) == 1:
            return aliquots_to_be_deleted[0]
         else:
            continue

   # return default aliqout   
   default_aliquot = duplicate_aliquots[0]
   try:
      plate_ids = [int(x[21:25]) for x in duplicate_aliquots]
      plate_ids = sorted(plate_ids, reverse=True)
      default_aliquot = [x for x in duplicate_aliquots if str(plate_ids[0]) in x][0]
   except:
      pass
   return default_aliquot


def remove_maf_duplicates(df, sample_type_info):

    # Multiple tumor aliquots for one sample
    tumor_sample_barcode_duplicates = df[df.duplicated('Tumor_SampleBarcode')]['Tumor_SampleBarcode'].unique()
    for dup in tumor_sample_barcode_duplicates:
       duplicate_aliquots = df[df['Tumor_SampleBarcode'] == dup]['Tumor_AliquotBarcode'].unique()
       if len(duplicate_aliquots) > 1:
 
          aliquot_to_keep =  select_aliquots(duplicate_aliquots)
          df = df[ ~
                   (df['Tumor_SampleBarcode'] == dup)
                   &
                   (df['Tumor_SampleBarcode'] != aliquot_to_keep)
                 ]

    # Multiple normal aliquots for one sample
    normal_sample_barcode_duplicates = df[df.duplicated('Normal_SampleBarcode')]['Normal_SampleBarcode'].unique()
    for dup in normal_sample_barcode_duplicates:
       duplicate_aliquots = df[df['Normal_SampleBarcode'] == dup]['Normal_AliquotBarcode'].unique()
       if len(duplicate_aliquots) > 1:
          aliquot_to_keep =  select_aliquots(duplicate_aliquots)
          df = df[ ~
                   (df['Normal_SampleBarcode'] == dup)
                   &
                   (df['Normal_AliquotBarcode'] != aliquot_to_keep)
                 ]
          
    #---------------------------------------
    # Multiple normals for one tumor sample
    #---------------------------------------
    tumor_aliqout_barcode_duplicates = df[df.duplicated('Tumor_AliquotBarcode')]['Tumor_AliquotBarcode'].unique()
    for dup in tumor_aliqout_barcode_duplicates:
       duplicate_aliquots = df[df['Tumor_AliquotBarcode'] == dup]['Normal_AliquotBarcode'].unique()
       if len(duplicate_aliquots) > 1:

          aliquot_to_keep =  select_aliquots(duplicate_aliquots)

          df = df[ ~ 
                   (df['Tumor_AliquotBarcode'] == dup) 
                   & 
                   (df['Normal_AliquotBarcode'] != aliquot_to_keep)
                 ]

   
    return df 
