cghub
================

scripts to parse CGHub REST API for CGHub files

python/main/import_cghub_manifest.py: main script.  processes the output from the REST 
call to CGHub to put together a list of CGHubRecordInfo classes.  returns 
the list of CGHubRecordInfo and the file information on the largest and 
smallest file found.

Notes:
```
cd main
export PYTHONPATH=../util:$PYTHONPATH
python ./import_cghub.py 
```

To output a TSV file for further downstream processing by the cghub download scripts,
```
python ./import_cghub.py --outputFileName downloadlist.tsv --outputTumorTypeCode CESC --outputSequenceType D
```

```
[...]
2015-07-29 09:39:07.164344 finished processing archive
2015-07-29 09:39:07.164371 outputting file to downloadlist.tsv
2015-07-29 09:39:07.164468 filtering for tumor type CESC
2015-07-29 09:39:07.164482 filtering for sequence type D
2015-07-29 09:39:07.166159 found 16 BAM entries matching filters
2015-07-29 09:39:07.166179 total selected BAM file size: 199029940294
```

```main(platform, type_uri = 'detail', log = None, limit = -1, verbose = False, print_response = False)```
* platform: which platform to fetch.  for CCLE, use '*Other_Sequencing_Multiisolate', for TCGA, use 'phs000178'
* type_uri: can be 'detail' or 'full'.  'detail' will fetch back just the detail portion of the XML, 'full' will return all the annotation and set the additional value for 'instrument_model' in CGHubRecord information
* log: an oprional log to pass in.  if None, will print to console
* limit:  if set, the parse will end after limit records and return
* verbose: if True, will print out additional information
* print_repsonse: if True, will print out the entire returned XML response

```python/utils/cghub_record_info.py```: defines two classes.  CGHubRecordInfo mirrors 
the columns returned back from the URL manifest.  It has 
two CGHubFileInfo that capture the tags associated with the BAM file and the 
BAI file.

