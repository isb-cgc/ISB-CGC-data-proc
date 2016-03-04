* To parse CCLE MAF file and upload transform njson file to the bucket

  ```
  python ccle_maf/transform.py config/data_etl.json ccle_maf/files.txt ccle_maf/bq_maf_columns.txt
  ```
* To load the tranform from bucket to bigquery

  ```
  python load.py config/data_etl.json
  ```
