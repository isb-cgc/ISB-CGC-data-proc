* [Overview](#overview)

<a id="overview"></a>
# Overview
--------------------------------------------------
ETL Pipeline to load various datasets into BigQuery

### export paths
```sh
export PYTHONPATH=[path to this bigqury_etl module]
export SSL_DIR=[path to directory having SSL keys]
export SCHEMA_DIR=[path to folder having the schemas]
```

### Pipeline Usage:
```
usage: pipeline.py [-h] [-w MAX_WORKERS] [--dry_run] [--create_new] [--debug]
                   datatype config_file

example: python pipeline.py protein config/data_etl_open.json --dry_run --debug --create_new
```
Datasets that are part of the pipeline:
  * protein
  * mirna_mirna
  * mirna_isoform
  * methylation
  * cnv
  * mrna_bcgsc
  * mrna_unc


### Datasets that are not part of Pipeline

* TCGA Annotations
 * python tcga_annotations/transform.py config/data_etl_open.json
 * python tcga_annotations/load.py config/data_etl_open.json

* mirtarbase
 * Change the mirtarbase.input_file parameter to latest mirtarbase file link
 * python mirtarbase/transform.py config/data_etl_open.json
 * python mirtarbase/load.py config/data_etl_open.json

### Tests
 * python tests/sanity_check_bq2.py isb-cgc tcga_data_staging Clinical
 * python tests/sanity_check_bq.py
