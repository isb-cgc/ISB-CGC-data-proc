ISB-CGC-data-proc
================

**gdc**: The gdc code uploads the contents of the NCI-GDC into GCS.

**data_upload**: The data upload code uploads the contents of the TCGA DCC into GCS.  
(Note that this code is mostly obsolete as the TCGA DCC no longer exists.)

**cghub**: Used by the data_upload code to obtain metadata on the bam and fasta files 
that reside in CGHub.  (Note that this code is entirely obsolete as CGHub no longer exists.)

**tcga_etl_pipeline**: ETL Pipeline to load various datasets into BigQuery.  Uses the metadata output of data_upload
and uses bigquery_etl to do thee actual creation of the BigQuery tables

**bigquery_etl**: This module provides tools for data extraction, transformation, and loading to Google BigQuery.
