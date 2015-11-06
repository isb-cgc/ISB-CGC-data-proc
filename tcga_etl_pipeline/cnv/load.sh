#load alphabetically! Bigquery has a 10000 file limit, we have 24000 files. Super hack!!
gsutil ls gs://isb-cgc-open/tcga/intermediary/cnv/bq_data_files/* | awk 'BEGIN{FS="bq_data_files/"}{print $2}' | awk 'BEGIN{FS=""}{print $1}' | sort | uniq | 
xargs -L1 -I% sudo python ../bigquery_etl/load/load_data_from_file.py -t=NEWLINE_DELIMITED_JSON isb-cgc tcga_data_staging CNV schemas/cnv.json  gs://isb-cgc-open/tcga/intermediary/cnv/bq_data_files/%* 
