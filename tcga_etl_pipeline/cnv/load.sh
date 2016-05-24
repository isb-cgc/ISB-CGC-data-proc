#load alphabetically! Bigquery has a 10000 file limit, we have 24000 files. Super hack!!
gsutil ls gs://2016-05-03-isb-cgc-open/tcga-runs/intermediary/cnv/bq_data_files/* | awk 'BEGIN{FS="bq_data_files/"}{print $2}' | awk 'BEGIN{FS=""}{print $1}' | sort | uniq | 
xargs -L1 -I% sudo python ../bigquery_etl/load/load_data_from_file.py -t=NEWLINE_DELIMITED_JSON isb-cgc 2016_05_03_tcga_data_open CNV schemas/cnv.json  gs://2016-05-03-isb-cgc-open/tcga-runs/intermediary/cnv/bq_data_files/%* 
