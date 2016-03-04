* Create the Antibody - Protein - Gene map
  * The scripts are in the folder - tcga_etl_pipeline/protein/antibody-gene-protein-map
  * download HGNC approved gene symbols file 
  ```
  curl -H"Accept:application/json" http://rest.genenames.org/fetch/status/Approved > hgnc_approved_1082015.json
  ```

  * Check  antibody annotation files
   * change the location in the script to point to folder containing antibody-annotation files.  
  ```
  python 1_fix_aa_files.py
  ```

  * Fix gene protein inconsistencies; add HGNC validation
  ```
  python 2_fix_gene_protein_incosistencies.py hgnc_approved.json 
  ```

* (main) Run the ETL
 * Run using the main pipeline
 * working dir: tcga_etl_pipeline
 ```
 python pipeline.py protein config/data_etl.json 
 ```
