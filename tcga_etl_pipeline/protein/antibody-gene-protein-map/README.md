* download the latest HGNC approved gene symbols
curl -H"Accept:application/json" http://rest.genenames.org/fetch/status/Approved


1. First check all the Antibody annotattion for any missing columns, bad column names etc
   sudo python 1_fix_aa_files.py
   * change the location in the script to point to antibody-annotation files

2.  python 2_fix_gene_protein_incosistencies.py hgnc_approved_1082015.json
