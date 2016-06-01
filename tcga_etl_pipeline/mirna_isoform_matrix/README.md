perl expression_matrix_mimat.pl -m tcga_mirna_bcgsc_hg19.adf -o hiseq -p /mnt/datadisk-3/isoform_files/IlluminaHiSeq/ -n IlluminaHiSeq_miRNASeq

sudo python melt_matrix.py files/expn_matrix_mimat_norm_IlluminaHiSeq_miRNASeq.txt

