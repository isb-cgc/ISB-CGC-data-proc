'''
Created on Dec 28, 2016

@author: michael
'''
from cPickle import load
from os import listdir

from gdc.etl.methylation import Methylation
from gdc.etl.mirna_matrix import miRNA_matrix
from gdc.test.test_setup import GDCTestSetup

class GDCETLTest(GDCTestSetup):
    def setUp(self):
        super(GDCETLTest, self).setUp()
        
    def test_etl_finalize(self):
#         Methylation(self.config).finalize(self.config, self.log)
        miRNA_matrix(self.config).finalize(self.config, self.log)
        
    def test_etl_melt(self):
        mapfile_path = self.config['download_base_output_dir'] + self.config['process_files']['datatype2bqscript']['Isoform Expression Quantification']['matrix_persist_subdir']
        mapfiles = listdir(mapfile_path)
        master_aliquot2info = {}
        for mapfile in mapfiles:
            with open(mapfile_path + mapfile, 'rb') as pickle:
                curmap = load(pickle)
            master_aliquot2info.update(curmap)
        aliquot2info = {}
        with open('tcga_etl_pipeline/mirna_isoform_matrix/hiseq/expn_matrix_mimat_norm_IlluminaHiSeq_miRNASeq_small.txt') as mimat:
            aliquots = mimat.readline().split('\t')
            for aliquot in aliquots[1:]:
                aliquot = aliquot.strip()
                aliquot =  aliquot.strip(".mirbase20")
                aliquot =  aliquot.strip(".hg19")
                try:
                    info = master_aliquot2info.popitem()[1]
                except:
                    pass
                aliquot2info[aliquot] = info
            
        miRNAmatrix = miRNA_matrix(self.config)
        miRNAmatrix.melt_matrix('tcga_etl_pipeline/mirna_isoform_matrix/hiseq/expn_matrix_mimat_norm_IlluminaHiSeq_miRNASeq_small.txt', 'IlluminaHiSeq', aliquot2info, self.config, self.log)
        miRNAmatrix.load_isoform_matrix(self.config, 'WRITE_EMPTY', self.log)

