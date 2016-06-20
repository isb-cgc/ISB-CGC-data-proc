from collections import OrderedDict


def select_aliquots(duplicate_aliquots):

    d = OrderedDict([
                      ('13:15', ['10'])
                    , ('19:20', ['D'])
                    , ('26:28', ['01', '08', '14', '09', '21', '30', '10', '12', '13', '31', '18', '25'])
                   ])

    for k, v in d.items():
        ki = k.strip().split(':')

        for y in v:
            aliquots_to_be_deleted = []
            for x in duplicate_aliquots:

#         for y in v:
                string = str(x[int(ki[0]):int(ki[1])])
            #print string, str(y)
                if string == str(y):
                    aliquots_to_be_deleted.append(x)
            if len(aliquots_to_be_deleted) == 1:
                return aliquots_to_be_deleted[0]
            else:
                continue

    # return default aliqout   
    default_aliquot = duplicate_aliquots[0]
    try:
        plate_ids = [int(x[21:25]) for x in duplicate_aliquots]
        plate_ids = sorted(plate_ids, reverse=True)
        default_aliquot = [x for x in duplicate_aliquots if str(plate_ids[0]) in x][0]
    except:
        pass
    return default_aliquot


def remove_maf_duplicates(df, sample_type_info, log):

 
    # Multiple tumor aliquots for one sample
    log.info('\t\tcheck for duplicate tumor barcodes.  dataframe size: %s' % (len(df)))
    tumor_sample_barcode_duplicates = df[df.duplicated('Tumor_SampleBarcode')]['Tumor_SampleBarcode'].unique()
    for dup in tumor_sample_barcode_duplicates:
        duplicate_aliquots = df[df['Tumor_SampleBarcode'] == dup]['Tumor_AliquotBarcode'].unique()
#        duplicate_aliquots_info = map(lambda x: sample_type_info.loc[x.split('-')[3][0:2]]['Definition'], duplicate_aliquots)
        if len(duplicate_aliquots) > 1:
 
            aliquot_to_keep =  select_aliquots(duplicate_aliquots)
#            print '\t'.join([dup, '; '.join(duplicate_aliquots), '; '.join(duplicate_aliquots_info), 'Multiple tumor aliquots for one sample', aliquot_to_keep])
            df = df[ ~
                   ((df['Tumor_SampleBarcode'] == dup)
                   &
                   (df['Tumor_AliquotBarcode'] != aliquot_to_keep))
                 ]
    log.info('\t\tfinished check for duplicate tumor barcodes.  dataframe size: %s' % (len(df)))

    # Multiple normal aliquots for one sample
    log.info('\t\tcheck for duplicate normal barcodes.  dataframe size: %s' % (len(df)))
    normal_sample_barcode_duplicates = df[df.duplicated('Normal_SampleBarcode')]['Normal_SampleBarcode'].unique()
    for dup in normal_sample_barcode_duplicates:
        duplicate_aliquots = df[df['Normal_SampleBarcode'] == dup]['Normal_AliquotBarcode'].unique()
#        duplicate_aliquots_info = map(lambda x: sample_type_info.loc[x.split('-')[3][0:2]]['Definition'], duplicate_aliquots)
        if len(duplicate_aliquots) > 1:
            aliquot_to_keep =  select_aliquots(duplicate_aliquots)
#            print '\t'.join([dup, '; '.join(duplicate_aliquots), '; '.join(duplicate_aliquots_info), 'Multiple normal aliquots for one sample', aliquot_to_keep])
            df = df[ ~
                   ((df['Normal_SampleBarcode'] == dup)
                   &
                   (df['Normal_AliquotBarcode'] != aliquot_to_keep))
                 ]
    log.info('\t\tfinished check for duplicate normal barcodes.  dataframe size: %s' % (len(df)))
          
    #---------------------------------------
    # Multiple normals for one tumor sample
    #---------------------------------------
    log.info('\t\tcheck for paired blood/tissue normal barcodes.  dataframe size: %s' % (len(df)))
    tumor_aliqout_barcode_duplicates = df[df.duplicated('Tumor_AliquotBarcode')]['Tumor_AliquotBarcode'].unique()
    for dup in tumor_aliqout_barcode_duplicates:
        duplicate_aliquots = df[df['Tumor_AliquotBarcode'] == dup]['Normal_AliquotBarcode'].unique()
    #    duplicate_aliquots_info = map(lambda x: sample_type_info.loc[x.split('-')[3][0:2]]['Definition'], duplicate_aliquots)
        if len(duplicate_aliquots) > 1:

            aliquot_to_keep =  select_aliquots(duplicate_aliquots)
    #       print '\t'.join([dup, '; '.join(duplicate_aliquots), '; '.join(duplicate_aliquots_info), 'Multiple normals for one tumor sample', aliquot_to_keep])

            df = df[ ~ 
                   ((df['Tumor_AliquotBarcode'] == dup) 
                   & 
                   (df['Normal_AliquotBarcode'] != aliquot_to_keep))
                 ]
    log.info('\t\tfinished check for paired blood/tissue normal barcodes.  dataframe size: %s' % (len(df)))

    return df 
