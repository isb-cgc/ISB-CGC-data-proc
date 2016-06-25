'''
Created on May 11, 2016

script to process the maf files listed in the input file.  this is to capture maf files
the pipeline script misses

@author: michael
'''
import json
import sys
import traceback

import transform

def main(config, inputfilename):
    '''
    function to take the list of maf file inputs from the input file and process
    them using transform.generate_oncotator_inputfiles()
    
    parameters:
        config: configuration file for settings
        inputfilename:  input file with keypaths into GCS for the maf files to process
    '''
    config = json.load(open(sys.argv[1]))
    project_id = config['project_id']
    bucket_name = config['buckets']['open']
    
    # get required headers/columns from the file
    # we need to preserve the order too
    # IMPORTANT: notice the lowercase column heading names.
    #   Through out the script we use lowecase heading names
    oncotator_columns = [line.rstrip('\n').lower() for line in config['maf']['oncotator_input_columns']]
    oncotator_input_files_dest = config['maf']['oncotator_input_files_dest']
    
    with open(inputfilename) as inputs:
        for input in inputs:
            input = input.strip()
            print 'processing \'%s\'' % (input)
            outputfilename =  oncotator_input_files_dest + input[input.rindex('/') + 1:].replace(".maf", ".txt")
            try:
                transform.generate_oncotator_inputfiles(project_id, bucket_name, input, outputfilename, oncotator_columns)
            except Exception as e:
                print 'problem processing %s: %s' % (outputfilename, e)
                traceback.print_exc(6)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])

