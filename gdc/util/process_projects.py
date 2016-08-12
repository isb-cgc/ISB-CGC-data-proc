'''
Created on Aug 10, 2016

@author: michael
'''
import logging

from gdc.util.gdc_util import get_map_rows, save2db
from util import create_log

def get_filter(program):
    return { 
            'op': '=',
            'content': {
                 'field': 'program.name',
                 'value': [program]
        }
    } 
    
def process_projects(config, program, log_dir):
    try:
        log_name = create_log(log_dir, program + '_' + 'project')
        log = logging.getLogger(log_name)

        log.info('begin process_projects for %s' % (program))
        project2info = get_map_rows(config, 'project', get_filter(program), log)
        save2db(config, 'metadata_gdc_project', project2info, config['process_projects']['project_table_mapping'], log)
        log.info('finished process_projects for %s' % (program))

        return project2info
    except:
        log.exception('problem processing projects for %s' % (program))
        raise

if __name__ == '__main__':
    pass