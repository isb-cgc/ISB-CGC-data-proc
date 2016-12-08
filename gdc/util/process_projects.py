'''
Created on Aug 10, 2016

@author: michael
'''
import logging

from gdc.util.gdc_util import get_map_rows, save2db
from util import close_log,create_log

def get_filter(program):
    return { 
            'op': '=',
            'content': {
                 'field': 'program.name',
                 'value': [program]
        }
    } 
    
def process_projects(config, endpt_type, program, log_dir):
    try:
        log_name = create_log(log_dir, program + '_' + 'project')
        log = logging.getLogger(log_name)

        log.info('begin process_projects for %s' % (program))
        project2info = get_map_rows(config, endpt_type, 'project', get_filter(program), log)
        program2info = {program: project2info[project2info.keys()[0]]}
        save2db(config, endpt_type, 'metadata_gdc_program', program2info, config['process_projects']['program_table_mapping'], log)
        save2db(config, endpt_type, 'metadata_gdc_project', project2info, config['process_projects']['project_table_mapping'], log)
        log.info('finished process_projects for %s' % (program))
        return project2info
    except:
        log.exception('problem processing projects for %s' % (program))
        raise
    finally:
        close_log(log)

def process_projects_for_programs(config, endpt_type, projects_endpt, log):
    try:
        log.info('begin process_projects_for_programs')
        project2info = get_map_rows(config, endpt_type, 'project', {}, log)
#         save2db(config, endpt_type, 'metadata_gdc_project', project2info, config['process_projects']['project_table_mapping'], log)
        log.info('finished process_projects_for_programs')
        return project2info
    except:
        log.exception('problem processing projects to get programs')
        raise

if __name__ == '__main__':
    pass