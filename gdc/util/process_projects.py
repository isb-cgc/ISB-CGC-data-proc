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
        project2info = get_map_rows(config, endpt_type, 'project', program, get_filter(program), log)
        program2info = {program: project2info[project2info.keys()[0]]}
        if config['process_program']:
            save2db(config, endpt_type, 'metadata_program', program2info, config[program]['process_projects']['program_table_mapping'], log)
        else:
            log.warning('\n\t====================\n\tnot saving to db for programs this run!\n\t====================')
        if config['process_project']:
            save2db(config, endpt_type, '%s_metadata_project' % program, project2info, config[program]['process_projects']['project_table_mapping'], log)
        else:
            log.warning('\n\t====================\n\tnot saving to db for projects this run!\n\t====================')
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
        project2info = get_map_rows(config, endpt_type, 'project', None, {}, log)
        log.info('finished process_projects_for_programs')
        return project2info
    except:
        log.exception('problem processing projects to get programs')
        raise

if __name__ == '__main__':
    pass