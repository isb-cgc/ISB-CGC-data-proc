'''
Created on June 20, 2016

Uploads the GDC files specified by the config file into Google Cloud Storage and saves
to the metadata tables

Copyright 2016, Institute for Systems Biology.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

@author: michael
'''
from argparse import ArgumentParser
from concurrent import futures
from datetime import date, datetime
import json
import logging
import requests

from gdc.util.process_cases import process_cases

import gcs_wrapper
from util import create_log, import_module, upload_etl_file, print_list_synopsis

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

executor = None

projects_fields = set()
cases_fields = []
files_fields = []

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def parseargs():
    # Setup argument parser
    parser = ArgumentParser()
    parser.add_argument("config", help="config file to get information and settings for the gdc upload")

    # Process arguments
    return parser.parse_args()

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def process_project(config, project, log_dir):
    try:
        project_id = project['project_id']
        log_dir += project_id + '/'
        log_name = create_log(log_dir, project_id)
        log = logging.getLogger(log_name)
        log.info('begin process_project(%s)' % (project_id))
        
        log.info('\tprocess cases for %s' % (project_id))
        case2info = process_cases(config, project_id, log_dir)
        log.info('\tcomplete process cases for %s' % (project_id))
        
        
        future2type = {}
        log.info('finished process_project(%s)' % (project_id))
        return case2info
    except:
        log.exception('problem processing project %s' % (project_id))
        raise
    
## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def process_program(config, program_name, projects, log_dir):
    try:
        log_dir += program_name + '/'
        log_name = create_log(log_dir, program_name)
        log = logging.getLogger(log_name)
        log.info('begin process_program(%s)' % (program_name))
        future2project = {}
        for project in projects:
            if project in config['skip_tumor_types']:
                log.info('\tskipping project %s' % (project['project_id']))
                continue
            if 'all' == config['project_names'][0] or project['project_id'] in config['project_names']:
                log.info('\tprocessing project %s' % (project['project_id']))
                future2project[executor.submit(process_project, config, project, log_dir)] = project['project_id']
            else:
                log.info('\tnot processing project %s' % (project['project_id']))
     
        future_keys = future2project.keys()
        while future_keys:
            future_done, future_keys = futures.wait(future_keys, return_when = futures.FIRST_COMPLETED)
            for future in future_done:
                project_id = future2project.pop(future)
                if future.exception() is not None:
                    log.exception('\t%s generated an exception--%s: %s' % (project_id, type(future.exception()).__name__, future.exception()))
                else:
#                     result = future.result()
                    log.info('\tfinished project %s' % (project_id))
    
        log.info('finished process_program(%s)' % (program_name))
    except:
        log.exception('problem processing program %s' % (program_name))
        raise

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def get_program_info (projects_endpt, program_name, log):

    log.info("\t\t>>> in get_project_info ... %s %s" % (projects_endpt, program_name))

    filt = { 'op': '=',
             'content': {
                 'field': 'program.name',
                 'value': [program_name] 
              } 
           } 

    params = { 'filters': json.dumps(filt) }
    params['from'] = 1
    params['size'] = 1000

    log.info("\t\tget request %s %s" % (projects_endpt, params))
    response = requests.get ( projects_endpt, params=params )
    rj = response.json()
    print_list_synopsis(rj['data']['hits'], '\t\tprojects for %s:' % (program_name), log)

    return rj['data']['hits']

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def process_programs(config, log_dir, log):
    log.info('begin process_programs()')
    future2program = {}
    for program_name in config['program_names']:
        projects = get_program_info (config['projects_endpt']['endpt'] + config['projects_endpt']['query'], program_name, log)
        future2program[executor.submit(process_program, config, program_name, projects, log_dir)] = program_name
    
    future_keys = future2program.keys()
    while future_keys:
        future_done, future_keys = futures.wait(future_keys, return_when = futures.FIRST_COMPLETED)
        for future in future_done:
            program = future2program.pop(future)
            if future.exception() is not None:
                log.exception('\t%s generated an exception--%s:%s' % (program, type(future.exception()).__name__, future.exception()))
            else:
#                 result = future.result()
                log.info('\tfinished program %s' % (program))
    log.info('finished process_programs()')

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def uploadGDC():
    print datetime.now(), 'begin uploadGDC()'
    global executor
    try:
        args = parseargs()
        with open(args.config) as configFile:
            config = json.load(configFile)
        
        log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
        log_name = create_log(log_dir, 'top_processing')
        log = logging.getLogger(log_name)
        log.info('begin uploadGDC()')
        
        executor = futures.ThreadPoolExecutor(max_workers=config['threads'])
        
        module = import_module(config['database_module'])
        module.ISBCGC_database_helper.initialize(config, log)
     
        if config['upload_files'] or config['upload_etl_files']:
            # open the GCS wrapper here so it can be used by all the projects/platforms to save files
            gcs_wrapper.open_connection()

        process_programs(config, log_dir, log)
    finally:
        if executor:
            executor.shutdown(wait=False)
        if gcs_wrapper:
            gcs_wrapper.close_connection()
    log.info('finished uploadGDC()')
    print datetime.now(), 'finished uploadGDC()'

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

if __name__ == '__main__':
    uploadGDC()

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
