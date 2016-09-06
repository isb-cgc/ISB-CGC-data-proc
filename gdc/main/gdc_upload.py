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
from multiprocessing import Semaphore

from gdc.util.gdc_util import request_facets_results
from gdc.util.process_annotations import process_annotations
from gdc.util.process_projects import process_projects
from gdc.util.process_cases import process_cases
from gdc.util.process_data_type import process_data_type

import gcs_wrapper
from util import create_log, import_module

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

semaphore = Semaphore(20)

projects_fields = set()
cases_fields = []
files_fields = []

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def parseargs():
    # Setup argument parser
    parser = ArgumentParser()
    parser.add_argument("config", help="config file to get information and settings for the gdc upload")

    # Process and return arguments
    return parser.parse_args()

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def process_project(config, project, log_dir):
    try:
        log_dir += project + '/'
        log_name = create_log(log_dir, project)
        log = logging.getLogger(log_name)
        
        log.info('begin process_project(%s)' % (project))
        
        case2info = {}
        if config['process_case']:
            log.info('\tprocess cases for %s' % (project))
            case2info = process_cases(config, project, log_dir)
            log.info('\tcompleted process cases for %s' % (project))
        else:
            log.warning('\n\t====================\n\tnot processing cases this run for %s!\n\t====================' % (project))
        
        if config['process_data_type']:
            with futures.ThreadPoolExecutor(max_workers=20) as executor:
                log.info('\tprocess data_types for %s' % (project))
                future2data_type = {}
                data_types = request_facets_results(config['files_endpt']['endpt'], config['facets_query'], 'data_type', log)
                for data_type in data_types:
                    if (len(config['data_type_restrict']) == 0 or data_type in config['data_type_restrict']):
                        with semaphore:
                            log.info('\t\tprocess data_type \'%s\' for %s' % (data_type, project))
                            future2data_type[executor.submit(process_data_type, config, project, data_type, log_dir)] = data_type
                    else:
                        log.info('\t\tnot processing data_type %s for %s' % (data_type, project))
             
                data_type2retry = {}
                future_keys = future2data_type.keys()
                while future_keys:
                    future_done, _ = futures.wait(future_keys, return_when = futures.FIRST_COMPLETED)
                    try:
                        for future in future_done:
                            data_type = future2data_type.pop(future)
                            if future.exception() is not None:
                                # TODO only retry on connection refused, not other exceptions
                                retry_ct = data_type2retry.setdefault(data_type, 0)
                                if retry_ct > 3:
                                    raise ValueError('%s failed multiple times--%s:%s' % (data_type, type(future.exception()).__name__, future.exception()))
                                data_type2retry[data_type] = retry_ct + 1
                                with semaphore:
                                    log.warning('\tWARNING: resubmitting %s--%s:%s.  try %s' % (data_type, type(future.exception()).__name__, future.exception(), retry_ct))
                                    new_future = executor.submit(process_data_type, config, project, data_type, log_dir, project + '_' + data_type.replace(' ', '') + '_%d' % (retry_ct))
                                    future2data_type[new_future] = data_type
                            else:
                                log.info('\t\tfinished process data_type \'%s\' for %s' % (data_type, project))
                                future_keys = future2data_type.keys()
                    except:
                        future_keys = future2data_type.keys()
                        log.exception('%s failed' % (data_type))
                log.info('\tcompleted process data_types for %s' % (project))
        else:
            log.warning('\n\t====================\n\tnot processing data types this run for %s!\n\t====================' % (project))
        
        log.info('finished process_project(%s)' % (project))
        return case2info
    except:
        log.exception('problem processing project %s' % (project))
        raise
    
## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def process_program(config, program_name, projects, log_dir):
    try:
        log_dir += program_name + '/'
        log_name = create_log(log_dir, program_name)
        log = logging.getLogger(log_name)
        log.info('begin process_program(%s)' % (program_name))
        future2project = {}
        if config['process_project']:
            with futures.ThreadPoolExecutor(max_workers=config['project_threads']) as executor:
                for project in projects:
                    if project in config['skip_projects']:
                        log.info('\tskipping project %s' % (project))
                        continue
                    if 0 == len(config['project_name_restrict']) or project in config['project_name_restrict']:
                        log.info('\tprocessing project %s' % (project))
                        future2project[executor.submit(process_project, config, project, log_dir)] = project
                    else:
                        log.info('\tnot processing project %s' % (project))
        else:
            log.warning('\n\t====================\n\tnot processing projects this run!\n\t====================')
     
        future_keys = future2project.keys()
        while future_keys:
            future_done, future_keys = futures.wait(future_keys, return_when = futures.FIRST_COMPLETED)
            for future in future_done:
                project = future2project.pop(future)
                if future.exception() is not None:
                    log.exception('\t%s generated an exception--%s: %s' % (project, type(future.exception()).__name__, future.exception()))
                else:
#                     result = future.result()
                    log.info('\tfinished project %s' % (project))
    
        log.info('finished process_program(%s)' % (program_name))
    except:
        log.exception('problem processing program %s' % (program_name))
        raise

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def get_program_info (config, projects_endpt, program_name, log_dir, log):

    log.info("\t\tin get_project_info: %s %s" % (program_name, projects_endpt))
    project2info = process_projects(config, program_name, log_dir + program_name + '/')
    return project2info.keys()

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def process_programs(config, log_dir, log):
    log.info('begin process_programs()')
    future2program = {}
    with futures.ProcessPoolExecutor(max_workers=config['processes']) as executor:
        for program_name in config['program_names']:
            log.info('\tstart program %s' % (program_name))
            projects = get_program_info(config, config['projects_endpt']['endpt'] + config['projects_endpt']['query'], program_name, log_dir, log)
            future2program[executor.submit(process_program, config, program_name, projects, log_dir)] = program_name
    
    future_keys = future2program.keys()
    while future_keys:
        future_done, future_keys = futures.wait(future_keys, return_when = futures.FIRST_COMPLETED)
        for future in future_done:
            program_name = future2program.pop(future)
            if future.exception() is not None:
                log.exception('\t%s generated an exception--%s:%s' % (program_name, type(future.exception()).__name__, future.exception()))
            else:
#                 result = future.result()
                log.info('\tfinished program %s' % (program_name))
    log.info('finished process_programs()')

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def initializeDB(config, log):
    module = import_module(config['database_module'])
    module.ISBCGC_database_helper.initialize(config, log)

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def uploadGDC():
    print datetime.now(), 'begin uploadGDC()'
    try:
        args = parseargs()
        with open(args.config) as configFile:
            config = json.load(configFile)
        
        log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
        log_name = create_log(log_dir, 'top_processing')
        log = logging.getLogger(log_name)

        log.info('begin uploadGDC()')
        
        initializeDB(config, log)
     
        if config['upload_open'] or config['upload_controlled'] or config['upload_etl_files']:
            # open the GCS wrapper here so it can be used by all the projects/platforms to save files
            gcs_wrapper.open_connection()

        if config['process_annotation']:
            process_annotations(config, log_dir)
        else:
            log.warning('\n\t====================\n\tnot processing annotations this run!\n\t====================')
        if config['process_program']:
            process_programs(config, log_dir, log)
        else:
            log.warning('\n\t====================\n\tnot processing programs this run!\n\t====================')
    finally:
        gcs_wrapper.close_connection()
    log.info('finished uploadGDC()')
    print datetime.now(), 'finished uploadGDC()'

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

if __name__ == '__main__':
    uploadGDC()

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
