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

the folloowing changes in the api have occured in GDC release 8:
Old Node    Deprecated property    New Node    New Property        
diagnosis    days_to_death    demographic    days_to_death        
diagnosis    cause_of_death    demographic    cause_of_death        
diagnosis    hiv_positive    followup    hiv_positive        
diagnosis    ldh_normal_range_upper    followup    ldh_normal_range_upper        
diagnosis    progression_or_recurrence    followup    progression_or_recurrence        
diagnosis    days_to_recurrence    followup    days_to_recurrence        
diagnosis    days_to_last_follow_up    followup    days_to_last_follow_up        
diagnosis    ldh_level_at_diagnosis    followup    ldh_level        
diagnosis    vital_status    demographic    vital_status        
diagnosis    days_to_birth    demographic    days_to_birth        
diagnosis    days_to_last_known_disease_status    NA (to be deleted)    NA (to be deleted)        
diagnosis    days_to_new_event    NA (to be deleted)    NA (to be deleted)        
exposure    height    followup    height        
exposure    weight    followup    weight        
exposure    bmi    followup    bmi        
exposure    cigarettes_per_day    NA (to be deleted)    NA (to be deleted)        
demographic    year_of_birth    NA (to be deleted)    NA (to be deleted)        
demographic    year_of_death    NA (to be deleted)    NA (to be deleted)        
project    disease_type    case    disease_type        
project    primary_site    case    primary_site
'''
from argparse import ArgumentParser
from concurrent import futures
from datetime import date, datetime
import json
import logging

from gdc.util.gdc_util import instantiate_etl_class
from gdc.util.gdc_util import request_facets_results
from gdc.util.process_annotations import call_metadata2annotation, process_annotations
from gdc.util.process_projects import process_projects_for_programs, process_projects
from gdc.util.process_cases import process_cases
from gdc.util.process_data_type import process_data_type
from gdc.util.process_images import process_images

from util import close_log, create_log, import_module

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

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

def process_project(config, endpt_type, program_name, project, log_dir):
    try:
        log_dir += project + '/'
        log_name = create_log(log_dir, project)
        log = logging.getLogger(log_name)
        
        log.info('begin process_project(%s)' % (project))
        
        case2info = {}
        if config['process_case']:
            
            log.info('\tprocess cases for %s' % (project))
            case2info = process_cases(config, endpt_type, program_name, project, log_dir)
            log.info('\tcompleted process cases for %s' % (project))
        else:
            log.warning('\n\t====================\n\tnot processing cases this run for %s!\n\t====================' % (project))
        
        file2info = {}
        if config['process_data_type']:
            with futures.ThreadPoolExecutor(max_workers=config['project_threads']) as executor:
                log.info('\tprocess data_types for %s' % (project))
                future2data_type = {}
                data_types = request_facets_results(config['files_endpt']['%s endpt' % (endpt_type)], config['facets_query'], 'data_type', log)
                for data_type in data_types:
                    if ((len(config['data_type_restrict']) == 0 or data_type in config['data_type_restrict']) and 
                          (data_type in config['data_type2isb_label'] or data_type in config['data_type_gcs'])):
                        log.info('\t\tprocess data_type \'%s\' for %s' % (data_type, project))
                        future2data_type[executor.submit(process_data_type, config, endpt_type, program_name, project, data_type, log_dir)] = data_type
                    else:
                        log.info('\t\tnot processing data_type %s for %s' % (data_type, project))
             
                retry_ct = 0
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
                                log.warning('\tWARNING: resubmitting %s--%s:%s.  try %s' % (data_type, type(future.exception()).__name__, future.exception(), retry_ct))
                                new_future = executor.submit(process_data_type, config, endpt_type, program_name, project, data_type, log_dir, project + '_' + data_type.replace(' ', '') + '_%d' % (retry_ct))
                                future2data_type[new_future] = data_type
                            else:
                                log.info('\t\tfinished process data_type \'%s\' for %s' % (data_type, project))
                                file2info = future.result()
                                future_keys = future2data_type.keys()
                    except:
                        future_keys = future2data_type.keys()
                        log.exception('%s failed for %s' % (data_type, project))
                log.info('\tcompleted process data_types for %s' % (project))
        else:
            log.warning('\n\t====================\n\tnot processing data types this run for %s!\n\t====================' % (project))
        
        log.info('finished process_project(%s)' % (project))
        return case2info, file2info
    except:
        log.exception('problem processing project %s' % (project))
        raise
    
## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def initialize_etl(config, program_name, log):
    if config['upload_etl_files']:
        for data_type in config[program_name]['process_files']['datatype2bqscript'].keys():
            if (len(config['data_type_restrict']) == 0 or data_type in config['data_type_restrict']):
                instantiate_etl_class(config, program_name, data_type, log).initialize(config, program_name, log)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def finalize_etl(config, program_name, log):
    if config['upload_etl_files']:
        for data_type in config[program_name]['process_files']['datatype2bqscript'].keys():
            if (len(config['data_type_restrict']) == 0 or data_type in config['data_type_restrict']):
                instantiate_etl_class(config, program_name, data_type, log).finalize(config, program_name, log)

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def process_program(config, endpt_type, program_name, projects, log_dir):
    try:
        log_dir += program_name + '_%s' % endpt_type + '/'
        log_name = create_log(log_dir, program_name)
        log = logging.getLogger(log_name)
        log.info('begin process_program(%s)' % (program_name))

        future2project = {}
        initialize_etl(config, program_name, log)
        with futures.ThreadPoolExecutor(max_workers=config['program_threads']) as executor:
            for project in projects:
                if project in config['skip_projects']:
                    log.info('\tskipping project %s' % (project))
                    continue
                if 0 == len(config['project_name_restrict']) or project in config['project_name_restrict']:
                    log.info('\tprocessing project %s' % (project))
                    future2project[executor.submit(process_project, config, endpt_type, program_name, project, log_dir)] = project
                else:
                    log.info('\tnot processing project %s' % (project))
 
        future_keys = future2project.keys()
        while future_keys:
            future_done, future_keys = futures.wait(future_keys, return_when = futures.FIRST_COMPLETED)
            for future in future_done:
                project = future2project.pop(future)
                if future.exception() is not None:
                    log.exception('\t%s generated an exception--%s: %s' % (project, type(future.exception()).__name__, future.exception()))
                else:
                    future.result()
                    log.info('\tfinished project %s' % (project))
        finalize_etl(config, program_name, log)
    
        log.info('finished process_program(%s)' % (program_name))
    except:
        log.exception('problem processing program %s' % (program_name))
        raise
    finally:
        close_log(log)

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def get_program_info(config, endpt_type, projects_endpt, program_name, log_dir, log):

    log.info("\t\tin get_project_info: %s %s" % (program_name, projects_endpt))
    project2info = process_projects(config, endpt_type, program_name, log_dir + program_name + '/')
    return project2info.keys()

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def get_programs(config, endpt_type, projects_endpt, log):

    log.info("\t\tin get_programs: %s" % (projects_endpt))
    project2info = process_projects_for_programs(config, endpt_type, projects_endpt, log)
    programs = set()
    for info in project2info.itervalues():
        programs.add(info['program']['name'])
    log.info("\t\tfinished get_programs: %s" % (programs))
    return programs

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def process_programs(config, endpt_type, log_dir, log):
    try:
        log.info('begin process_programs()')
        programs = get_programs(config, endpt_type, log_dir, log)
        for program_name in programs:
            if 0 == len(config['program_name_restrict']) or program_name in config['program_name_restrict']:
                log.info('\tstart program %s' % (program_name))
                projects = get_program_info(config, endpt_type, config['projects_endpt']['%s endpt' % (endpt_type)] + config['projects_endpt']['query'], program_name, log_dir, log)
                process_program(config, endpt_type, program_name, projects, log_dir)
                log.info('\tfinished program %s' % (program_name))
                
        log.info('finished process_programs()')
    except:
        log.exception('problem processing programs')
        raise

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def initializeDB(config, log):
    module = import_module(config['database_module'])
    helper = module.ISBCGC_database_helper
    helper.initialize(config, log)
    
    # populate the data_availability tables
    if config['update_schema']:
        isb_labels = set(config['data_type2isb_label'].values())
        for build in config['genomic_builds']:
            params = [[build, isb_label] for isb_label in isb_labels]
            for program_name in config['program_name_restrict']:
                helper.column_insert(config, params, '%s_metadata_data_type_availability' % (program_name), ['genomic_build', 'isb_label'], log)
    
## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def process_run_param_option(config, adjust, option, msg, paramtype):
    if 'int' == paramtype:
        try:
            config[option] = int(adjust)
            return True
        except:
            print 'not an integer, retry'
            return False
    elif adjust in ['f', 'q']:
        return True
    elif 't' == adjust:
        config[option] = True
        return True
    else:
        print 'unknown choice, retry' 
        return False
    
## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def process_run_param_options(config, adjust, options, msg, paramtype):
    if 't' == adjust:
        for option in options:
            done = False
            print '%s is set to %s' % (option.replace('_', ' '), config[option])
            while not done:
                adjust = raw_input(msg % (option.replace('_', ' ')))
                done = process_run_param_option(config, adjust[0], option, msg, paramtype)
                if 'q' == adjust[0]:
                    return True 
    elif 'f' == adjust:
        print 'not adjusting'
    else:
        print 'unrecoginized option, retry'
        return False
    return True
    
## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def finalize(config, log):
    if config['process_annotation']:
        call_metadata2annotation(config, log)

    if config['process_case'] and config['process_metadata_attrs']:
        for program_name in config['program_names']:
            if 0 == len(config['program_name_restrict']) or program_name in config['program_name_restrict']:
                postproc_module = import_module(config[program_name]['process_cases']['postproc_case']['postproc_module'])
                postproc_module.process_metadata_attrs(config, log)
    
    if 'process_images' in config and config['process_images']:
        process_images(config, log)
    
## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def uploadGDC():
    print datetime.now(), 'begin uploadGDC()'

    gcs_wrapper = None
    try:
        args = parseargs()
        with open(args.config) as configFile:
            config = json.load(configFile)
        
        log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
        log_name = create_log(log_dir, 'top_processing')
        log = logging.getLogger(log_name)
        
        log.info('begin uploadGDC()')
        
        initializeDB(config, log)
     
        if config['upload_files'] or config['upload_etl_files']:
            # open the GCS wrapper here so it can be used by all the projects/platforms to save files
            gcs_wrapper = import_module(config['gcs_wrapper'])
            gcs_wrapper.open_connection(config, log)
    
        for endpt_type in config['endpt_types']:
            log.info('processing %s endpoints' % (endpt_type))
            if config['process_annotation']:
                process_annotations(config, endpt_type, log_dir)
            else:
                log.warning('\n\t====================\n\tnot processing annotations this run!\n\t====================')
            process_programs(config, endpt_type, log_dir, log)
        finalize(config, log)
    except:
        raise
    finally:
        if gcs_wrapper:
            gcs_wrapper.close_connection()

    log.info('finished uploadGDC()')
    print datetime.now(), 'finished uploadGDC()'

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

if __name__ == '__main__':
    uploadGDC()

## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
