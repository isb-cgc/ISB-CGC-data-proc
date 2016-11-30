'''
Created on Jul 7, 2016

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
from datetime import date
import json as js
import logging
import requests
import sys
import time

from util import create_log


def get_values_for_field(endpoint, field, field2values, output, template, print_status, log):
    try:
        field = '.'.join(field.split('.')[1:])
        response = requests.get(template % (endpoint, field))
        json = response.json()
    except Exception as e:
        retry = 3
        while 0 < retry:
            time.sleep(1)
            retry -= retry
            log.warning('retrying request for %s because of %s' % (field, e))
            response = requests.get(template % (endpoint, field))
            json = response.json()
            break
        if 0 == retry:
            log.exception('problem getting value from response for field %s: %s' % (field, response))
            return
    response.raise_for_status()
    if print_status:
        log.info('\t\t\tfinding values for field \'%s\'' % (field))
    data = json['data']
    output.write('\t\t\t%s\n' % (field))
    if 'aggregations' not in data:
        if 'warnings' in json:
            if 'facets' in json['warnings']:
                if 'unrecognized values: ' in json['warnings']['facets']:
                    output.write('\t\t\t\tfield is not valid\n')
                else:
                    output.write('\t\t\t\tfacets warning: %s\n' % (json['warnings']['facets']))
            else:
                output.write('\t\t\t\tgeneral warning: %s\n' % (json['warnings']))
                return
        else:
            log.error('unknown response format: %s' % (json))
            output.write('\t\t\t\tunknown response format: %s' % (json))
    elif 'buckets' not in data['aggregations'][field]:
        field_info = data['aggregations'][field]
        if 'count' in field_info:
            if 0 == field_info['count']:
                field2values[field] = {'counts':'no values'}
                output.write('\t\t\t\t<no values>\n')
            else:
                field2values[field] = {'count':field_info}
                output.write('\t\t\t\tnumeric--count: %s max: %s sum: %s avg: %s min: %s\n' % (field_info['count'], field_info['max'], field_info['sum'], field_info['avg'], field_info['min']))
        else:
            log.error('unknown aggregations response format: %s' % (json))
            output.write('\t\t\t\tunknown aggregations response format: %s' % (json))
    else:
        buckets = data['aggregations'][field]['buckets']
        too_many = False
        if 60 < len(buckets):
            too_many = True
        if 0 == len(buckets) or (1 == len(buckets) and '_missing' == buckets[0]['key']):
            field2values[field] = {'buckets':'no values'}
            output.write('\t\t\t\t<no values>\n')
        elif too_many:
            field2values[field] = {'buckets':'many values'}
            output.write('\t\t\t\tfirst values of greater than %s: %s\n' % (len(buckets), ', '.join('%s(%s)' % (bucket['key'], bucket['doc_count']) for bucket in buckets[:3])))
        else:
            field2values[field] = {'buckets':set(bucket['key'] for bucket in buckets)}
            output.write('\t\t\t\tvalues(%d):\n\t\t\t\t\t%s\n' % (len(buckets), '\n\t\t\t\t\t'.join('%s(%s)' % (bucket['key'], bucket['doc_count']) for bucket in buckets)))
    return field

def create_field_report(configfilename):
    log = None
    try:
        with open(configfilename) as configFile:
            config = js.load(configFile)
        
        log_dir = str(date.today()).replace('-', '_') + '_log' + '/'
        log_name = create_log(log_dir, 'field_report')
        log = logging.getLogger(log_name)
        log.info('start create_field_report()')
        
        endpts = config['field_report']['endpoints']
        # fetch the queryable fields
        output_path = config['field_report']['output_path']
        output_file = str(date.today()).replace('-', '_') + '_' + config['field_report']['output_file']
        with open(output_path + output_file, 'w') as output:
            for endpoint in endpts:
                output.write('Field value report:\n')
                log.info('\tstart endpoint \'%s\'' % (endpoint))
                output.write('\tendpoint \'%s\'\n' % (endpoint))
                template2field2values = {}
                mapping_templates = config['field_report']['endpoint_mapping_templates']
                url_templates = config['field_report']['url_templates']
                for mapping_template, url_template in zip(mapping_templates, url_templates):
                    response = requests.get(mapping_template % (endpoint))
                    response.raise_for_status()
                    fields = response.json()['_mapping'].keys()
                    fields.sort()

                    field2values = template2field2values.setdefault(url_template, {})
                    mod_count = len(fields) / 20
                    count = 0
                    log.info('\t\tfinding field values for base url \'%s\'' % (url_template.split('%')[0]))
                    output.write('\t\tfinding field values for base url \'%s\'\n' % (url_template.split('%')[0]))
                    log.info('\t\tgot information on the fields for %s.  found %s fields' % (endpoint, len(fields)))
                    output.write('\t\tfound %s fields\n' % (len(fields)))
                    for field in fields:
                        try:
                            progress = False if len(fields) < 50 else True if 0 == count else 0 == count % mod_count
                            get_values_for_field(endpoint, field, field2values, output, url_template, progress, log)
                        except:
                            log.exception('problem for field %s' % (field))
                            raise
                        count += 1
        
                log.info('start field value comparison between current and legacy endpoints')
                output.write('\nField value comparison report:\n')
                regfield2values = None
                legfield2values = None
                for url in template2field2values:
                    if 'legacy' in url:
                        legfield2values = template2field2values[url]
                    else:
                        regfield2values = template2field2values[url]
        
                onlyreg = []
                same_counts = []
                reg_nocounts = []
                leg_nocounts = []
                same_buckets = []
                same_buckets_no = []
                same_buckets_many = []
                reg_nobucketvalues = []
                leg_nobucketvalues = []
                reg_nomanybucketvalues = []
                leg_nomanybucketvalues = []
                for field in regfield2values:
                    regvalues = regfield2values[field]
                    if field not in legfield2values:
                        onlyreg += [field]
                        continue
                    legvalues = legfield2values[field]
                    if 'count' in regvalues:
                        if 'count' in legvalues:
                            output.write('\tprocessing %s\n' % (field))
                            output.write('\t\t%s is a count field for both endpoints\n' % (field))
                            if 'no values' == regvalues['count'] and 'no values' != legvalues['count']:
                                reg_nocounts += [field]
                            elif 'no values' == legvalues['count'] and 'no values' != regvalues['count']:
                                leg_nocounts += [field]
                            else:
                                same_counts += [field]
                        elif 'buckets' in legvalues:
                            # this (not surprisingly) doesn't appear to happen
                            output.write('\tprocessing eqiv and diffs for %s\n' % (field))
                            output.write('\t\t%s is a count field for the current endpoint and a buckets field for legacy\n' % (field))
                    if 'buckets' in regvalues:
                        if 'count' in legvalues:
                            # this (not surprisingly) doesn't appear to happen
                            output.write('\tprocessing %s\n' % (field))
                            output.write('\t\t%s is a buckets field for the current endpoint and a count field for legacy\n' % (field))
                        elif 'buckets' in legvalues:
                            if 'no values' == regvalues['buckets'] and 'no values' != legvalues['buckets']:
                                reg_nobucketvalues += [field]
                            elif 'no values' == legvalues['buckets'] and 'no values' != regvalues['buckets']:
                                leg_nobucketvalues += [field]
                            elif 'many values' == regvalues['buckets'] and 'many values' != legvalues['buckets']:
                                leg_nomanybucketvalues += [field]
                            elif 'many values' == legvalues['buckets'] and 'many values' != regvalues['buckets']:
                                reg_nomanybucketvalues += [field]
                            elif regvalues['buckets'] not in ('no values', 'many values') and legvalues['buckets'] not in ('no values', 'many values'):
                                equiv = True
                                if 0 < len(regvalues['buckets'] - legvalues['buckets']):
                                    output.write('\tprocessing eqiv and diffs for %s\n' % (field))
                                    output.write('\t\t\tthe current endpoint has these additional values:\n\t\t\t\t%s\n' % 
                                        ('\n\t\t\t\t'.join(str(value) for value in (regvalues['buckets'] - legvalues['buckets']))))
                                    equiv = False
                                if 0 < len(legvalues['buckets'] - regvalues['buckets']):
                                    if 0 == len(regvalues['buckets'] - legvalues['buckets']):
                                        output.write('\tprocessing eqiv and diffs for %s\n' % (field))
                                    output.write('\t\t\tthe legacy endpoint has these additional values:\n\t\t\t\t%s\n' % 
                                        ('\n\t\t\t\t'.join(str(value) for value in (legvalues['buckets'] - regvalues['buckets']))))
                                    equiv = False
                                if equiv:
                                    same_buckets += [field]
                                else:
                                    output.write('\t\t\tthe current and legacy endpoint share these values:\n\t\t\t\t%s\n' % 
                                        ('\n\t\t\t\t'.join(str(value) for value in (legvalues['buckets'] & regvalues['buckets']))))
                            else:
                                if 'no values' == regvalues['buckets']:
                                    same_buckets_no += [field]
                                elif 'many values' == regvalues['buckets']:
                                    same_buckets_many += [field]
                                else:
                                    raise ValueError('unexpected case for %s: %s %s' % (field, regvalues['buckets'], legvalues['buckets']))
             
                if ( 0 < len(same_counts)):
                    output.write('\tcount fields for both endpoints:\n\t\t%s\n' % ('\n\t\t'.join(sorted(same_counts))))
                if ( 0 < len(reg_nocounts)):
                    output.write('\tregular endpoints that have no count values but the legacy does:\n\t\t%s\n' % ('\n\t\t'.join(sorted(reg_nocounts))))
                if ( 0 < len(leg_nocounts)):
                    output.write('\tlegacy endpoints that have no values but the current does:\n\t\t%s\n' % ('\n\t\t'.join(sorted(leg_nocounts))))
        
                output.write('\n\tsummary comparsion of field counts:')
                if ( 0 < len(same_buckets)):
                    output.write('\t\teqivalent bucket fields for both endpoints:\n\t\t%s\n' % ('\n\t\t'.join(sorted(same_buckets))))
                if ( 0 < len(same_buckets_no)):
                    output.write('\t\tbucket fields with no values for both endpoints:\n\t\t%s\n' % ('\n\t\t'.join(sorted(same_buckets_no))))
                if ( 0 < len(same_buckets_many)):
                    output.write('\t\tbucket fields with many values for both endpoints:\n\t\t%s\n' % ('\n\t\t'.join(sorted(same_buckets_many))))
                if ( 0 < len(reg_nobucketvalues)):
                    output.write('\t\tregular endpoints that have no bucket values but the legacy does:\n\t\t%s\n' % ('\n\t\t'.join(sorted(reg_nobucketvalues))))
                if ( 0 < len(leg_nobucketvalues)):
                    output.write('\t\tlegacy endpoints that have no bucket but the current does:\n\t\t%s\n' % ('\n\t\t'.join(sorted(leg_nobucketvalues))))
                if ( 0 < len(reg_nomanybucketvalues)):
                    output.write('\t\tregular endpoints that don\'t have many bucket values but the legacy does:\n\t\t%s\n' % ('\n\t\t'.join(sorted(reg_nomanybucketvalues))))
                if ( 0 < len(leg_nomanybucketvalues)):
                    output.write('\t\tlegacy endpoints that don\'t have many bucket values but the current does:\n\t\t%s\n' % ('\n\t\t'.join(sorted(leg_nomanybucketvalues))))
        
                output.write('\t\tfields only in current endpoint:\n\t\t%s\n' % '\n\t\t'.join(sorted(onlyreg)))
                onlyleg = []
                for field in legfield2values:
                    if field not in regfield2values:
                        onlyleg += [field]
                output.write('\t\tfields only in legacy endpoint:\n\t\t%s\n' % '\n\t\t'.join(sorted(onlyleg)))
                log.info('finished field value comparison between current and legacy endpoints')

        log.info('finished create_field_report()')
    except:
        if log:
            log.exception('problem with creating the field report')
        raise

if __name__ == '__main__':
    create_field_report(sys.argv[1])

