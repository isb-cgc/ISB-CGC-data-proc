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

def create_field_report(configfilename):
    log = None
    try:
        with open(configfilename) as configFile:
            config = js.load(configFile)
        
        log_dir = str(date.today()).replace('-', '_') + '_log' + '/'
        log_name = create_log(log_dir, 'field_report')
        log = logging.getLogger(log_name)
        log.info('start create_field_report()')
        
        with open(config['field_report']['field_file']) as fields_file:
            lines = fields_file.read().split('\n')
            
        mod_count = (len(lines) - 4) / 20
        with open(config['field_report']['output_file'], 'w') as output:
            template2field2values = {}
            output.write('Field value report:\n')
            templates = config['field_report']['url_templates']
            for index, template in enumerate(templates):
                field2values = template2field2values.setdefault(template, {})
                count = 0
                log.info('\tfinding field values for base url \'%s\'' % (template.split('%')[0]))
                output.write('%s\tfinding field values for base url \'%s\'\n' % ('' if 0 == index else '\n', template.split('%')[0]))
                for field in lines:
                    try:
                        field = field.rstrip()
                        if not field.startswith(' '):
                            if 0 < len(field):
                                endpoint = field.split(' ')[0]
                                log.info('\t\tstart endpoint \'%s\'' % (endpoint))
                                output.write('%s\t\tendpoint \'%s\'\n' % ('' if 0 == count else '\n', endpoint))
                            continue
                        field = field.lstrip()
                        try:
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
                                continue
                        if 0 == count % mod_count:
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
                                continue
                            else:
                                log.error('unknown response format: %s' % (json))
                                output.write('\t\t\t\tunknown response format: %s' % (json))
                        elif 'buckets' not in data['aggregations'][field]:
                            field_info = data['aggregations'][field]
                            if 'count' in field_info:
                                if 0 == field_info['count']:
                                    field2values[field] = {'counts': 'no values'}
                                    output.write('\t\t\t\t<no values>\n')
                                else:
                                    field2values[field] = {'count': field_info}
                                    output.write('\t\t\t\tnumeric--count: %s max: %s sum: %s avg: %s min: %s\n' % (field_info['count'], field_info['max'], field_info['sum'], field_info['avg'], field_info['min']))
                            else:
                                log.error('unknown aggregations response format: %s' % (json))
                                output.write('\t\t\t\tunknown aggregations response format: %s' % (json))
                        else:
                            buckets = data['aggregations'][field]['buckets']
                            too_many = False
                            if 40 < len(buckets):
                                too_many = True
                            if 0 == len(buckets) or (1 == len(buckets) and '_missing' == buckets[0]['key']):
                                field2values[field] = {'buckets': 'no values'}
                                output.write('\t\t\t\t<no values>\n')
                            elif too_many:
                                field2values[field] = {'buckets': 'many values'}
                                output.write('\t\t\t\tfirst values of greater than %s: %s\n' % (len(buckets), ', '.join('%s(%s)' % (bucket['key'], bucket['doc_count']) for bucket in buckets[:3])))
                            else:
                                field2values[field] = {'buckets': set(bucket['key'] for bucket in buckets)}
                                output.write('\t\t\t\tvalues(%d):\n\t\t\t\t\t%s\n' % (len(buckets), '\n\t\t\t\t\t'.join('%s(%s)' % (bucket['key'], bucket['doc_count']) for bucket in buckets)))
                                    
                        count += 1
                    except:
                        log.exception('problem for field %s on line %s: %s' % (field, count, json))
                        raise

            log.info('start field value comparison between regular and legacy endpoints')
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
                output.write('\tprocessing %s\n' % (field))
                regvalues = regfield2values[field]
                if field not in legfield2values:
                    onlyreg += [field]
                    continue
                legvalues = legfield2values[field]
                if 'count' in regvalues:
                    if 'count' in legvalues:
                        output.write('\t\t%s is a count field for both endpoints\n' % (field))
                        if 'no values' == regvalues['count'] and 'no values' != legvalues['count']:
                            reg_nocounts += [field]
                        elif 'no values' == legvalues['count'] and 'no values' != regvalues['count']:
                            leg_nocounts += [field]
                        else:
                            same_counts += [field]
                    elif 'buckets' in legvalues:
                        # this (not surprisingly) doesn't appear to happen
                        output.write('\t\t%s is a count field for the regular endpoint and a buckets field for legacy\n' % (field))
                if 'buckets' in regvalues:
                    if 'count' in legvalues:
                        # this (not surprisingly) doesn't appear to happen
                        output.write('\t\t%s is a buckets field for the regular endpoint and a count field for legacy\n' % (field))
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
                                output.write('\t\t\tthe regular endpoint has these additional values:\n\t\t\t\t%s\n' % 
                                    ('\n\t\t\t\t'.join(str(value) for value in (regvalues['buckets'] - legvalues['buckets']))))
                                equiv = False
                            if 0 < len(legvalues['buckets'] - regvalues['buckets']):
                                output.write('\t\t\tthe legacy endpoint has these additional values:\n\t\t\t\t%s\n' % 
                                    ('\n\t\t\t\t'.join(str(value) for value in (legvalues['buckets'] - regvalues['buckets']))))
                                equiv = False
                            if equiv:
                                same_buckets += [field]
                            else:
                                output.write('\t\t\tthe regular and legacy endpoint share these values:\n\t\t\t\t%s\n' % 
                                    ('\n\t\t\t\t'.join(str(value) for value in (legvalues['buckets'] - regvalues['buckets']))))
                        else:
                            if 'no values' == regvalues['buckets']:
                                same_buckets_no += [field]
                            elif 'many values' == regvalues['buckets']:
                                same_buckets_many += [field]
                            else:
                                raise ValueError('unexpected case for %s: %s %s' % (field, regvalues['buckets'], legvalues['buckets']))
         
            if ( 0 < len(same_counts)):
                output.write('\tcount fields for both endpoints:\n\t\t%s\n' % ('\n\t\t'.join(same_counts)))
            if ( 0 < len(reg_nocounts)):
                output.write('\tregular endpoints that have no count values but the legacy does:\n\t\t%s\n' % ('\n\t\t'.join(reg_nocounts)))
            if ( 0 < len(leg_nocounts)):
                output.write('\tlegacy endpoints that have no values but the regular does:\n\t\t%s\n' % ('\n\t\t'.join(leg_nocounts)))

            if ( 0 < len(same_buckets)):
                output.write('\teqivalent bucket fields for both endpoints:\n\t\t%s\n' % ('\n\t\t'.join(same_buckets)))
            if ( 0 < len(same_buckets_no)):
                output.write('\tbucket fields with no values for both endpoints:\n\t\t%s\n' % ('\n\t\t'.join(same_buckets_no)))
            if ( 0 < len(same_buckets_many)):
                output.write('\tbucket fields with many values for both endpoints:\n\t\t%s\n' % ('\n\t\t'.join(same_buckets_many)))
            if ( 0 < len(reg_nobucketvalues)):
                output.write('\tregular endpoints that have no bucket values but the legacy does:\n\t\t%s\n' % ('\n\t\t'.join(reg_nobucketvalues)))
            if ( 0 < len(leg_nobucketvalues)):
                output.write('\tlegacy endpoints that have no bucket but the regular does:\n\t\t%s\n' % ('\n\t\t'.join(leg_nobucketvalues)))
            if ( 0 < len(reg_nomanybucketvalues)):
                output.write('\tregular endpoints that don\'t have many bucket values but the legacy does:\n\t\t%s\n' % ('\n\t\t'.join(reg_nomanybucketvalues)))
            if ( 0 < len(leg_nomanybucketvalues)):
                output.write('\tlegacy endpoints that don\'t have many bucket values but the regular does:\n\t\t%s\n' % ('\n\t\t'.join(leg_nomanybucketvalues)))

            output.write('\tfields only in regular endpoint:\n\t\t%s\n' % '\n\t\t'.join(onlyreg))
            onlyleg = []
            for field in legfield2values:
                if field not in regfield2values:
                    onlyleg += [field]
            output.write('\tfields only in legacy endpoint:\n\t\t%s\n' % '\n\t\t'.join(onlyleg))
            log.info('finished field value comparison between regular and legacy endpoints')

        log.info('finished create_field_report()')
    except:
        if log:
            log.exception('problem with creating the field report')
        raise

if __name__ == '__main__':
    create_field_report(sys.argv[1])

