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
            output.write('Field value report:\n')
            templates = config['field_report']['url_templates']
            for index, template in enumerate(templates):
                count = 0
                log.info('\tfinding field values for base url \'%s\'' % (template.split('%')[0]))
                output.write('%s\tfinding field values for base url \'%s\'\n' % ('' if 0 == index else '\n', template.split('%')[0]))
                for field in lines:
                    try:
                        field = field.rstrip()
                        if not field.startswith(' '):
                            endpoint = field.split(' ')[0]
                            log.info('\t\tstart endpoint \'%s\'' % (endpoint))
                            output.write('%s\t\tendpoint \'%s\'\n' % ('' if 0 == count else '\n', endpoint))
                            continue
                        field = field.lstrip()
                        response = requests.get(template % (endpoint, field))
                        json = response.json()
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
                                output.write('\t\t\t\t<no values>\n')
                            elif too_many:
                                output.write('\t\t\t\tfirst values of greater than %s: %s\n' % (len(buckets), ', '.join('%s(%s)' % (bucket['key'], bucket['doc_count']) for bucket in buckets[:3])))
                            else:
                                output.write('\t\t\t\tvalues(%d):\n\t\t\t\t\t%s\n' % (len(buckets), '\n\t\t\t\t\t'.join('%s(%s)' % (bucket['key'], bucket['doc_count']) for bucket in buckets)))
                                    
                        count += 1
                    except:
                        log.exception('problem for field %s on line %s: %s' % (field, count, json))
                        raise

        log.info('finished create_field_report()')
    except:
        if log:
            log.exception('problem with creating the field report')
        raise

if __name__ == '__main__':
    create_field_report(sys.argv[1])

