'''
Created on Jul 27, 2016

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
from datetime import datetime
import json
import requests

if __name__ == '__main__':
    print '%s: starting upload of gdc files' % (datetime.now())
    print '\t%s: getting gdc status' % (datetime.now())
    status_endpt = 'https://gdc-api.nci.nih.gov/status'
    response = requests.get(status_endpt)
    print json.dumps(response.json(), indent=2)

    file_ids = []
    params = {'ids': file_ids}
    with open('main/2016_07_28_run/TCGA/TCGA-COAD/TCGA-COAD_miRNAExpressionQuantification_file_ids.txt') as file_id_file:
        for line in file_id_file:
            file_ids += [line.strip()]

    try:
        url = 'https://gdc-api.nci.nih.gov/data'
        headers = {'Content-Type': 'application/json'}
        print '\t%s: starting fetch of gdc files' % (datetime.now())
        retries = 0
        while True:
            try:
                response = requests.post(url, params=params, headers=headers, stream=True)
            except Exception as e:
                if retries < 2:
                    retries += 1
                    print '\t\t%s: request try %d on error %s' % (datetime.now(), retries, e)
                    continue
                raise
        print '\t%s: finished fetch of gdc files' % (datetime.now())
        
        response.raise_for_status()
        lines = response.text.split('\n')
        local_filename = "gdc_download.tar.gz"
        count = 1
        print '\t%s: starting write of gdc files' % (datetime.now())
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=2048): 
                if chunk: # filter out keep-alive new chunks
                    print '\t\tgot chunk %d' % (count)
                    count += 1
                    f.write(chunk)
        print '\t%s: finished write of gdc files' % (datetime.now())
        mirna2count = {}
        for line in lines:
            fields = line.split('\t')
            count = mirna2count.setdefault(fields[0], 0)
            mirna2count[fields[0]] = count + 1
        print '%s\n' % ('\n'.join('%s: %d' % (key, value) for key, value in mirna2count.iteritems()))
        print '%s: finished upload of gdc files' % (datetime.now())
    except:
        raise
