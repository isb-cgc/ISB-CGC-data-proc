'''
Created on Dec 28, 2016

opyright 2017, Institute for Systems Biology.

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

from concurrent import futures

from gdc.test.test_setup import GDCTestSetup
from gdc.util.process_data_type import set_uploaded_path

class GDCProcessDataTypeTest(GDCTestSetup):
    def setUp(self):
        super(GDCProcessDataTypeTest, self).setUp()

    def test_set_uploaded_path(self):
        future2project = {}
        with futures.ThreadPoolExecutor(max_workers=16) as executor:
            for project in self.config['tcga_project_names_all']:
                self.log.info('processing project %s' % (project))
                future2project[executor.submit(set_uploaded_path, self.config, 'current', 'TCGA', project, 'Aligned Reads', self.log)] = project

        future_keys = future2project.keys()
        while future_keys:
            future_done, future_keys = futures.wait(future_keys, return_when = futures.FIRST_EXCEPTION)
            for future in future_done:
                project = future2project.pop(future)
                if future.exception() is not None:
                    self.log.exception('%s generated an exception--%s: %s' % (project, type(future.exception()).__name__, future.exception()))
                else:
                    future.result()
                    self.log.info('finished project %s' % (project))
