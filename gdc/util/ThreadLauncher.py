'''
Created on Jun 24, 2017

Copyright 2017, Institute for Systems Biology.

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

def launch_threads(config, tag, thread_info, log):
    try:
        future2thread = {}
        fn = thread_info['fn']
        with futures.ThreadPoolExecutor(max_workers=config['program_threads']) as executor:
            for params in thread_info[tag]['params']:
                log.info('\tprocessing call %s' % (params[0]))
                future2thread[executor.submit(fn, *params)] = params[0]

        results = {}
        future_keys = future2thread.keys()
        while future_keys:
            future_done, future_keys = futures.wait(future_keys, return_when = futures.FIRST_COMPLETED)
            for future in future_done:
                project = future2thread.pop(future)
                if future.exception() is not None:
                    log.exception('\t%s generated an exception--%s: %s' % (project, type(future.exception()).__name__, future.exception()))
                    raise future.exception()
                else:
                    results.update(future.result())
                    log.info('\tfinished project %s' % (project))
        log.info('finished processing threads for %s' % (tag))
        return results
    except:
        log.exception('problem processing threads for %s' % (tag))
        raise

if __name__ == '__main__':
    pass