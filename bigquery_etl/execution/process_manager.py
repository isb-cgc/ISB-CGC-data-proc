#!/usr/bin/env python

# Copyright 2015, Institute for Systems Biology.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import concurrent.futures
import time
import pandas as pd
import sqlite3
import random

class ProcessManager(object):
    """Concurrent futures - Sqlite3 based task queue system;
    """
    def __init__(self, max_workers, db, table, resubmit=False, log=None):
        self.max_workers = max_workers
        self.db_filename = db
        self.table = table
        self.resubmit = resubmit
        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)
        self.futures = {}
        self.log = log

    def to_db(self, queue_df, conn, table_name):
        """Submit to queue - sqllite database
        """
        queue_df.to_sql(con=conn, name=table_name, if_exists='append', index=False)
        self.log.info('inserted ' + str(len(queue_df)) + ' records.')

    def connect_db(self):
        """
        Connect to sqlite3 database
        """
        # sqlite3
        conn = sqlite3.connect(self.db_filename, check_same_thread=False)
        return conn

    def submit(self, f, *args, **kwargs):
        """Submit the job"""
        for n in range(4):
            try:
                future = self.executor.submit(f, *args, **kwargs)
                self.futures[future] = (f, args, kwargs)
                break
            except Exception as e:
                self.log.exception('problem submitting jobs.  try %s' % (n))
                # sleep exponentially
                time.sleep((2 ** n) + random.randint(0, 1000) / 1000)
                pass
        else:
            raise ValueError('unable to submit job %s' % (f))

    def start(self):
        """Start the process manager, check for exceptions
        """
        exceptions = []
        while self.futures:

            res = concurrent.futures.wait(
                self.futures,
                return_when=(concurrent.futures.FIRST_COMPLETED))

            self.log.info('Tasks DONE: %s  NOT_DONE: %s  EXCEPTIONS: %s' % (len(res.done), len(res.not_done), len(exceptions)))

            for future in res.done:
                f, args, kwargs = self.futures[future]
                del self.futures[future]
                exc = future.exception()

                # save the result to db, including errors
                df = pd.DataFrame([args[4]])
                df['errors'] = str(exc)
                self.to_db(df, self.connect_db(), self.table)

                if exc is None:
                    self.on_success(future, exc, f, *args, **kwargs)
                else:
                    exceptions.append((future, exc))
                    self.on_error(future, exc, f, *args, **kwargs)
                    if self.resubmit:
                        self.log.warning('Resubmitting')
                        self.submit(f, *args, **kwargs)

        if len(exceptions) >= 1:
            self.log.error(("Exceptions: %s" % ("\n".join(map(str, exceptions)))))
            self.log.error("Failed: Found exceptions.")
            self.log.error("Go/No Go: No Go")
        else:
            self.log.info("Go/No Go: Go")

    def on_error(self, future, exc, f, *args, **kwargs):
        """Record if error"""
        self.log.error('Got exception from %s: %s' % (future, exc))
        # see if you can get the traceback - Todo
        # raise Exception("Exception in a future", (future, exc))

    def on_success(self, future, exc, f, *args, **kwargs):
        """Record if success"""
        self.log.info('Success: Future finished: %s' % (future.result()))
