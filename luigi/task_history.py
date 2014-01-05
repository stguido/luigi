# Copyright (c) 2013 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import abc
import traceback
import logging
import task
import threading

from task_status import PENDING, FAILED, DONE, RUNNING, UNKNOWN

logger = logging.getLogger('luigi.server')


class Task(object):
    ''' Interface for methods on TaskHistory
    '''
    def __init__(self, task_id, status, host=None):
        self.task_family, self.parameters = task.id_to_name_and_params(task_id)
        self.status = status
        self.record_id = None
        self.host = host


class TaskHistory(object):
    ''' Abstract Base Class for updating the run history of a task
    '''
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def task_scheduled(self, task_id, deps):
        pass

    @abc.abstractmethod
    def task_finished(self, task_id, successful):
        pass

    @abc.abstractmethod
    def task_started(self, task_id, worker_host):
        pass

    # TODO(erikbern): should web method (find_latest_runs etc) be abstract?


class StatusUpdate(object):
    ''' Interface Status updates used by background workers
    '''
    def __init__(self, task_id, status, deps=None, host=None):
        self.task_id = task_id
        self.status = status
        self.deps = deps
        self.host = host


class HistoryWorker(threading.Thread):
    def __init__(self, queue, task_history):
        threading.Thread.__init__(self)
        self._queue = queue
        self._task_history = task_history

    def run(self):
        while True:
            update = self._queue.get()
            try:
                if update.status == DONE or update.status == FAILED:
                    successful = (update.status == DONE)
                    self._task_history.task_finished(update.task_id, successful)
                elif update.status == PENDING:
                    self._task_history.task_scheduled(update.task_id, update.deps)
                elif update.status == RUNNING:
                    self._task_history.task_started(update.task_id, update.host)
                logger.info("Task history updated for %s with %s" % (update.task_id, update.status))
            except:
                logger.warning("Error saving Task history for %s with %s" % (update.task_id, update.status), exc_info=1)
            self._queue.task_done()
