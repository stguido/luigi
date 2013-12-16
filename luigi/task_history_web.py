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

import json
import tornado.web
import task_history
import logging
logger = logging.getLogger("luigi.server")


class BaseTaskHistoryHandler(tornado.web.RequestHandler):
    def initialize(self, api):
        self._api = api

    def get_template_path(self):
        return 'luigi/templates'


class RecentRunHandler(BaseTaskHistoryHandler):
    def get(self):
        tasks = self._api.task_history.find_latest_runs()
        self.render("recent.html", tasks=tasks)


class ByNameHandler(BaseTaskHistoryHandler):
    def get(self, name):
        tasks = self._api.task_history.find_all_by_name(name)
        self.render("recent.html", tasks=tasks)


class ByIdHandler(BaseTaskHistoryHandler):
    def get(self, id):
        task = self._api.task_history.find_task_by_id(id)
        self.render("show.html", task=task)


class ByParamsHandler(BaseTaskHistoryHandler):
    def get(self, name):
        payload = self.get_argument('data', default="{}")
        arguments = json.loads(payload)
        tasks = self._api.task_history.find_all_by_parameters(name, session=None, **arguments)
        self.render("recent.html", tasks=tasks)


def get_handlers(api):
    handlers = [
        (r'/history', RecentRunHandler, {'api': api}),
        (r'/history/by_name/(.*?)', ByNameHandler, {'api': api}),
        (r'/history/by_id/(.*?)', ByIdHandler, {'api': api}),
        (r'/history/by_params/(.*?)', ByParamsHandler, {'api': api})
    ]
    return handlers