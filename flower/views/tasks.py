import copy
import json
import logging
from functools import total_ordering

from tornado import web

from ..utils.tasks import as_dict, get_task_by_id, iter_tasks
from ..utils.search import parse_search_terms
from ..views import BaseHandler

logger = logging.getLogger(__name__)


class TaskView(BaseHandler):
    @web.authenticated
    def get(self, task_id):
        task = get_task_by_id(self.application.events, task_id)

        if task is None:
            raise web.HTTPError(404, f"Unknown task '{task_id}'")
        task = self.format_task(task)
        self.render("task.html", task=task)


@total_ordering
class Comparable:
    """
    Compare two objects, one or more of which may be None.  If one of the
    values is None, the other will be deemed greater.
    """

    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        try:
            return self.value < other.value
        except TypeError:
            return self.value is None


class TasksDataTable(BaseHandler):
    @web.authenticated
    def get(self):
        app = self.application
        draw = self.get_argument('draw', type=int)
        start = self.get_argument('start', type=int)
        length = self.get_argument('length', type=int)
        search = self.get_argument('search[value]', type=str)

        column = self.get_argument('order[0][column]', type=int)
        sort_by = self.get_argument(f'columns[{column}][data]', type=str)
        sort_order = self.get_argument('order[0][dir]', type=str) == 'desc'

        def key(item):
            return getattr(item[1], sort_by)

        self.maybe_normalize_for_sort(app.events.state.tasks_by_timestamp(), sort_by)

        sorted_tasks = sorted(
            iter_tasks(app.events, search=search),
            key=key,
            reverse=sort_order
        )

        filtered_tasks = []

        for task in sorted_tasks[start:start + length]:
            task_dict = as_dict(self.format_task(task)[1])
            if task_dict.get('worker'):
                task_dict['worker'] = task_dict['worker'].hostname
            task_dict['timestamp'] = task_dict.get('timestamp')
            filtered_tasks.append(task_dict)

        self.write(dict(draw=draw, data=filtered_tasks,
                        recordsTotal=len(sorted_tasks),
                        recordsFiltered=len(sorted_tasks)))

    @classmethod
    def maybe_normalize_for_sort(cls, tasks, sort_by):
        sort_keys = {'name': str, 'state': str, 'received': float, 'started': float, 'runtime': float}
        if sort_by in sort_keys:
            for _, task in tasks:
                attr_value = getattr(task, sort_by, None)
                if attr_value:
                    try:
                        setattr(task, sort_by, sort_keys[sort_by](attr_value))
                    except TypeError:
                        pass

    @web.authenticated
    def post(self):
        app = self.application
        draw = self.get_argument('draw', type=int)
        start = self.get_argument('start', type=int)
        length = self.get_argument('length', type=int)
        search = self.get_argument('search[value]', type=str)
        sort_column = self.get_argument('order[0][column]', type=int)
        sort_order = self.get_argument('order[0][dir]', type=str)

        taskname = self.get_argument('taskname', None)
        workername = self.get_argument('workername', None)

        column = {
            0: 'name',
            1: 'uuid',
            2: 'state',
            6: 'received',
            7: 'started',
            8: 'duration',
            9: 'runtime',
            10: 'worker',
            13: 'retries',
            14: 'revoked',
        }

        sort_column = column[sort_column]
        sort_order = '-' if sort_order == 'desc' else ''

        tasks = self.get_filtered_tasks(
            app.events,
            limit=length,
            offset=start,
            sort_by=sort_order + sort_column,
            search=search,
            taskname=taskname,
            workername=workername
        )

        filtered_tasks = []
        for _, task in tasks:
            task_data = task.as_dict()
            task_data['worker'] = task.worker.hostname if task.worker else None
            task_data['timestamp'] = task.timestamp
            filtered_tasks.append(task_data)

        response = {
            'draw': draw,
            'recordsTotal': app.events.state.task_count(),
            'recordsFiltered': len(filtered_tasks),
            'data': filtered_tasks,
        }

        self.write(json.dumps(response))

    def format_task(self, task):
        uuid, args = task
        custom_format_task = self.application.options.format_task

        if custom_format_task:
            try:
                args = custom_format_task(copy.copy(args))
            except Exception:
                logger.exception("Failed to format '%s' task", uuid)
        return uuid, args

    def get_filtered_tasks(self, events, limit=None, offset=0, sort_by=None, search=None, taskname=None, workername=None):
        tasks = list(events.state.tasks_by_timestamp())  # Convert generator to list
        filtered_tasks = []

        search_terms = parse_search_terms(search) if search else None

        for task in tasks:
            if search_terms and not self.match_task(task, search_terms):
                continue
            if taskname and taskname.lower() not in task.name.lower():
                continue
            if workername and (not task.worker or workername.lower() not in task.worker.hostname.lower()):
                continue
            filtered_tasks.append(task)

        if sort_by:
            reverse = sort_by.startswith('-')
            sort_key = sort_by[1:] if reverse else sort_by
            filtered_tasks.sort(key=lambda x: getattr(x, sort_key, None), reverse=reverse)

        return filtered_tasks[offset:offset+limit] if limit else filtered_tasks[offset:]

    def match_task(self, task, terms):
        for term in terms:
            if term.key:
                value = getattr(task, term.key, None)
                if value is None:
                    return False
                if term.operator == '=' and term.value.lower() not in str(value).lower():
                    return False
            else:
                if term.value.lower() not in task.name.lower():
                    return False
        return True


class TasksView(BaseHandler):
    @web.authenticated
    def get(self):
        app = self.application
        capp = self.application.capp

        time = 'natural-time' if app.options.natural_time else 'time'
        if capp.conf.timezone:
            time += '-' + str(capp.conf.timezone)

        self.render(
            "tasks.html",
            tasks=[],
            columns=app.options.tasks_columns,
            time=time,
        )
