import json
import logging
import requests
import threading
from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch


class Callback:
    def __init__(self, zk_hosts, zk_root):
        self.zk = KazooClient(zk_hosts)
        self.root = zk_root
        self.event = threading.Event()
        self.tasks = {}

    def get_task(self, task_id):
        node = '/{}/tasks/{}'.format(self.root, task_id)
        data, _ = self.zk.get(node)
        task = json.loads(data.decode())
        targets = {}
        for target in self.zk.get_children('{}/targets'):
            path = '{}/targets/{}'.format(node, target)
            status, _ = self.zk.get(path)
            targets[target] = status.decode()
        task['targets'] = targets
        return task

    def delete(self, task_id):
        callback_node = '/{}/callback/{}'.format(self.root, task_id)
        task_node = '/{}/tasks/{}'.format(self.root, task_id)
        tx = self.zk.transaction()
        tx.delete(callback_node)
        tx.delete(task_node)
        tx.commit()

    def run(self, task_id):
        task = self.get_task(task_id)
        try:
            requests.post(task['callback'], json=task)
            self.delete(task_id)
        except Exception as e:
            logging.error(e)

    def watch_tasks(self, tasks):
        for task_id in set(tasks).difference(self.tasks):
            self.run(task_id)
        self.tasks = tasks
        return not self.event.is_set()

    def watch(self):
        ChildrenWatch(self.zk, '/{}/callback'.format(self.root), self.watch_tasks)

    def compensate(self):
        while not self.event.is_set():
            for task in self.zk.get_children('/{}/callback'.format(self.root)):
                self.run(task)
            self.event.wait(10)

    def start(self):
        self.zk.start()
        self.watch()
        self.compensate()

    def shutdown(self):
        self.event.set()
        self.zk.close()
