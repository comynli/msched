import json
import threading
from functools import partial
from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock, LockTimeout
from kazoo.recipe.watchers import DataWatch, ChildrenWatch


def count(targets, statuses):
    return len([x for x in targets.values if x in statuses])


def choose(targets, count):
    c = 0
    for target, status in targets.items():
        if status == 'N' and c < count:
            c += 1
            yield target


class Scheduler:
    def __init__(self, zk_hosts, zk_root):
        self.zk = KazooClient(zk_hosts)
        self.root = zk_root
        self.tasks = set()
        self.event = threading.Event()

    def get_targets(self, task_id):
        result = {}
        node = '/{}/tasks/{}/targets'.format(self.root, task_id)
        for target in self.zk.get_children(node):
            path = '{}/{}'.format(node, target)
            status, _ = self.zk.get(path)
            result[target] = status.decode()
        return result

    def callback(self, task_id):
        node = '/{}/callback/{}'.format(self.root, task_id)
        self.zk.ensure_path(node)

    def copy_task(self, targets, task):
        for target in targets:
            node = '/{}/agents/{}/tasks/{}'.format(self.root, target, task['id'])
            tx = self.zk.transaction()
            tx.create(node, json.dumps(task).encode())
            tx.set_data('/{}/tasks/{}/targets/{}'.format(self.root, task['id'], target), b'W')
            tx.commit()

    def schedule(self, task_id):
        node = '/{}/tasks/{}'.format(self.root, task_id)
        lock_node = '{}/lock'.format(node)
        self.zk.ensure_path(lock_node)
        lock = Lock(self.zk, lock_node)
        try:
            if lock.acquire(timeout=1):
                data, _ = self.zk.get(node)
                task = json.loads(data.decode())
                p = task.get('parallel', 1)
                rate = task.get('fail_rate', 0)
                targets = self.get_targets(task_id)
                if count(targets, ('F', ))/len(targets) > rate:
                    return self.callback(task_id)
                if count(targets, ('F', 'S', 'K')) == len(targets):
                    return self.callback(task_id)
                wait_schedule = choose(targets, p - count(targets, ('W', 'R')))
                self.copy_task(wait_schedule, task)
        except LockTimeout:
            pass
        finally:
            lock.release()

    def watch_new_task(self, tasks):
        for task_id in set(tasks).difference(self.tasks):
            self.schedule(task_id)
            DataWatch(self.zk, '/{}/signal/{}'.format(self.root, task_id),
                      partial(self.watch_exist_task, task_id=task_id))
        self.tasks = tasks
        return not self.event.is_set()

    def watch_exist_task(self, task_id, *args):
        if self.zk.exists('/{}/callback/{}'.format(self.root, task_id)):
            return False
        self.schedule(task_id)
        return True

    def watch(self):
        ChildrenWatch(self.zk, '/{}/signal'.format(self.root), self.watch_new_task)

    def start(self):
        self.zk.start()
        self.watch()
        self.event.wait()

    def shutdown(self):
        self.event.set()
        self.zk.close()
