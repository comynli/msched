import json
import uuid
import struct
from socketserver import BaseRequestHandler
from socketserver import ThreadingTCPServer
from kazoo.client import KazooClient


def encode(data):
    buf = json.dumps(data).encode()
    length = len(buf)
    return struct.pack('<l{}s'.format(length), length, buf)


class MasterHandler(BaseRequestHandler):
    def heartbeat(self, heartbeat):
        data = json.dumps({'version': heartbeat['version'], 'timestamp': heartbeat['timestamp']})
        self.server.zk.set('/{}/agents/{}/heartbeat'.format(self.server.root, heartbeat['id']), data.encode())

    def poll_task(self, heartbeat):
        node = '/{}/agents/{}/tasks'.format(self.server.root, heartbeat["id"])
        tasks = self.server.zk.get_children(node)
        if tasks:
            node = '{}/{}'.format(node, tasks.pop())
            data, _ = self.server.zk.get(node)
            task = json.loads(data.decode())
            return task

    def set_task_status(self, agent_id, task_id, status):
        tx = self.server.zk.transaction()
        tx.set_data('/{}/tasks/{}/targets/{}'.format(self.server.root, task_id, agent_id), status.encode())
        if status in ('F', 'S', 'K'):
            tx.delete('/{}/agents/{}/tasks/{}'.format(self.server.root, agent_id, task_id))
        tx.set_data('/{}/signal/{}', uuid.uuid4().bytes)
        tx.commit()

    def handle(self):
        buf = self.request.recv(4)
        length, *_ = struct.unpack('<l', buf)
        buf = self.request.recv(length)
        pack, *_ = struct.unpack('<{}s'.format(length), buf)
        heartbeat = json.loads(pack.decode())
        self.heartbeat(heartbeat)
        if heartbeat.get('task') is None:
            task = self.poll_task(heartbeat)
            if task is not None:
                self.set_task_status(heartbeat['id'], task['id'], 'R')
                buf = encode(task)
                self.request.send(buf)
        else:
            task = heartbeat['task']
            status = 'S'
            if task.get('code') != 0:
                status = 'F'
            self.set_task_status(heartbeat['id'], task['id'], status)
            # #TODO process output


class Master:
    def __init__(self, zk_hosts, zk_root, address):
        self.zk = KazooClient(zk_hosts)
        self.root = zk_root
        self.server = ThreadingTCPServer(address, MasterHandler)
        self.server.zk = self.zk
        self.server.root = self.root

    def start(self):
        self.zk.start()
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()
        self.zk.close()
