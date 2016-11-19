import os
import json
import struct
import socket
import logging
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor
from .command import Command

__version__ = '0.0.1'


def encode(data):
    buf = json.dumps(data).encode()
    length = len(buf)
    return struct.pack('<l{}s'.format(length), length, buf)


class Agent:
    def __init__(self, master):
        self.master = master
        self.tasks = {}
        self.so = None
        self.event = threading.Event()
        self.polling = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=1)

    def connect(self):
        self.so = socket.socket()
        self.so.connect(self.master)

    def poll_task(self):
        self.polling.set()
        buf = self.so.recv(4)
        length, *_ = struct.unpack('<l', buf)
        buf = self.so.recv(length)
        data, *_ = struct.unpack('<{}s'.format(length), buf)
        task = json.loads(data.decode())
        cmd = Command(task)
        future = cmd.run()
        if future is not None:
            future.add_done_callback(self.on_task_done)
        self.tasks['current'] = task

    def on_task_done(self):
        self.heartbeat()
        self.tasks.pop('current', None)

    def heartbeat(self):
        data = {
            'id': os.uname().nodename,
            'version': __version__,
            'timestamp': datetime.datetime.now().timestamp(),
            'task': self.tasks.get('current')
        }
        try:
            self.so.send(encode(data))
            if data.get('task') is None and not self.polling.is_set():
                self.executor.submit(self.poll_task).add_done_callback(lambda: self.polling.clear())
        except Exception as e:
            logging.error('send heartbeat error: {}'.format(e))
            self.connect()

    def start(self):
        while not self.event.is_set():
            self.heartbeat()
            self.event.wait(1)

    def shutdown(self):
        self.event.set()
