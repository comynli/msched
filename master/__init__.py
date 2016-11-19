from socketserver import BaseRequestHandler
from socketserver import ThreadingTCPServer


class MasterHandler(BaseRequestHandler):
    def handle(self):
        self.request.recv(4)