from subprocess import Popen, PIPE, STDOUT


class Command:
    def __init__(self, task):
        self.script = task['script']
        self.timeout = task.get('timeout', 30)
        self.task = task

    def run(self):
        with Popen(['/bin/bash', '-l', '-c', self.script],
                   stdout=PIPE,
                   stderr=STDOUT,
                   start_new_session=True) as proc:
            code = proc.wait(self.timeout)
            out, _ = proc.communicate()
            self.task['code'] = code
            self.task['output'] = out
