import multiprocessing
import beanstalkc
import re


class Nodo(multiprocessing.Process):

    ip = 'localhost'
    port = 11300

    def __init__(self, row, s):
        super(Nodo, self).__init__()
        self.tubes = {}
        self.name = str(s)

        self.tubeme = beanstalkc.Connection(host=self.ip, port=self.port)
        self.tubeme.watch(str(s))

        self.tuberesp = beanstalkc.Connection(host=self.ip, port=self.port)

        for n in row:
            self.tubes[int(n)] = beanstalkc.Connection(host=self.ip, port=self.port)
            self.tubes[int(n)].use(n)

    def run(self):
        pass

    def send_message(self, dest, message):
        self.tubes[dest].put('M-' + self.name + '-' + message)

    def receive_message(self):
        job = self.tubeme.reserve()
        typ, sender, message = re.split('-', job.body, 2)
        if typ == 'M':
            print('Message type')
        else:
            print('Signal type')
        print sender
        print message
        job.delete()

    def send_signal
