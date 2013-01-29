# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Algoritmo de terminación Dijkstra-Scholten sobre Beanstalkd
# Copyright (c) 2013 - Manuel Joaquin Díaz Pol
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program.  If not, see <http://www.gnu.org/licenses/>.
#
#==============================================================================
#

import multiprocessing
import beanstalkc
import re
import time


class Nodo(multiprocessing.Process):

    ip = 'localhost'
    port = 11300

    def __init__(self, row, s):
        super(Nodo, self).__init__()
        self.tubes = {}
        self.name = str(s)
        self.worked = 1

        self.inDeficitList = [0 for x in range(0, 14)]
        self.inDeficit = 0
        self.outDeficit = 0
        self.parent = -1

        self.tubeme = beanstalkc.Connection(host=self.ip, port=self.port)
        self.tubeme.watch(str(s))

        self.tuberesp = beanstalkc.Connection(host=self.ip, port=self.port)

        for n in row:
            self.tubes[int(n)] = beanstalkc.Connection(host=self.ip, port=self.port)
            self.tubes[int(n)].use(n)

    def run(self):
        while True:
            if self.receive_message():
                break
        print('Fin ' + self.name)

    def make_job(self, message):
        #print(message)
        time.sleep(1)

    def send_message(self, dest, message):
        self.tubes[dest].put('M-' + self.name + '-' + message)

    def receive_message(self):
        job = self.tubeme.reserve(timeout=10)
        if job is None:
            return 1
        typ, sender, message = re.split('-', job.body, 2)
        if typ == 'M':
            print('Mensaje de ' + sender + ' para ' + self.name)
            if self.worked:
                for key in self.tubes.keys():
                    self.send_message(key, message)
            self.worked = 0
            self.make_job(message)
            self.send_signal(int(sender))
        else:
            print('Signal de ' + sender + ' a ' + self.name)

        job.delete()

    def send_signal(self, n):
        self.tuberesp.use(n)
        self.tuberesp.put('S-' + self.name + '-')

    def close_connection(self):
        self.tubeme.close()
        self.tuberesp.close()
        for bean in self.tubes.values():
            bean.close()