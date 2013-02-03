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
    maplist = {}

    def __init__(self, row, s):
        multiprocessing.Process.__init__(self)
        self.tubes = {}
        self.name = str(s)
        self.worked = 1
        self.completed = 0
        self.ended = 0

        self.inDeficitList = [0 for x in range(0, 15)]
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
            while True:
                self.receive_message()
                if self.completed or self.ended:
                    break
            print('Fin ' + self.name)

            self.worked = 1
            self.completed = 0

            if self.ended:
                break

    def make_job(self, message):
        #print(message)
        #time.sleep(1)
        pass

    def send_message(self, dest, message):
        if(self.parent != -1 or self.name == '0'):
            self.tubes[dest].put('M-' + self.name + '-' + message)
            self.outDeficit += 1

    def receive_message(self):
        job = self.tubeme.reserve(timeout=1)
        if job is None:
            self.send_signal(1)
            return
        typ, sender, message = re.split('-', job.body, 2)
        if typ == 'M':
            print('Mensaje de ' + sender + ' para ' + self.name)

            if(self.parent == -1):
                self.parent = int(sender)
                self.maplist[int(self.name)] = self.parent
            self.inDeficit += 1
            self.inDeficitList[int(sender)] += 1

            if self.worked:
                for key in self.tubes.keys():
                    self.send_message(key, message)
            self.worked = 0

            self.make_job(message)
            self.send_signal()
        elif typ == 'S':
            print('Signal de ' + sender + ' a ' + self.name)
            self.outDeficit -= 1
        elif typ == 'E':
            for key in self.tubes.keys():
                self.send_end(key)
            self.ended = 1

        job.delete()

    def send_signal(self, *last):
        if (self.inDeficit > 1 and not last):
            n = 0
            for e in self.inDeficitList:
                if (e > 1 or (e == 1 and n != self.parent)):
                    break
                n += 1

            self.tuberesp.use(n)
            self.tuberesp.put('S-' + self.name + '-')

            self.inDeficitList[n] -= 1
            self.inDeficit -= 1

        elif (self.inDeficit == 1 and self.outDeficit == 0):
            self.tuberesp.use(self.parent)
            self.tuberesp.put('S-' + self.name + '-')

            self.inDeficitList[self.parent] = 0
            self.inDeficit = 0
            self.parent = -1
            self.completed = 1

    def send_end(self, dest):
        self.tubes[dest].put('E--')

    def close_connection(self):
        self.tubeme.close()
        self.tuberesp.close()
        for bean in self.tubes.values():
            bean.close()