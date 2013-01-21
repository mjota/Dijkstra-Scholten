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
            print('Message type from ' + sender)
        else:
            print('Signal type from ' + sender)
        print message
        job.delete()

    def send_signal(self, dest):
        self.tuberesp.use(str(dest))
        self.tuberesp.put('S-' + self.name + '-')

    def close_connection(self):
        self.tubeme.close()
        self.tuberesp.close()
        for bean in self.tubes.values():
            bean.close()