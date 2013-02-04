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

import nodo
import time


class EnvironmentNode(nodo.Nodo):

    message = 'Test'

    def __init__(self, row, s, q, times, parents):
        nodo.Nodo.__init__(self, row, s, q, parents)
        self.times = times

    def send_message(self, dest, message):
        self.tubes[dest].put('M-' + self.name + '-' + message)
        self.outDeficit += 1
        self.mes += 1

    def run(self):
        times = []
        nmes = []
        for n in range(0, 3):
            initime = time.time()
            self.message_init(self.message)
            self.make_job(self.message)

            while self.outDeficit > 0:
                self.receive_message()

            endtime = time.time()
            times.append(str("%.2f" % (endtime - initime)) + 's')
            print('Fin ' + self.name)

            nmes.append([self.mes, self.sig])
            self.mes = 0
            self.sig = 0

            self.worked = 1
            self.completed = 0

        for key in self.tubes.keys():
            self.send_end(key)

        self.queue.put(nmes)
        self.times.put(times)

    def message_init(self, message):
        for key in self.tubes.keys():
            self.send_message(key, message)

    def tube_clean(self):
        for n in range(0, 15):
            self.tuberesp.watch(str(n))
            while True:
                job = self.tuberesp.reserve(timeout=1)
                if job is not None:
                    job.delete()
                else:
                    break
