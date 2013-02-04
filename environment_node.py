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

    message = '1'

    def __init__(self, row, leng, numnode, nummes, times, parents, nlaunch):
        """Inicializa el número de lanzamientos y la cola de tiempos"""
        nodo.Nodo.__init__(self, row, leng, numnode, nummes, parents)

        self.times = times
        self.nlaunch = nlaunch

    def send_message(self, dest, message):
        """Envía un mensaje y lo contabiliza"""
        self.tubes[dest].put('M-' + self.name + '-' + message)

        self.outDeficit += 1
        self.mes += 1

    def run(self):
        """Proceso de lanzamiento"""
        times = []
        nummes = []

        #Lanza nlaunch mensajes
        for n in range(0, self.nlaunch):
            initime = time.time()

            #Envía mensaje inicial
            self.message_init(self.message)
            self.make_job(self.message)

            #Escucha hasta que todos sus hijos han terminado
            while self.outDeficit > 0:
                self.receive_message()

            #Captura la duración del proceso
            endtime = time.time()
            times.append(str("%.4f" % (endtime - initime)) + 's')

            #Guarda la cantidad de mensajes enviados e inicializa
            nummes.append([self.mes, self.sig])
            self.mes = 0
            self.sig = 0

        #Envía señal de finalización
        for key in self.tubes.keys():
            self.send_end(key)

        #Guarda el número de mensajes y los tiempos en las colas
        self.nummes.put(nummes)
        self.times.put(times)

    def message_init(self, message):
        """Envía mensaje inicial a sus hijos"""
        for key in self.tubes.keys():
            self.send_message(key, message)

    def tube_clean(self):
        """Limpia mensajes de finalización perdidos"""
        for n in range(0, self.leng):
            self.tuberesp.watch(str(n))
            while True:
                job = self.tuberesp.reserve(timeout=0.001)
                if job is not None:
                    job.delete()
                else:
                    break
