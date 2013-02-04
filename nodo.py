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

    def __init__(self, tubes, leng, numnode, nummes, parents):
        multiprocessing.Process.__init__(self)

        #Recoge id nodo, número total de nodos y colas
        self.name = str(numnode)
        self.leng = leng
        self.nummes = nummes
        self.parents = parents

        #Inicializa tuberías destino y lista de padres
        self.tubes = {}
        self.lparents = []

        #Inicializa contadores de mensajes
        self.mes = 0
        self.sig = 0

        #Inicializa variables de control de tareas realizadas
        self.worked = 1
        self.completed = 0
        self.ended = 0

        #Inicializa deficits
        self.inDeficitList = [0 for x in range(0, self.leng)]
        self.inDeficit = 0
        self.outDeficit = 0
        self.parent = -1

        #Conecta tubería de recepción
        self.tubeme = beanstalkc.Connection(host=self.ip, port=self.port)
        self.tubeme.watch(str(numnode))

        #Conecta tubería para signals
        self.tuberesp = beanstalkc.Connection(host=self.ip, port=self.port)

        #Conecta con las tuberías de envío de mensajes
        for n in tubes:
            self.tubes[int(n)] = beanstalkc.Connection(host=self.ip, port=self.port)
            self.tubes[int(n)].use(n)

    def run(self):
        """Proceso de lanzamiento"""
        nummes = []

        #Repite hasta que recibe señal de finalización
        while True:
            #Escucha hasta que completa todas las tareas
            while True:
                self.receive_message()
                if self.completed or self.ended:
                    break

            #Guarda la cantidad de mensajes enviados e inicializa
            nummes.append([self.mes, self.sig])
            self.mes = 0
            self.sig = 0

            #Inicializa variables de uso para siguiente recepción
            self.worked = 1
            self.completed = 0

            #Si recibe señal de finalización añade a las colas y sale
            if self.ended:
                nummes.pop()
                self.nummes.put(nummes)
                self.parents.put([int(self.name), self.lparents])
                break

    def make_job(self, message):
        """Realiza tarea"""
        time.sleep(int(message))

    def send_message(self, dest, message):
        """Si ya ha recibido algún mensaje lo envía y lo contabiliza"""
        if(self.parent != -1):
            self.tubes[dest].put('M-' + self.name + '-' + message)
            self.outDeficit += 1
            self.mes += 1

    def receive_message(self):
        """Recibe mensajes, señales de trabajo realizado o finalización.
        Si no tiene mensajes o señales intenta enviar finalización al padre"""
        job = self.tubeme.reserve(timeout=0.001)
        if job is None:
            self.send_signal()
            return
        typ, sender, message = re.split('-', job.body, 2)

        #Mensaje de trabajo
        if typ == 'M':
            #Guarda el padre si todavía no lo tenía
            if(self.parent == -1):
                self.parent = int(sender)
                self.lparents.append(self.parent)

            #Añade uno a la lista de tareas
            self.inDeficit += 1
            self.inDeficitList[int(sender)] += 1

            #Envía el mensaje a todos sus destinos si todavía no lo había hecho
            if self.worked:
                for key in self.tubes.keys():
                    self.send_message(key, message)
            self.worked = 0

            self.make_job(message)
            self.send_signal()

        #Señal de trabajo realizado
        elif typ == 'S':
            self.outDeficit -= 1

        #Señal de finalización de todas las tareas
        elif typ == 'E':
            #Envía señal a sus destinos y finaliza
            for key in self.tubes.keys():
                self.send_end(key)
            self.ended = 1

        job.delete()

    def send_signal(self, *last):
        """Envía señal de trabajo realizado"""
        #Debe un signal a alguien más que el padre
        if (self.inDeficit > 1):
            n = 0
            #Recorre la lista de nodos con mensajes recibidos pendientes y
            #envía un signal siempre que no sea el padre. Contabiliza el signal
            for e in self.inDeficitList:
                if (e > 1 or (e == 1 and n != self.parent)):
                    break
                n += 1

            self.tuberesp.use(n)
            self.tuberesp.put('S-' + self.name + '-')
            self.sig += 1

            #Disminuye en uno el número de signals pendientes del nodo receptor
            # y del cómputo general
            self.inDeficitList[n] -= 1
            self.inDeficit -= 1

        #Todas las tareas completadas (hijas también), notificación al padre
        elif (self.inDeficit == 1 and self.outDeficit == 0):
            self.tuberesp.use(self.parent)
            self.tuberesp.put('S-' + self.name + '-')
            self.sig += 1

            #Deja a 0 los pendientes al padre y del cómputo general
            self.inDeficitList[self.parent] = 0
            self.inDeficit = 0

            #Libera el nodo padre y marca como completada
            self.parent = -1
            self.completed = 1

    def send_end(self, dest):
        """Envía señal de finalización de todas la tareas"""
        self.tubes[dest].put('E--')

    def close_connection(self):
        """Desconecta todas las conexiones con otros nodos"""
        self.tubeme.close()
        self.tuberesp.close()
        for bean in self.tubes.values():
            bean.close()