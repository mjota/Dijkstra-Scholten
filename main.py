#!/usr/bin/env python
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
import environment_node
import csv
import sys
import re
import multiprocessing
import pydot


class main:

    NLAUNCH = 10    # Número de lanzamientos

    def __init__(self):
        """Inicializa los nodos y las colas donde recibirá resultados"""
        self.nodes = []
        self.mes = multiprocessing.Queue()
        self.times = multiprocessing.Queue()
        self.parent = multiprocessing.Queue()

    def open_file(self):
        """Abre el fichero .csv o .dot indicado en el primer argumento del
        terminal.
        Formato del fichero CSV: el número de línea-1 marca el número de nodo
        Separados por comas los nodos destino"""
        try:
            fileR = open(sys.argv[1], "r")
        except IOError:
            print("Imposible abrir fichero")
        except:
            print("Introduce nombre de fichero")
        else:
            #Se ha seleccionado un fichero CSV
            if re.match(".*(\.csv)$", sys.argv[1]):
                fileC = csv.reader(fileR, delimiter=',')
                filecsv = [n for n in fileC]
                self.create_nodes(filecsv)

            #Se ha seleccionado un fichero DOT
            elif re.match(".*(\.dot)$", sys.argv[1]):
                filedot = [[] for n in range(0, 15)]
                graph = pydot.graph_from_dot_file(sys.argv[1])
                res = graph.get_edges()
                for n in res:
                    filedot[int(n.get_source())].append(int(n.get_destination()))
                self.create_nodes(filedot)
            else:
                print('Extensión de fichero no válida')

    def create_nodes(self, readable):
        """Crea los nodos a partir del fichero CSV o DOT.
        Primero crea el nodo entorno.
        Les pasa las colas donde recibirá los resultados."""
        leng = readable.__len__()
        numnode = 0
        for row in readable:
            if numnode == 0:
                self.nodes.append(environment_node.EnvironmentNode(row, leng,
                numnode, self.mes, self.times, self.parent, self.NLAUNCH))
            else:
                self.nodes.append(nodo.Nodo(row, leng, numnode,
                self.mes, self.parent))
            numnode += 1

    def launch_nodes(self):
        """Lanza los nodos en distintos procesos"""
        for node in self.nodes:
            node.start()

        for node in self.nodes:
            node.join()

    def close_node(self):
        """Limpia mensajes pendientes y cierra la conexión con beanstalk"""
        self.nodes[0].tube_clean()
        for node in self.nodes:
            node.close_connection()

    def show_results(self):
        """Recoge los tiempos y el número de mensajes de las colas
        Muestra en pantalla los resultados"""
        nummes = [0 for x in range(0, self.NLAUNCH)]
        numsig = [0 for x in range(0, self.NLAUNCH)]
        times = self.times.get()

        #Recoge la cola de número de mensajes y los suma
        while not self.mes.empty():
            row = self.mes.get()
            n = 0
            for dup in row[0:self.NLAUNCH]:
                nummes[n] += dup[0]
                numsig[n] += dup[1]
                n += 1

        print('Número total mensajes trabajo: ' + str(nummes))
        print('Número total mensajes signal: ' + str(numsig))
        print('Tiempos: ' + str(times))

        self.writeCSV(nummes, numsig, times)

    def writeCSV(self, nmes, nsig, times):
        """Crea un fichero CSV con los resultados obtenidos"""
        filew = open('Result_' + sys.argv[1] + '.csv', 'w')
        filec = csv.writer(filew)

        filec.writerow(['Test', 'Mensajes trabajo', 'Signals', 'Tiempo'])
        for n in range(0, self.NLAUNCH):
            filec.writerow([n, nmes.pop(), nsig.pop(), times.pop()])

        filew.close()
        print('Fichero CSV con resultados creado en Result_' +
        sys.argv[1] + '.csv')

    def print_graph(self):
        """Genera los grafos Spanning Tree recogidos de la cola de padres"""
        graphlist = []

        while not self.parent.empty():
            graphlist.append(self.parent.get())
        graph = pydot.Dot(graph_type='digraph')

        #Añade vértices y aristas
        for n in range(0, self.NLAUNCH):
            for row in graphlist:
                edge = pydot.Edge(str(n) + '.' + str(row[1][n]),
                str(n) + '.' + str(row[0]))
                graph.add_edge(edge)

        graph.write_png(sys.argv[1] + '.png')
        print('Spanning Tree creado en ' + sys.argv[1] + '.png')

if __name__ == '__main__':
    main = main()
    main.open_file()
    main.launch_nodes()
    main.close_node()

    main.show_results()
    main.print_graph()


