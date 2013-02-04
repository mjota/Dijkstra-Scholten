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
#import dot_parser
import pydot


class main:

    NLAUNCH = 10

    def __init__(self):
        self.nodes = []
        self.mes = multiprocessing.Queue()
        self.times = multiprocessing.Queue()
        self.parent = multiprocessing.Queue()

    def open_file(self):
        try:
            fileR = open(sys.argv[1], "r")
        except IOError:
            print("Imposible abrir fichero")
        except:
            print("Introduce nombre de fichero")
        else:
            if re.match(".*(\.csv)$", sys.argv[1]):
                self.fileC = csv.reader(fileR, delimiter=',')
            elif re.match(".*(\.dot)$", sys.argv[1]):
                print('Es dot')
                #graph = pydot.graph_from_dot_file('somefile.dot')
            else:
                print('Extensión de fichero no válida')

    def create_nodes(self):
        s = 0
        for row in self.fileC:
            if s == 0:
                self.nodes.append(environment_node.EnvironmentNode(row, s, self.mes, self.times, self.parent, self.NLAUNCH))
            else:
                self.nodes.append(nodo.Nodo(row, s, self.mes, self.parent))
            s += 1

    def launch_nodes(self):
        for node in self.nodes:
            node.start()

        for node in self.nodes:
            node.join()

    def make_maplist():
        pass

    def close_node(self):
        #self.nodes[0].tube_clean()
        for node in self.nodes:
            node.close_connection()

    def show_results(self):
        nmes = [0 for x in range(0, self.NLAUNCH)]
        nsig = [0 for x in range(0, self.NLAUNCH)]
        times = self.times.get()

        while not self.mes.empty():
            row = self.mes.get()
            print row
            n = 0
            for dup in row:
                nmes[n] += dup[0]
                nsig[n] += dup[1]
                n += 1
        print('Número total mensajes trabajo: ' + str(nmes))
        print('Número total mensajes signal: ' + str(nsig))
        print('Tiempos: ' + str(times))
        self.writeCSV(nmes, nsig, times)

    def writeCSV(self, nmes, nsig, times):
        """Create CSV file"""
        fileW = open('Result_' + sys.argv[1] + '.csv', 'w')
        fileC = csv.writer(fileW)
        fileC.writerow(['Test', 'Mensajes trabajos', 'Signals', 'Tiempo'])
        for n in range(0, self.NLAUNCH):
            fileC.writerow([n, nmes.pop(), nsig.pop(), times.pop()])
        fileW.close()

    def print_graph(self):
        graphlist = []
        while not self.parent.empty():
            graphlist.append(self.parent.get())
        graph = pydot.Dot(graph_type='digraph')
        for n in range(0, self.NLAUNCH):
            for row in graphlist:
                edge = pydot.Edge(str(n) + '.' + str(row[1][n]), str(n) + '.' + str(row[0]))
                graph.add_edge(edge)
        graph.write_png(sys.argv[1] + '.png')
        print('Spanning Tree creado en ' + sys.argv[1] + '.png')

if __name__ == '__main__':
    main = main()
    main.open_file()
    main.create_nodes()
    main.launch_nodes()

    main.show_results()
    main.print_graph()
    main.close_node()


