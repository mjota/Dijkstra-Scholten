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
#import pydot


class main:

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
            else:
                print('Extensión de fichero no válida')

    def create_nodes(self):
        s = 0
        for row in self.fileC:
            if s == 0:
                self.nodes.append(environment_node.EnvironmentNode(row, s, self.mes, self.times, self.parent))
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
        while not self.mes.empty():
            print self.mes.get()
        while not self.times.empty():
            print self.times.get()
        while not self.parent.empty():
            print self.parent.get()
        self.nodes[0].tube_clean()
        for node in self.nodes:
            node.close_connection()

if __name__ == '__main__':
    main = main()
    main.open_file()
    main.create_nodes()
    main.launch_nodes()

    main.close_node()


