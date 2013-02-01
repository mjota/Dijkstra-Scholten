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
import csv
import sys
#import pydot


class main:

    def __init__(self):
        self.nodes = []

    def open_file(self):
        try:
            fileR = open(sys.argv[1], "r")
        except IOError:
            print("Imposible abrir fichero")
        except:
            print("Introduce nombre de fichero")
        else:
            self.fileC = csv.reader(fileR, delimiter=',')

    def create_nodes(self):
        s = 0
        for row in self.fileC:
            self.nodes.append(nodo.Nodo(row, s))
            s += 1
        self.message_init = nodo.Nodo([0], 0)

    def launch_nodes(self):
        self.message_init.send_message(0, 'Mensaje de prueba')
        for node in self.nodes:
            node.start()

        for node in self.nodes:
            node.join()
        self.nodes[0].receive_message()

    def make_maplist():
        pass

    def close_node(self):
        for node in self.nodes:
            node.close_connection()

if __name__ == '__main__':
    main = main()
    main.open_file()
    main.create_nodes()
    main.launch_nodes()

    main.close_node()


