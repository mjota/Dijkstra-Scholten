#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

    def launch_node(self):
        for node in self.nodes:
            node.start()

        for node in self.nodes:
            node.join()

    def close_node(self):
        for node in self.nodes:
            node.close_connection()

    def test_node(self):
        self.nodes[1].send_message(2, 'Mensaje de prueba-A')
        self.nodes[0].send_signal(2)
        self.nodes[2].receive_message()
        self.nodes[2].receive_message()

if __name__ == '__main__':
    main = main()
    main.open_file()
    main.create_nodes()
    main.test_node()

    main.close_node()


