#!/usr/bin/env python

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

        '''
        self.prueba1.tubes[1].use('0')
        self.prueba1.tubes[2].watch('1')

        self.prueba2.tubes[1].use('2')
        self.prueba2.tubes[2].watch('3')

        print self.prueba1.tubes[1].using()
        print self.prueba1.tubes[2].watching()
        print self.prueba2.tubes[1].using()
        print self.prueba2.tubes[2].watching()
        """
        self.nodes[0].tubes[1].put('ur')
        print self.nodes[8].tubes[1] is self.nodes[9].tubes[1]
        job = self.nodes[1].tubes[1].reserve()
        print job.body
        job.delete()
        print self.nodes[0].tubes[1] is self.nodes[1].tubes[1]
        #print self.nodes[1].test
        #print self.nodes[2].test
        #self.nodes[0].start()
        """
        '''

    def launch_node(self):
        for node in self.nodes:
            node.start()

        for node in self.nodes:
            node.join()

    def test_node(self):
        self.nodes[1].send_message(2, 'Mensaje de prueba-A')
        self.nodes[2].receive_message()

if __name__ == '__main__':
    main = main()
    main.open_file()
    main.create_nodes()
    main.test_node()


