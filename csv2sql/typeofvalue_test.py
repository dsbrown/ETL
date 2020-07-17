#!/usr/bin/env python 
# -*- coding: utf-8 -*-

def is_it_a_number(text):
    try:
        int(text)
        return 'INT'
    except ValueError:
        pass

    try:
        float(text)
        return 'FLOAT'
    except ValueError:
        pass
    return 'STR'

y=is_it_a_number('1')
print "1 is " + str(y)
print "1.0 is " + is_it_a_number('1.0')
print "x is " + is_it_a_number('x')
print "X is " + is_it_a_number('X')
print "-1 is " + is_it_a_number('-1')
print ".56789 is " + is_it_a_number('.56789')
print "Foobar is " + is_it_a_number('Foobar')
print "1j is " + is_it_a_number('1j')
print "2+4 is " + is_it_a_number('2+4')
print "2+4i is " + is_it_a_number('2+4i')

