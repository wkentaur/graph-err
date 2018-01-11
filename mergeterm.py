#!/usr/bin/python
# -*- coding: utf-8 -*-
# Kasuta kujul:  mergeterm.py  'liidetav term' 'siht term'

from UudisKratt import UudisKratt
import sys
import logging


logging.basicConfig(filename='log/mergeterm.log',level=logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s')

def confirm(prompt=None, resp=False):
    
    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')
        
    while True:
        ans = input(prompt)
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            print('please enter y or n.')
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False
            
actor = UudisKratt()

if (len(sys.argv) == 3):

	inList = sys.argv
	first = inList[1]
	target = inList[2]
	print("Liidan >%s< mõistega >%s< ?" % (first, target))
	if ( confirm() ):
		actor.mergeTermInto(first, target)
else:
	print("Kasuta kujul:  %s  'liidetav term' 'siht term'" % (sys.argv[0]))

