# -*- coding: utf-8 -*-
"""
Created on Fri Dec 21 18:08:06 2018

@author: jason 
"""
from lsparse import lsparse
import pandas as pa
import sys

def do_thing(filename):
        ls = lsparse(file)
        tmp = ls.splitsdf
        tmp2 = ls.attemptsdf[['started','ended','realtime']].dropna()
        print(tmp.to_string())
        print('\n\n\n')
        print(tmp.to_csv())
        tmp2.to_csv(file + ".completions.csv")
    

if __name__ == '__main__':
        file = sys.argv[1]
        do_thing(file)
        asdf = input("press enter to continue...")
        
        
        
        