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
        tmp['gold'] = tmp['gold'].apply(pa.Timedelta.total_seconds)
        tmp['pb_segment'] = tmp['pb_segment'].apply(pa.Timedelta.total_seconds)
        with open('tmp.txt',"w") as fd:
            fd.write(tmp.to_csv())
        print(tmp.to_string())
    

if __name__ == '__main__':
        file = sys.argv[1]
        do_thing(file)
        asdf = input("press enter to continue...")
        
        
        
        