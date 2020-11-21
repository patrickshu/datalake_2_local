#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 10:14:35 2020

@author: patrickshu
"""

import re
import sys
import pandas as pd

save_file = 'lake_output'

def process_line(length_list, line):
    '''
    processing header(column names) and data rows
    length_list: Python list, number of charactors per column stored as integer in
    a list
    line: string, line to be processed
    '''
    col_list = []
    start_pos = 1
    for l in length_list:
        col_list.append(line[start_pos:start_pos+l].strip())
        start_pos+=(l+1)
        
    return col_list


def process_line_delimiter(line, delimiter='|'):
    '''
    processing data rows based on delimiter
    line: string, line to be processed
    '''      
    return [x.strip() for x in line.split('|')][1:-1]

def file_to_df(save_file):
    
    '''
    from Hive stdout to Pandas Dataframe
    save_file: the stdout from Hive saved as text file
    console will print out the progress.
    '''
    
    with open(save_file, 'r') as f:
        
        regex = re.compile(r'''[a-zA-Z0-9]+''')
        # process the first line, get length of each column
        header_length = f.readline()
        try:
            assert not re.search(regex, header_length)
        except AssertionError:
            print('unknown file format')
            sys.exit()
        counter = 0
        length_list = []
        for x in header_length:
            if x == '+' and counter == 0:
                pass
            elif x == '-':
                counter += 1
            elif x == '+':
                length_list.append(counter)
                counter = 0
        # get the column names from second line, based on length_list
        col_names_line = f.readline()
        col_list = process_line(length_list, col_names_line)
        
        result_df = pd.DataFrame(columns=col_list)
        
        # deal with rest of the lines - data rows
        rest_lines = f.readlines()   
        row_num = 0
        for l in rest_lines:
            if (l.startswith("|")) and (l != col_names_line):
                try:
                    result_df.loc[row_num] = process_line_delimiter(l)
                except ValueError:
                    print(l)
            row_num += 1
            if row_num % 100 == 0:
                print(row_num, "lines loaded")
        print('file loaded')      
    return result_df
    