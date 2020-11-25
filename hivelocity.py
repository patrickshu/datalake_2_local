#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 15:41:32 2020

@author: patrickshu
"""

import paramiko
import time
import os
import re
import sys
import pandas as pd

# sample Beeline command to run a Hive query.
# In line with beeline syntax, need to add "" surrounding the sql query
'''beeline -e "select vin from revr_bmbs_dmp_offline.rcrd_vhcl_srvc where vin = 'LE40G4KB9HL101219'"'''

# TO-DO 
# exception handling

def file_2_df(save_file):
    
    '''
    from file to Pandas Dataframe
    save_file: the stdout from Hive saved as text file
    console will print out the progress.
    '''
    
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
        
        # deal with rest of the lines - data rows
        rest_lines = f.readlines()   
        row_num = 0
        result_dict = {}
        for l in rest_lines:            
            if (l.startswith("|")) and (l != col_names_line):
                result_dict[row_num] = process_line_delimiter(l)
            row_num += 1
            if row_num % 10000 == 0:
                print("{} lines loaded".format(row_num))
        result_df = pd.DataFrame.from_dict(result_dict,
                                           orient='index',
                                           columns=col_list)
        print('file loaded')      
    return result_df


def string_2_df(console_output,
                save_file):
    '''
    convert datalake output in string, to a Pandas DataFrame.
    First saved to a text file, then return a DataFrame.
    
    console_output: output from Hive console, string
    save_file: the saved text file path
    '''
    print('saving to file -', save_file)
    with open(save_file, 'w') as f:
        f.write(console_output) 
    print('saving to dataframe...')
    result_df = file_2_df(save_file)
    return result_df
    

def hive_2_df(command,
               az_pw,
               bdp_pw,
               edge_node='172.22.10.4', 
               azure_jump='172.20.1.158',
               az='',
               bdp='',
               save_file='lake_output'
               ):
    '''
    From SQL running on hive, to Pandas DataFrame.
    Connect to Hive on Datalake, run the SQL query.
    First saved to a text file, then return a DataFrame.
    
    command: SQL string
    az_pw: azure password
    bdp_pw: bdp_xxxx_s password
    edge_node: edge node ip
    azure_jump: azure jump server ip
    az: azure user name (az_a_xxxxx)
    bdp: bdp_xxxx_s password
    save_file: file name to store the raw console text output
    '''
    
    hive = Hive_console(az_pw=az_pw, 
                        bdp_pw=bdp_pw,
                        edge_node=edge_node, 
                        azure_jump=azure_jump,
                        az=az,
                        bdp=bdp
                        )
    stdin, stdout, stderr = hive.jhost.exec_command(command)
    print('saving to string...')
    result_df = string_2_df(stdout.read().decode("utf-8"), save_file)

    hive.close()
    print('connection closed\n')
    return result_df

  
class Hive_console:
    
    '''
    Connect to Datalake and execute SQL string on Hive. 
    Optionally save query results to Pandas DataFrame.
    
    '''
    
    def __init__(self, 
                 az_pw, 
                 bdp_pw,
                 edge_node='172.22.10.4', 
                 azure_jump='172.20.1.158',
                 az='',
                 bdp=''):
        '''      
        az_pw: azure password
        bdp_pw: bdp_xxxx_s password
        edge_node: edge node ip
        azure_jump: azure jump server ip
        az: azure user name (az_a_xxxxx)
        bdp: bdp_xxxx_s password
        '''
        
        self.az_pw = az_pw
        self.bdp_pw = bdp_pw
        self.edge_node = edge_node
        self.azure_jump = azure_jump
        self.az = az
        self.bdp = bdp
        self.status = 'Disconnected'
        # Start Datalake connection in initiation.
        self._start()
    
    
    def __repr__(self):
        return 'Hive_console({}, {}, {})'.format(self.az, self.bdp, self.status)
    
           
    def _start(self):
        '''
        Start Datalake connnection.
        '''
        self.vm = paramiko.SSHClient()
        self.vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.vm.connect(hostname=self.azure_jump, 
                   username=self.az, 
                   password=self.az_pw)
    
        vmtransport = self.vm.get_transport()
        dest_addr = (self.edge_node, 22)
        local_addr = (self.azure_jump, 22)
    
        vmchannel = vmtransport.open_channel("direct-tcpip", 
                                             dest_addr, 
                                             local_addr)
        self.jhost = paramiko.SSHClient()
        self.jhost.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.jhost.connect(self.edge_node, 
                      username=self.bdp, 
                      password=self.bdp_pw, 
                      sock=vmchannel)
        self.status = 'Connected'
    
    
    def _auto_reconnect(f):
        '''
        Automatically reconnect to Hive if disconnected.
        
        '''
        def wrapper(*args, **kwargs):
            try:
                f(*args, **kwargs)
            except AttributeError:
                print('Disconnected from Hive, auto reconnecting')
                args[0]._start()
                f(*args, **kwargs)
        return wrapper

    
    @_auto_reconnect
    def execute(self, command, verbose=False, to_df=False):
        '''
        From SQL running on hive, to Pandas DataFrame.
        Connect to Hive on Datalake, run the SQL query.
        First saved to a text file, then return a DataFrame.          
        
        command: SQL string
        verbose: default False
        to_df: whether return a DataFrame, default False
        
        The temp file generated by string_2_df() will be removed automatically
        '''
        self.command = '''beeline -e ''' + '''\"''' + command +  '''\"'''
        stdin, stdout, stderr = self.jhost.exec_command(self.command)
        err_msg = str(stderr.read().decode("utf-8"))
        out_msg = str(stdout.read().decode("utf-8"))
        if verbose:
            print(err_msg)
        print(out_msg)
        if (err_msg.lower().find('error') > 0) and (not verbose):
            print(err_msg)
        temp_file = 'temp_file_' + str(time.time())[-6:]
        if to_df:
            result_df = string_2_df(out_msg, temp_file)
            os.remove(temp_file)
            return result_df
        else:
            return
    
    def close(self):
        '''
        Disconnect from Datalake.
        '''
        self.jhost.close()
        self.vm.close()
        self.status = 'Disonnected'
    
    