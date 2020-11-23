#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 15:41:32 2020

@author: patrickshu
"""

import paramiko
from stdout_to_df import file_to_df

# sample SQL query.
#following beeline syntax, need to add "" surrounding the sql query
'''beeline -e "select vin from revr_bmbs_dmp_offline.rcrd_vhcl_srvc where vin = 'LE40G4KB9HL101219'"'''

def connect_dl(command,
               az_pw,
               bdp_pw,
               edge_node='172.22.10.4', 
               azure_jump='172.20.1.158',
               az='az_a_ShuYingmu',
               bdp='bdp_admin_s',
               save_file='lake_output'
               ):
    '''
    command: SQL string
    az_pw: azure password
    bdp_pw: bdp_xxxx_s password
    edge_node: edge node ip
    azure_jump: azure jump server ip
    az: azure user name (az_a_xxxxx)
    bdp: bdp_xxxx_s password
    save_file: file name to store the raw console text output
    '''
    #print('start connecting...')
    vm = paramiko.SSHClient()
    vm.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    vm.connect(hostname=azure_jump, username=az, password=az_pw)
    
    vmtransport = vm.get_transport()
    dest_addr = (edge_node, 22)
    local_addr = (azure_jump, 22)
    
    vmchannel = vmtransport.open_channel("direct-tcpip", dest_addr, local_addr)
    jhost = paramiko.SSHClient()
    jhost.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    jhost.connect(edge_node, username=bdp, password=bdp_pw, sock=vmchannel)
    
    print('hive running...')
    stdin, stdout, stderr = jhost.exec_command(command)
    print('saving to file -', save_file)
    with open(save_file, 'w') as f:
        f.write(stdout.read().decode("utf-8")) 
    print('saving to dataframe...')
    result_df = file_to_df(save_file)
    jhost.close()
    vm.close()
    print('connection closed\n')
    return result_df

  
class Hive_console:
    '''
    execute SQL string on Datalake
    
    '''
    
    def __init__(self, 
                 az_pw, 
                 bdp_pw,
                 edge_node='172.22.10.4', 
                 azure_jump='172.20.1.158',
                 az='az_a_ShuYingmu',
                 bdp='bdp_admin_s'):
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
        self._start()
           
    def _start(self):
        '''
        manually start Datalake connnection
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

    
    def execute(self, command, verbose=False):
        '''
        command: SQL string
        verbose: default False
        '''
        self.command = '''beeline -e ''' + '''\"''' + command +  '''\"'''
        stdin, stdout, stderr = self.jhost.exec_command(self.command)
        err_msg = str(stderr.read().decode("utf-8"))
        if verbose:
            print(err_msg)
        print(stdout.read().decode("utf-8"))
        if (err_msg.lower().find('error') > 0) and (not verbose):
            print(err_msg)
       
    
    def close(self):
        '''
        disconnect from Datalake
        '''
        self.jhost.close()
        self.vm.close()
    
    
    