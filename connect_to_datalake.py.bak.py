#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 15:41:32 2020

@author: patrickshu
"""


import paramiko
from stdout_to_df import file_to_df
import pandas as pd

pw_path = '/Users/patrickshu/Desktop/PW.xlsx'
pw = pd.read_excel(pw_path, 
                   sheet_name='pw', 
                   header=None)

edge_node = '172.22.10.4'
azure_jump = '172.20.1.158'
az = 'az_a_ShuYingmu'
az_pw = pw.loc[pw[0]==az,1].values[0]
bdp = 'bdp_admin_s'
bdp_pw = pw.loc[pw[0]==bdp,1].values[0]

#sql = "select vin, datediff(CURRENT_DATE, REGDATE) days, num_of_mantenance, num_of_normal_srvc, num_of_checking, num_of_e_repairing, num_of_painting, num_of_other_srvc from revr_bmbs_dmp_offline.rcrd_vhcl_srvc where vin = 'LE40G4KB9HL101219'"
command = '''beeline -e "select vin, datediff(CURRENT_DATE, REGDATE) days, num_of_mantenance, num_of_normal_srvc, num_of_checking, num_of_e_repairing, num_of_painting, num_of_other_srvc from revr_bmbs_dmp_offline.rcrd_vhcl_srvc where vin = 'LE40G4KB9HL101219'"'''

def test():
    print('here it is - inner folder')


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
    '''
    print('start connecting...')
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
    
    print('connected...')
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
  
def connect_dl_console_output(command,
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
    
    DOESNT WORK YET
    '''
    print('start connecting...')
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
    
    print('connected...')
    stdin, stdout, stderr = jhost.exec_command(command)
    print('saving to file -', save_file)
    with open(save_file, 'w') as f:
        f.write(stdout.read().decode("utf-8")) 
    print(stdout)
    #result_df = file_to_df(save_file)
    jhost.close()
    vm.close()
    print('connection closed\n')  


if __name__ == '__main__':
    connect_dl(az_pw=az_pw,
               bdp_pw=bdp_pw,
               command=command)
