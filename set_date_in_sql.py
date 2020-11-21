#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 14 15:59:18 2020

@author: patrickshu
"""

def replace_date(file_name, old, new):
	"""
	replace certain text in a text file.
	
	file_name: file name of the text file.
	old: old string, to be replaced
	new: new string, the string to replace the old
	"""
	with open(file_name, 'r') as f:
		string_result = f.read().replace(old, new)
	with open(file_name, 'w') as f:
		f.write(string_result)


		