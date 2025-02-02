# -*- coding: utf-8 -*-
"""
Created on Sat Sep 28 13:19:41 2019

@author: gurjaspal
"""
#REFERENCES
#https://realpython.com/python-csv/
#https://realpython.com/python-sockets/
#https://cs.lmu.edu/~ray/notes/pythonnetexamples/
#https://realpython.com/read-write-files-python/
#https://realpython.com/python-sockets/
import csv
from configuration import initial_data_path, intermediate_data_path, final_data_path
from rpc_constants import INITIAL_STAGE, INTERMEDIATE_STAGE, FINAL_STAGE


class Database:
    
    def __init__(self):
        self.initial_data_path = initial_data_path
        self.intermediate_data_path = intermediate_data_path
        self.final_data_path = final_data_path
        
#        Start with empty file if the Database is reinitialized
        with open(self.initial_data_path, "w+"):
            pass
        
        with open(self.intermediate_data_path, "w+"):
            pass
        
        with open(self.final_data_path, "w+"):
            pass        
        
        self.initial_file = open(self.initial_data_path, "r+")
        self.intermediate_file = open(self.intermediate_data_path, "r+")
        self.final_file = open(self.final_data_path, "r+")
        fieldnames = ['key', 'value']

        self.writer_initial_data = csv.DictWriter(self.initial_file, fieldnames=fieldnames)
        self.writer_initial_data.writeheader()
        
        self.writer_intermediate_data = csv.DictWriter(self.intermediate_file, fieldnames=fieldnames)
        self.writer_intermediate_data.writeheader()
        
        self.writer_final_data = csv.DictWriter(self.final_file, fieldnames=fieldnames)
        self.writer_final_data.writeheader()
        
        self.reader_initial = csv.DictReader(self.initial_file)
        self.reader_intermediate = csv.DictReader(self.intermediate_file)
        self.reader_final = csv.DictReader(self.final_file)
            
    def set_value(self, key, value, data_stage):  
        
        if data_stage == INITIAL_STAGE:
            
            self.writer_initial_data.writerow({'key':key, 'value': value})
            self.initial_file.flush()
            
        elif data_stage == INTERMEDIATE_STAGE:
            
            self.writer_intermediate_data.writerow({'key':key, 'value': value})
            self.intermediate_file.flush()
            
        elif data_stage == FINAL_STAGE:
            
            self.writer_final_data.writerow({'key':key, 'value': value})
            self.final_file.flush()
        
    def get_value(self, key , stage):
        path = ""
        if stage == INITIAL_STAGE:
            path = self.initial_data_path
        elif stage == INTERMEDIATE_STAGE:
            path = self.intermediate_data_path
        elif stage == FINAL_STAGE:
            path = self.final_data_path
        
        with open(path) as f:
            reader = csv.DictReader(f)
            for row in reversed(list(reader)):
                if row['key'] == key:
                    return row['value']
        return None, None
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    