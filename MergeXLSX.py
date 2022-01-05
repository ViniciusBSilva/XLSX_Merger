# -*- coding: utf-8 -*-
"""
Created on Tue Dec 14 17:20:35 2021

@author: vinsilva
"""

import pandas as pd
import os

from datetime import datetime

class FileReader:
    
    FILL_NA_WITH = ''
    
    def __init__(self, path):
        self.__path = path


    def __read_file(self, filename):
        full_path = '{}\{}'.format(self.__path,filename)
        df = pd.read_excel( full_path )
        df = df.fillna(self.FILL_NA_WITH)
        return File(filename, df);
    
    
    def read_all_files(self):
        file_list = []
        for filename in os.listdir( self.__path ):
            file = self.__read_file(filename)
            file_list.append(file)
        return file_list
    

class File:
    
    keys = ['Sender_Partner', 'Sender_Component', 'Receiver_Partner', 'Receiver_Component', 'Interface', 'Interface_Namespace', 'Scenario_Identifier']
    
    def __init__(self, filename, df):
        self.__filename = filename
        self.__df = df
        self.__set_prefix()
        self.__fix_columns()
   
    
    def __set_prefix(self):
        self.__prefix = self.__filename.split(sep='_', maxsplit=1)[0]
    
    
    def __fix_columns(self):
        self.__remove_columns_spaces()
        self.__rearrange_columns()
        self.__rename_columns()
    
    
    def __remove_columns_spaces(self):
        rename_columns = []
        for column in self.__df.columns:
            rename_column = column.replace(' ', '_')
            rename_columns.append(rename_column)
        self.__df.set_axis(rename_columns, axis=1, inplace=True)


    def __rearrange_columns(self):
        column_list = File.keys.copy();
        column_list.append('Error')
        column_list.append('Scheduled')
        column_list.append('Successful')
        column_list.append('Terminated_with_error')
        self.__df = self.__df[column_list]
        
        
    def __rename_columns(self): 
        map = { 'Error' : '{}_Error'.format(self.__prefix),
                'Scheduled' : '{}_Scheduled'.format(self.__prefix),
                'Successful' : '{}_Successful'.format(self.__prefix),
                'Terminated_with_error' : '{}_Terminated_with_error'.format(self.__prefix) }
        self.__df.rename(columns=map, inplace=True)
   
    def get_key_values(self):
        return self.__df[File.keys]
        
    def get_df(self):
        return self.__df
        
    
class FileMerger:
    
    PREFIX = 'Merge_'
    
    def __init__(self, file_list):
        self.file_list = file_list    
    
        
    def merge(self):
        self.df_merged = self.__create_keys_file()
        for item in file_list:
            self.df_merged = self.df_merged.merge(item.get_df(), how='left', on=File.keys)
    
    
    def __create_keys_file(self):
        df_list = []
        for item in self.file_list:
            df_list.append(item.get_df()[File.keys])
        df_keys = pd.concat(df_list)
        df_keys.sort_values(File.keys, ascending=True, inplace=True)
        df_keys.drop_duplicates(subset=File.keys, inplace=True)
        return df_keys
    
    
    def export_to_excel(self, root_path, print_path):
        file_path = r'{}\{}{}.xlsx'.format(root_path, FileMerger.PREFIX, datetime.now().strftime("%Y%m%d_%H%M%S"))
        self.df_merged.to_excel(file_path, index=False)
        if print_path:
            print(file_path)
    
root_path = input("Root path: ")    
files_path = '{}\merge'.format(root_path)
        
file_reader = FileReader(files_path)
file_list = file_reader.read_all_files()

merger = FileMerger(file_list)
merger.merge()
merger.export_to_excel(root_path, print_path=True)
        



