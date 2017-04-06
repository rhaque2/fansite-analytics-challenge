import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pylab import *
import os.path

col_names = ['host', 'space1', 'space2', 'timestamp', 'timezone', 'request', 'code', 'bytes'];
df1 = pd.read_csv('log.csv', delim_whitespace=True, names=col_names, header=None,
                 error_bad_lines=False);   
# read csv data and place into dataframe
df2 = df1.iloc[:,[5,7]];  # index out request and bytes

df2['freq'] = df2.groupby('request')['request'].transform('count');
df2_bytes = df2.iloc[:,[1]];  # index out bytes to new dataframe 
df2_freq = df2.iloc[:,[2]];  # index out frequencies of request to new dataframe 
                   
df2_bytes = df2_bytes.rename(columns={'bytes': 'bandwidth_consumed'});                
df2_freq = df2_freq.rename(columns={'freq': 'bandwidth_consumed'});   
df2_bytes = df2_bytes.apply(int64);  # turn bytes into integer
bandwidth_consumed = df2_bytes * df2_freq;   

df2['bandwidth_consumed'] = bandwidth_consumed;
initial_resources = df2.sort(['bandwidth_consumed'], ascending=[False]);

new_resources = initial_resources.drop_duplicates(subset=['request', 'bytes', 'freq', 
'bandwidth_consumed'], keep='first');
   
newer_resources = new_resources.drop_duplicates(subset=['request'], keep='first');  

top_resources = newer_resources.request[0:10]   # index out top 10 resources
intensive = [];
for a in top_resources:
    intensive.append(str(a))  # put resources into list called "intensive"       


save_path = 'C:/Users/Ruhul/Desktop/fansite-analytics-challenge/log_output'
resources = os.path.join(save_path, "resources.txt")         

the_file = open(resources, "w")
for item in intensive:
    the_file.write("%s\n" % item)  # write list of top 10 resources to txt file 
  
the_file.close()
              