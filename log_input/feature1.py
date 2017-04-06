import pandas as pd
from pylab import *
import os.path

col_names = ['host', 'space1', 'space2', 'timestamp', 'timezone', 'request', 'code', 'bytes'];
df = pd.read_csv('log.csv', delim_whitespace=True, names=col_names, header=None,
                 error_bad_lines=False);   
# read csv data and place into dataframe
print("Checkpoint 1") 
                             
df_host = df.iloc[:,[0]];   # index out hosts
counts = df_host['host'].value_counts().to_dict();  # get frequency of hosts

sorted_counts = sorted(counts, key=counts.get, reverse=True); # sort hosts
print("Checkpoint 2") 
                      
active = [];
for a in sorted_counts:
    active.append(str(a)+','+str(counts[a])) # show hosts and their frequencies 

most_active = active[0:10]  # index out top 10 hosts
print("Checkpoint 3") 

save_path = 'C:/Users/Ruhul/Desktop/fansite-analytics-challenge/log_output'
hosts = os.path.join(save_path, "hosts.txt")         

the_file = open(hosts, "w")
for item in most_active:
    the_file.write("%s\n" % item)   # write list of top 10 hosts to txt file 
  
the_file.close()
print("Checkpoint 4") 