import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pylab import *
import os.path
from datetime import datetime
from datetime import timedelta

col_names = ['host', 'space1', 'space2', 'timestamp', 'timezone', 'request', 'code', 'bytes']
df1 = pd.read_csv('log.csv', delim_whitespace=True, names=col_names, header=None,
                 error_bad_lines=False);     
# read csv data and place into dataframe
df2 = df1.iloc[:,[3,4]];   # index out timestamp and timezone
timestamp = list(df2.timestamp)  # turn timestamp into list
timezone = list(df2.timezone)  # turn timezone into list
  
time = []            
for i, j in zip(timestamp, timezone):
    time.append(str(i)+str(j));   # combine to form a list with full "time" 

df2['time'] = time;   # turn "time" into dataframe

time_format = pd.to_datetime(df2['time'], format='[%d/%b/%Y:%H:%M:%S-0400]');
 

# This part is very slow                            
count = 1;
index = 0;
temp_Index = index;
result = [];
for value in time_format:    # goes through each timestamp
    temp_Index = index;
    while temp_Index - 1 >= 0:
        temp_Index -= 1;
        if abs(value - time_format[temp_Index]) <= timedelta(hours = 1):
            count += 1;
        else:
            temp_Index = -1;
    temp_Index = index;
    while temp_Index + 1 < len(time_format):
        temp_Index += 1;
        if abs(time_format[temp_Index] - value) <= timedelta(hours = 1):
            count += 1;
        else:
            temp_Index = len(time_format);
    result.append(str(value)+','+ str(count)); # add timestamp and count to list
    index += 1;
    count = 1;
                              
                          

freq = [];
for element in result:
    parts = element.split(',')
    freq.append(parts)
       
    
new_df = pd.DataFrame(freq, columns=list('xy'))

new_df['y'] = new_df.y.apply(int64); # turn the frequencies into integers

sorted_freqs = new_df.sort(['y'], ascending=[False]);
                          
top_hours = sorted_freqs[0:10];   # index out top 10 hours
start_hour = top_hours.iloc[:,[0]];  
visits = top_hours.iloc[:,[1]];   
new_start_hour = list(start_hour.x);  # turn start_hour into list
new_visits = list(visits.y);  # turn visits into list                       
                        
busiest = [];
for m, n in zip(new_start_hour, new_visits):
    busiest.append(str(m)+' -0400,'+str(n));   # combine to form final list                        
    
save_path = 'C:/Users/Ruhul/Desktop/fansite-analytics-challenge/log_output'
hours = os.path.join(save_path, "hours.txt")         

the_file = open(hours, "w")
for item in busiest:
    the_file.write("%s\n" % item)  # write list of top 10 hours to txt file 
  
the_file.close()
                                             













