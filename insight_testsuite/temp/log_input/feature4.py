import pandas as pd
from pylab import *
import os.path
from datetime import datetime
from datetime import timedelta

col_names = ['host', 'space1', 'space2', 'timestamp', 'timezone', 'request', 'code', 'bytes']
df1 = pd.read_csv('log.csv', delim_whitespace=True, names=col_names, header=None,
                 error_bad_lines=False);     
# read csv data and place into dataframe
print("Checkpoint 1") 

dftime = df1.iloc[:,[3,4]];   # index out timestamp and timezone
timestamp = list(dftime.timestamp);  # turn timestamp into list
timezone = list(dftime.timezone);  # turn timezone into list
print("Checkpoint 2") 
  
time = [];        
for i, j in zip(timestamp, timezone):
    time.append(str(i)+str(j));   # combine to form a list with full "time" 
print("Checkpoint 3")     

df1['time'] = time;   # put the time inside the original dataframe

df1['time'] = pd.to_datetime(df1['time'], format='[%d/%b/%Y:%H:%M:%S-0400]');

df1['freq'] = df1.groupby('code')['code'].transform('count'); # group freq. of codes 
    
df2 = df1[df1.code != 200];  # extract rows without 200 as reply code
print("Checkpoint 4")     
         
# This loop will help find which indexes are consecutive
speed = 0;
result = [];
count = 0;
for i in range(len(df2.index)):
    if i > 0 and (df2.index[i] - df2.index[i-1]) == 1:  # if the difference is 1
        count += 1;
    else:
        count = 0;
    if count > 1: 
        result.append(str(df2.index[i-1])+','+str(count))  
        # make a list with 2nd of 3 failed attempts
        count = 0; 
    speed += 1;    
    if(speed % 100000 == 0):
        print(speed)   # checking how fast the code is running       
        
print("Checkpoint 5") 
   
freq = [];
for element in result:
    parts = element.split(',')
    freq.append(parts)
print("Checkpoint 6")     
           
df3 = pd.DataFrame(freq, columns=list('xy'));  # turn list into a data frame
df3 = df3.rename(columns={'x': 'placement'});  # change column name               
df3 = df3.rename(columns={'y': 'amount'});    # change column name 

df4 = df3[df3.amount.str.contains("2") == True];  # only keep rows with 2 as string value
df4 = df4.convert_objects(convert_numeric = True);  # convert into integers
print("Checkpoint 7")    

trios = [];   # 3 consecutive failed attempt trios will go here 
first_attempt = []; 
third_attempt = [];        
for value in df4.placement:
    first = value - 1;   
    second = value;
    third = value + 1;
    trios.append(str(first)+','+str(second)+','+str(third))
    first_attempt.append(str(first))
    third_attempt.append(str(third))

first_attempt = [int(i) for i in first_attempt]; # index for start of 20-second timer
third_attempt = [int(i) for i in third_attempt]; # index for start of blockage
print("Checkpoint 8") 

timer_start = [];
for g in first_attempt:    
    timer_start.append(str(df1.time[g]))    
    
timer_start = pd.to_datetime(timer_start);  # 20-second timer starts now
print("Checkpoint 9")                             
                 
blockage_start = [];
for p in third_attempt:    
    blockage_start.append(str(df1.time[p]))
    
blockage_start = pd.to_datetime(blockage_start); 
print("Checkpoint 10")
                     
# 20 second test   

third_attempt20 = [];                               
blockage_start20 = []; 
for c, d, e in zip(timer_start, blockage_start, third_attempt):
    if (d - c) <= timedelta(seconds = 20):   # 20 second clock
       blockage_start20.append(d)
       third_attempt20.append(e)           

blockage_start20 = pd.to_datetime(blockage_start20); # start of blockage                   
print("Checkpoint 11") 
                                                                              
# Which ones to block? 
                 
blocked_hosts = [];
for k in third_attempt20:
    blocked_hosts.append(str(df1.host[k]))  # get list of blocked hosts
print("Checkpoint 12") 

# When to end the block?
                             
blockage_end = [];                              
for q in blockage_start20:
    blockage_end.append(str(q + timedelta(minutes = 5)))
                        
blockage_end = pd.to_datetime(blockage_end); # end of blockage  
print("Checkpoint 13") 
                            
# Here is where the code slows down

speed = 0; 
mask = [];            
for s, t in zip(blockage_start20, blockage_end):                              
    mask.append((df1['time'] > s) & (df1['time'] <= t)); # Boolean mask
    # to find 5 minute blocks within the data    
    speed += 1;       
    if(speed % 10 == 0):
        print(speed)   # checking how fast the code is running  

speed = 0;   
combined_df = [];
for u in mask:
    new_df = df1.loc[u];
    combined_df.append(new_df);  # get list of dataframes
    speed += 1;       
    if(speed % 50 == 0):
        print(speed)   # checking how fast the code is running                     

combined_df = pd.concat(combined_df);  # turn list of dataframes into dataframe
print("Checkpoint 14") 

speed = 0;                       
mask2 = [];            
for w in blocked_hosts:                              
    mask2.append(combined_df['host'] == w);  # append blocked hosts to a Boolean mask                       
    speed += 1;    
    if(speed % 10 == 0):
        print(speed)   # checking how fast the code is running 

speed = 0; 
final_df = [];
for v in mask2:
    newer_df = combined_df.loc[v];
    final_df.append(newer_df);   # get list of dataframes 
    speed += 1;    
    if(speed % 50 == 0):
        print(speed)   # checking how fast the code is running 

final_df = pd.concat(final_df);  # turn list of dataframes into dataframe
print("Checkpoint 15") 
                    
final_host = list(final_df.host);   # turn dataframe column into a list                  

final_dftime = final_df.iloc[:,[3,4]];   # index out timestamp and timezone
final_timestamp = list(final_dftime.timestamp);  # turn timestamp into list
final_timezone = list(final_dftime.timezone);  # turn timezone into list
                    
final_time = [];        
for i, j in zip(final_timestamp, final_timezone):
    final_time.append(str(i)+str(j));   # combine to form a list with full "time" 
    
final_request = list(final_df.request);  # turn dataframe column into a list                      
final_code = list(final_df.code)   # turn dataframe column into a list 
final_bytes = list(final_df.bytes)   # turn dataframe column into a list 
print("Checkpoint 16") 
                    
complete = [];
for a, b, c, d, e in zip(final_host, final_time, final_request, final_code, final_bytes):
    complete.append(str(a)+' - - '+str(b)+' '+str(c)+' '+str(d)+' '+str(e)) 

save_path = 'C:/Users/Ruhul/Desktop/fansite-analytics-challenge/log_output'
blocked = os.path.join(save_path, "blocked.txt")         

the_file = open(blocked, "w")
for item in complete:
    the_file.write("%s\n" % item)   # write list of attempts to txt file 
  
the_file.close()
print("Checkpoint 17")      
           