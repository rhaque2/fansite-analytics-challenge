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
timestamp = list(dftime.timestamp)  # turn timestamp into list
timezone = list(dftime.timezone)  # turn timezone into list
print("Checkpoint 2")               

time = [];            
for i, j in zip(timestamp, timezone):
    time.append(str(i)+str(j));   # combine to form a list with full "time" 
print("Checkpoint 3") 

df1['time'] = time;   # put the time inside the original dataframe
print("Checkpoint 4")   

time_format = pd.to_datetime(df1['time'], format='[%d/%b/%Y:%H:%M:%S-0400]');
# turn dataframe column into datetime format 
print("Checkpoint 5")
     
count = 0;
result = [];
counter_list = [];
for value in time_format:
    if len(counter_list) == 0:
        counter_list.append(value)  # appending the first value to list
    else:
        while (value - counter_list[0]) > timedelta(hours = 1):
            result.append(str(counter_list[0])+','+str(len(counter_list)))
            counter_list.pop(0)   # takes out first value from counter_list
        counter_list.append(value)   # keeps appending until hour past earliest 
    count += 1
    if(count % 100000 == 0):
        print(count)

print("Checkpoint 6")      
                     
freq = [];
for element in result:
    parts = element.split(",")
    freq.append(parts)
print("Checkpoint 7")
           
new_df = pd.DataFrame(freq, columns=list('xy'));

new_df['y'] = new_df.y.apply(int64); # turn the frequencies into integers

sorted_freqs = new_df.sort(['y'], ascending=[False]);
print("Checkpoint 8")                          

new_hours = sorted_freqs.drop_duplicates(subset=['x'], keep='first');                           
top_hours = new_hours[0:10];   # index out top 10 hours
start_hour = top_hours.iloc[:,[0]];  
visits = top_hours.iloc[:,[1]];   
new_start_hour = list(start_hour.x);  # turn start_hour into list
new_visits = list(visits.y);  # turn visits into list  
print("Checkpoint 9")                     
                        
busiest = [];
for m, n in zip(new_start_hour, new_visits):
    busiest.append(str(m)+' -0400,'+str(n));   # combine to form final list
print("Checkpoint 10")                        
    
save_path = 'C:/Users/Ruhul/Desktop/fansite-analytics-challenge/log_output'
hours = os.path.join(save_path, "hours.txt")         

the_file = open(hours, "w")
for item in busiest:
    the_file.write("%s\n" % item)  # write list of top 10 hours to txt file 
  
the_file.close()
print("Checkpoint 11")                     