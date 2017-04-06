import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pylab import *
import os.path

col_names = ['host', 'space1', 'space2', 'timestamp', 'timezone', 'request', 'code', 'bytes']
df1 = pd.read_csv('log.csv', delim_whitespace=True, names=col_names, header=None,
                 error_bad_lines=False);     
# read csv data and place into dataframe

df1['freq'] = df1.groupby('code')['code'].transform('count');

df2 = df1[df1.code != 200]





count = 0
b = 0
result = []                       
for a in df2.index:
    for d in a:
        if abs(d - c) <= 2:
            count = count + 1
            b = b + 1
    result.append(str(c)+','+str(count))
    count = 0
    b = 0 
    



'''                       
a = [200,4,5,7,8,11]
count = 0
b = 0
result = []
for c in a:
    for d in a:
        if abs(d - c) <= 2:
            count = count + 1
            b = b + 1
    result.append(str(c)+','+str(count))
    count = 0
    b = 0                       
   
'''

   