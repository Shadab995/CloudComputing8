
# coding: utf-8

# In[1]:


import sys
path = fr"{str(sys.executable)}"
get_ipython().system('"{path}" -m pip install pymongo --user')
get_ipython().system('"{path}" -m pip install numpy --user')
get_ipython().system('"{path}" -m pip install pandas --user')
get_ipython().system('"{path}" -m pip install matplotlib --user')


# In[3]:


import pymongo, numpy as np, pandas as pd, matplotlib, csv, json


# Quick importation of several packages, including pymongo for mongoDB connection and queries.

# In[45]:


from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')
db = client["cloud_app"]
Accidents = db["Accidents"]
Casualties = db["Casualties"]
Vehicles = db["Vehicles"]

#Code runs with the csv files in the same local directory for relative path use, i've renamed the files by removing the nymbers
#at the end, feel free to put them back in the code or rename them in your file system

chunksize = 50000
for chunk in pd.read_csv('Accidents.csv', chunksize=chunksize,index_col=False,low_memory=False):
    chunk.rename(columns = {'Accident_Index':'_id'}, inplace = True)
    Accidents.insert_many(chunk.to_dict('records'))
for chunk in pd.read_csv('Casualties.csv', chunksize=chunksize,index_col=False,low_memory=False):
    chunk['_id'] = [{'Accident_Index':key1,'Casualty_Reference':key2,'Vehicle_Reference':key3} for key1, key2,key3 in zip(chunk.Accident_Index, chunk.Casualty_Reference, chunk.Vehicle_Reference)]
    del chunk['Casualty_Reference']
    del chunk['Accident_Index']
    del chunk['Vehicle_Reference']
    Casualties.insert_many(chunk.to_dict('records'))
for chunk in pd.read_csv('Vehicles.csv', chunksize=chunksize,index_col=False,low_memory=False):
    chunk['_id'] = [{'Accident_Index':key1,'Vehicle_Reference':key2} for key1, key2 in zip(chunk.Accident_Index, chunk.Vehicle_Reference)]
    del chunk['Vehicle_Reference']
    del chunk['Accident_Index']
    Vehicles.insert_many(chunk.to_dict('records'))


# we'll use that to query the data to then process it and insert it back in the denormalized schema
# Since we'll have to do operations on data on specific instances, it'll be easier to query mongodb to get them in the right order to not miss any rows when we reformat the data.
