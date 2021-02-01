#!/usr/bin/env python
"""
Convert and prepare input tripdata csv file to parquet 
"""
import pandas as pd
from pandas import to_datetime
import numpy as np
import sys

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate distance in miles between two cooridnates
    latitidude, longitude in decimal degrees 

    """
    lon1, lat1, lon2, lat2 = map(np.radians ,[lon1, lat1, lon2, lat2])

    h = np.sin((lat2-lat1)/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2-lon1)/2.0)**2

    miles = 3959 * (2 * np.arcsin(np.sqrt(h))) 
    return miles 
    
def preparefile(filename):
    """
    Clean and convert citibike tripdata csv file to parquet 
    """
    try:
        df = pd.read_csv(filename)
    except IOError as e:
        print(e)
        exit 
    
    # convert start and end time columns to datetime 
    df['starttime'] = pd.to_datetime(df['starttime'])
    df['stoptime'] = pd.to_datetime(df['stoptime'])
    
    # convert trip duration from seconds to minutes 
    df['tripminutes'] = df['tripduration'] // 60 
    
    # exctract date and time components, convert to category 
    df['start hour']=df['starttime'].dt.hour.astype('category')
    df['start day']=df['starttime'].dt.day.astype('category')
    df['weekday']=df['starttime'].dt.weekday.astype('category') # day of week 
    
    # create weekend column as True for weekend days 
    df['weekend'] = [ d >= 5 for d in df['weekday']]
        
    # convert object columns to categories
    cols = ['start station name', 'end station name', 'bikeid',  'usertype', 'gender']
    for col in cols:
        df[col] = df[col].astype('category')
    
    # Create age column, omitting rows w/o gender specified to avoid default 1969
    # and born before 1946 
    skip = (df['gender'] == 0) | (df['birth year'] < 1946) 
    df['age'] = (2020 - df['birth year']).mask(skip,None)
    
    # calculate distance between stations 
    df['distance']=haversine(df['start station latitude'],df['start station longitude'],df['end station latitude'],df['end station longitude'])

    # write file with same name and extension parquet
    parquetfile=filename.split('.')[0]+'.parquet'
    df.to_parquet(parquetfile) 
    return parquetfile 


if __name__ == "__main__":
    print(f"Processing file: {sys.argv[1]}")
    file = preparefile(sys.argv[1])
    print(f"Complete: {file}")
    
