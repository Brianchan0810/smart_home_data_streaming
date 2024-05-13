import json
import pandas as pd
import datetime as dt

full_df = pd.read_csv('HomeC.csv', header=0)

df = full_df[['time', 'use [kW]', 'gen [kW]', 'icon','temperature', 'humidity', 'windSpeed', 'precipIntensity']]

df = df.rename(columns={'time':'eventTimestamp','use [kW]':'consumption', 'gen [kW]': 'generation', 'icon':'overall'})

df['eventTimestamp'] = pd.to_datetime(df['eventTimestamp'], unit='s')

df['eventSecond'] = df['eventTimestamp'].dt.second

df = df[df['eventSecond']==0].drop(['eventSecond'], axis=1)

df['eventTimestamp'] = df['eventTimestamp'].apply(lambda x: x.isoformat(timespec='seconds'))

data_in_list = []

for record in df.to_dict('records'):
    d = {'eventTimestamp': record['eventTimestamp'],
         'Energy':{
             'consumption': record['consumption'],
             'generation': record['generation']
         },
         'Weather':{'overall':record['overall'],
                   'temperature':record['temperature'],
                   'humidity':record['humidity'],
                   'windSpeed':record['windSpeed'],
                   'precipIntensity':record['precipIntensity']
                   }
        }
    data_in_list.append(d)

with open("smart_home_data.json", "w") as file:
    json.dump(data_in_list, file)