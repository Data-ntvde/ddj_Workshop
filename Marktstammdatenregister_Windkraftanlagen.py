#!/usr/bin/env python
# coding: utf-8

# # Marktstammdatenregister: Windkraftanlagen
# 
# https://www.marktstammdatenregister.de/MaStR/Einheit/Einheiten/OeffentlicheEinheitenuebersicht
# 
# Dieser Code liest die Daten für den Energieträger "Wind" aus dem Marktstammdatenregister aus. 
# Datenabfragen sind bis max 5000 Zeilen möglich, daher wird die Abfrage nach Bundesländern und wo nötig nach weiteren Kriterien aufgesplittet. 
# Ergebnisse werden zu einem dataframe zusammengefügt. 

# In[1]:


import pandas as pd
import datetime
import requests
import json


# In[2]:


laender_id_dict = {'00': 'Brandenburg',
                   '01': 'Berlin',
                   '02': 'Baden-Württemberg',
                   '03': 'Bayern',
                   '04': 'Bremen',
                   '05': 'Hessen',
                   '06': 'Hamburg',
                   '07': 'Mecklenburg-Vorpommern',
                   '08': 'Niedersachsen',
                   '09': 'Nordrhein-Westfalen',
                   '10': 'Rheinland-Pfalz',
                   '11': 'Schleswig-Holstein',
                   '12': 'Saarland',
                   '13': 'Sachsen',
                   '14': 'Sachsen-Anhalt',
                   '15': 'Thüringen',
                   '16': 'AWZ' # Ausschließliche Wirtschaftszone
                  }            


# In[3]:


bundeslaender_ids = list(laender_id_dict.keys())
#print(bundeslaender_ids)


# In[4]:


for b in bundeslaender_ids:
    land_url = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=100000&group=&filter=Bundesland~eq~%2714' + b + '%27~and~Energietr%C3%A4ger~eq~%272497%27'
    print(land_url)


# In[5]:


def get_data(bundesland_id):
    
    if bundesland_id == '08': # Niedersachsen muss wegen >6000 WKA noch mal gefiltert werden
        NI_bruttoleistung_ueber_2000 = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=5000&group=&filter=Bruttoleistung%20der%20Einheit~gt~%272000%27~and~Bundesland~eq~%271408%27~and~Energietr%C3%A4ger~eq~%272497%27'
        NI_bruttoleistung_unter_2001 = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=5000&group=&filter=Bruttoleistung%20der%20Einheit~lt~%272001%27~and~Bundesland~eq~%271408%27~and~Energietr%C3%A4ger~eq~%272497%27'
        
        two_links = [NI_bruttoleistung_ueber_2000, NI_bruttoleistung_unter_2001]
        
        df = pd.DataFrame()
        
        for link in two_links:
            r = requests.get(link)
            content = r.text
            get_json = json.loads(content)
            data = get_json['Data']
            NI_df = pd.DataFrame(data)
            
            df = df.append(NI_df)
        
    else:
    
        land_url = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=100000&group=&filter=Bundesland~eq~%2714' + bundesland_id + '%27~and~Energietr%C3%A4ger~eq~%272497%27'
    
        r = requests.get(land_url)
        content = r.text
        get_json = json.loads(content)
        data = get_json['Data']
        df = pd.DataFrame(data)
    
    return df


# In[6]:


all_dfs = pd.DataFrame()

for bl in bundeslaender_ids:
    df = get_data(bl)
    all_dfs = all_dfs.append(df)

#all_dfs


# In[7]:


all_cols = list(all_dfs.columns)
date_cols = [all_cols[6], all_cols[7], all_cols[9]]
date_cols


# In[8]:


# Datumsangaben liegen als str vor, der einen timecode enthält. 
# Diese Funktion dient der Konvertierung in datetime zur Anwendung auf die Datumsspalten

def timestamp_to_datetime(str_date):
    
    try:
        timestamp_x = str_date.strip('/Date(').strip(')/')
        datetime_x = pd.to_datetime(int(timestamp_x), utc=True, unit='ms').date()
    
    except:
        datetime_x = '-'
    
    return datetime_x 


# In[9]:


#Anwendung auf die Datumsspalten

for each_date_col in date_cols: 
    all_dfs[each_date_col] = all_dfs.apply(lambda row: timestamp_to_datetime(row[each_date_col]), axis=1)
    
all_dfs


# In[15]:


full_data_df = all_dfs.copy()
full_data_df.dtypes


# In[16]:


# cast IDs and to str and values to int

id_cols = ['AnlagenbetreiberId', 'PersonenArtId', 'LokationId']
value_cols = ['Bruttoleistung', 'Nettonennleistung']

full_data_df[id_cols] = full_data_df[id_cols].astype('Int64').astype(str)
full_data_df[value_cols] = full_data_df[value_cols].astype(int)


# In[17]:


full_data_df


# In[18]:


full_data_df.dtypes


# In[ ]:


full_data_df.to_csv('Windkraftanlagen_DE_full_data.csv', index=False)


# # Offshore Data

# In[ ]:


'''

def get_offshore_data(offshore_url):
    
    r = requests.get(offshore_url)
    content = r.text
    get_json = json.loads(content)
    data = get_json['Data']
    df = pd.DataFrame(data)
    
    return df
    
'''


# In[ ]:


'''

cluster_nordsee = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=4000&group=&filter=Energietr%C3%A4ger~eq~%272497%27~and~Cluster%20Nordsee~eq~%271546%2C1555%2C1556%2C1557%2C1558%2C1547%2C1548%2C1549%2C1550%2C1551%2C1552%2C1553%2C1554%2C2963%27'
cluster_ostsee = 'https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=1000&group=&filter=Cluster%20Ostsee~eq~%271540%2C1541%2C1542%2C1543%2C1544%2C2970%2C2962%27~and~Energietr%C3%A4ger~eq~%272497%27'

nordsee_df = get_offshore_data(cluster_nordsee)
ostsee_df = get_offshore_data(cluster_ostsee)

offshore_dfs = nordsee_df.append(ostsee_df)

offshore_dfs
#print('Ostseee:', len(ostsee_df))
#print('Nordsee:', len(nordsee_df))

'''


# 33714 Ergebnisse für Wind
# 
# BW: 953 
# BY: 1342
# BE: 10
# BB: 4350
# HB: 94
# HH: 68
# HE: 1256
# MV: 2054
# NI: 6842
# NW: 4208
# RP: 1886
# SL: 237
# SN: 972
# SA: 3045
# SH: 3890
# TH: 989
# 
# NI > 2000: 2684
# NI < 2001: 4158
# Summe: 6842
# 
# Onshore: 32196
# (Summe Bundeslaender)
# 
# AWZ: 1518
# 
# Ostseee: 308
# Nordsee: 1306
# offshore: 1614
# 

# Baden-Württemberg
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271402%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Bayern
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271403%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Berlin
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271401%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Brandenburg
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271400%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Bremen
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271404%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Hamburg
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271406%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Hessen
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271405%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Mecklenburg-Vorpommern
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271407%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Niedersachsen
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271408%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Nordrhein-Westfalen
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271409%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Rheinland-Pfalz
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271410%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Saarland
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271412%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Sachsen
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271413%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Sachsen-Anhalt
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271414%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Schleswig-Holstein
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271411%27~and~Energietr%C3%A4ger~eq~%272497%27
# 
# Thüringen
# https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetVerkleinerteOeffentlicheEinheitStromerzeugung?sort=EinheitMeldeDatum-desc&page=1&pageSize=10&group=&filter=Bundesland~eq~%271415%27~and~Energietr%C3%A4ger~eq~%272497%27
