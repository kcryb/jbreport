#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 30 11:09:46 2019

@author: caseyburridge
"""

import pandas as pd
import prestodb

#Connect to database
conn = prestodb.dbapi.connect (
host = 'prs-eu-1.avct.io',
port = 8080,
user = 'casey',
catalog = 'hive',
schema = 'default'
)
cur = conn.cursor()

date1 = '2019-01-29'
date2 = '2019-01-29'

def querydata():
        
    query = f"SELECT DATE_FORMAT(ymdh AT TIME ZONE 'UTC', '%Y-%m-%d') AS \"ymd\", tacticlabels.name AS \"tactic_name\", IF(IS_NAN(ROUND((SUM(revenue) / 1000), 3)), 0, COALESCE(NULLIF(ROUND((SUM(revenue) / 1000), 3), INFINITY()), 0)) AS \"revenue\", SUM(impressions) AS \"impressions\", SUM(IF(trackable, IF(viewduration >= 0, (viewduration / 1000), 0), 0)) AS \"view_duration\", IF(IS_NAN(ROUND(((SUM(revenue) / 1000) / SUM((postviewconversions + postclickconversions + postviewableconversions))), 3)), 0, COALESCE(NULLIF(ROUND(((SUM(revenue) / 1000) / SUM((postviewconversions + postclickconversions + postviewableconversions))), 3), INFINITY()), 0)) AS \"erpa\" FROM performanceanalyticsoptimised LEFT JOIN labels as tacticlabels ON (tacticlabels._id=tacticid AND tacticlabels.service='tactics') WHERE (ymdh >= timestamp '{date1} 00:00:00 UTC' AT TIME ZONE 'UTC' AND ymdh <= timestamp '{date2} 23:59:59 UTC' AT TIME ZONE 'UTC' AND timestamp_trunc_hour >= DATE_FORMAT(timestamp '{date1} 00:00:00 UTC' AT TIME ZONE 'UTC', '%Y-%m-%d-%H') AND timestamp_trunc_hour <= DATE_FORMAT(timestamp '{date2} 23:59:59 UTC' AT TIME ZONE 'UTC', '%Y-%m-%d-%H') AND (brandid = '58418c68047608ab67033150') AND (IF(conversionpriority IS NULL, 0, conversionpriority) = 0) AND (accountid = '5644792d1681c82e739afff5') AND (IF((postviewconversions + postclickconversions + postviewableconversions) > 0, COALESCE(attributionmode, 0), 0) = 0)) GROUP BY DATE_FORMAT(ymdh AT TIME ZONE 'UTC', '%Y-%m-%d'), tacticlabels.name ORDER BY 1 ASC"
    
    headers = ['Date', 'Tactic', 'Revenue', 'Impressions', 'View-Duration (s)', 'eRPA']

    cur.execute(query)
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=headers)
    return df

def add_columns(df):
    
    df['CPS'] = round(df['Revenue'] / df['View-Duration (s)'], 5)
    df['Avg Duration/User'] = round(df['View-Duration (s)'] / df['Impressions'],1)
    
    df.to_csv('dftest.csv', index=False)

#def avg_dur_conv():
    
    
df = querydata()
add_columns(df)
    
    
    
    
    
    