# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 16:46:43 2020

@author: O46743 - Piotr Walędzik
"""

from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
import httplib2
import pandas as pd
import numpy as np
import time
import sys
import os

start_time = time.time()

 
#Create service credentials
json_path = os.path.dirname(os.path.realpath(__file__))+'\client_secrets.json'
credentials = ServiceAccountCredentials.from_json_keyfile_name(json_path, ['https://www.googleapis.com/auth/analytics.readonly'])
  
#Create a service object
http = credentials.authorize(httplib2.Http())
service = build('analytics', 'v4', http=http, discoveryServiceUrl=('https://analyticsreporting.googleapis.com/$discovery/rest'))
##############################################################################

# For error hanlding
period_A = False
period_B = False

while (period_A == False) or (period_B == False):
    try:
        # Period A
        startDate_A = input("\nStart date for period A [YYYY-MM-DD]:\n")
        endDate_A = input("End date for period A [YYYY-MM-DD]:\n")
        if startDate_A[4] == '-' and startDate_A[7] == '-' and endDate_A[4] == '-' and endDate_A[7] == '-':
            period_A = True
        else:
            raise ValueError('Wrong date format!')
        
        # Period B
        startDate_B = input("\nStart date for period B [YYYY-MM-DD]:\n")
        endDate_B = input("End date for period B [YYYY-MM-DD]:\n")
        
        if startDate_B[4] == '-' and startDate_B[7] == '-' and endDate_B[4] == '-' and endDate_B[7] == '-':
            period_B = True
        else:
            raise ValueError('Wrong date format!')
            
    except Exception:
        print('\nWARNING! Make sure that both dates are in this format: YYYY-MM-DD')

##############################################################################

# API reponse PERIOD A
print('\nDownloading Period A data...')

# PROGRESS
sys.stdout.write("\r 5%")

try:
    response_A = service.reports().batchGet(
        body = {
            "reportRequests": [
                {
                    "viewId": "183866975", # Add View ID from GA
                    "pageSize": "100000", # Return API's maximum possible number of rows (max=100,000)
                    "filtersExpression": "ga:eventCategory==search;ga:eventAction==click", # Filter on search and click only
                    "dateRanges": [
                        {
                            "startDate": startDate_A,
                            "endDate": endDate_A
                            }
                        ],
                    "dimensions": [
                        {
                            "name": "ga:dimension7" # "dimension7" = "Search term that caused a clicked"
                            },
                        {
                            "name": "ga:eventLabel"
                            }
                        ],
                    "metrics": [
                        {
                            "expression": "ga:totalEvents"
                            }
                        ]
                    }
                ]
            }
        ).execute()
    
except Exception as e:
    print(e)
    sys.exit()


# PROGRESS
sys.stdout.write("\r 100%")
print('\nPeriod A data has been downloaded.')
##############################################################################

# API reponse PERIOD B
print('\nDownloading Period B data...')

# PROGRESS
sys.stdout.write("\r 5%")

try:
    response_B = service.reports().batchGet(
        body = {
            "reportRequests": [
                {
                    "viewId": "183866975", # Add View ID from GA
                    "pageSize": "100000", # Return API's maximum possible number of rows (max=100,000)
                    "filtersExpression": "ga:eventCategory==search;ga:eventAction==click", # Filter on search and click only
                    "dateRanges": [
                        {
                            "startDate": startDate_B,
                            "endDate": endDate_B
                            }
                        ],
                    "dimensions": [
                        {
                            "name": "ga:dimension7" # "dimension7" = "Search term that caused a clicked"
                            },
                        {
                            "name": "ga:eventLabel"
                            }
                        ],
                    "metrics": [
                        {
                            "expression": "ga:totalEvents"
                            }
                        ]
                    }
                ]
            }
        ).execute()
    
except Exception as e:
    print(e)
    sys.exit()


# PROGRESS
sys.stdout.write("\r 100%")
print('\nPeriod B data has been downloaded.')
##############################################################################

print('\nPeriod A and B data extraction has started...')

# Period A data extraction
# Create empty variables/placeholders
data = []
df_row = pd.DataFrame()
df_report_A = pd.DataFrame(columns=["Search term that caused a clicked", "Event Label", "Total Events"])

progress = 100
i = 0

#Extract Data
for report in response_A.get('reports', []):
  
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])
    
    progress = 100/len(rows)
  
    for row in rows:
  
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])
        value = int(dateRangeValues[0]['values'][0])
        
        data = [(dimensions[0], dimensions[1], value)]
        df_row = pd.DataFrame(data, columns=["Search term that caused a clicked", "Event Label", "Total Events"])
        df_report_A = df_report_A.append(df_row)
        
        i = round(i + progress,2)
        sys.stdout.write(f"\r {i}%")
    
sys.stdout.write(f"\r 100.00%")
print('\nPeriod A data extraction has finished.\n')

# Sorting
df_report_A = df_report_A.sort_values(by=["Total Events", "Search term that caused a clicked"],ascending=False)

# Index setup
df_report_A['Index'] = [i for i in range(len(df_report_A))]
df_report_A = df_report_A.set_index("Index")
##############################################################################

# Period B data extraction
# Create empty variables/placeholders
data = []
df_row = pd.DataFrame()
df_report_B = pd.DataFrame(columns=["Search term that caused a clicked", "Event Label", "Total Events"])

progress = 100
i = 0

#Extract Data
for report in response_B.get('reports', []):
  
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])
    
    progress = 100/len(rows)
  
    for row in rows:
  
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])
        value = int(dateRangeValues[0]['values'][0])
        
        data = [(dimensions[0], dimensions[1], value)]
        df_row = pd.DataFrame(data, columns=["Search term that caused a clicked", "Event Label", "Total Events"])
        df_report_B = df_report_B.append(df_row)
        
        i = round(i + progress,2)
        sys.stdout.write(f"\r {i}%")

sys.stdout.write(f"\r 100.00%")
print('\nPeriod B data extraction has finished.\n')

# Sorting
df_report_B = df_report_B.sort_values(by=["Total Events", "Search term that caused a clicked"],ascending=False)

# Index setup
df_report_B['Index'] = [i for i in range(len(df_report_B))]
df_report_B = df_report_B.set_index("Index")

# Data Frames structuring - final touches
df_report_A_short = df_report_A[['Search term that caused a clicked','Total Events']].copy()
df_report_A_short['Search term that caused a clicked'] = df_report_A_short['Search term that caused a clicked'].str.lower()
df_report_A_short = df_report_A_short.groupby(['Search term that caused a clicked'], as_index=False)[['Total Events']].agg('sum')
df_report_A_short = df_report_A_short.sort_values(by=["Total Events", "Search term that caused a clicked"],ascending=False)


df_report_B_short = df_report_B[['Search term that caused a clicked','Total Events']].copy()
df_report_B_short['Search term that caused a clicked'] = df_report_B_short['Search term that caused a clicked'].str.lower()
df_report_B_short = df_report_B_short.groupby(['Search term that caused a clicked'], as_index=False)[['Total Events']].agg('sum')
df_report_B_short = df_report_B_short.sort_values(by=["Total Events", "Search term that caused a clicked"],ascending=False)
   
print('\nPeriod A and B data extraction has finished...')
##############################################################################

# NLP
# Remember to download the model by executing in console:
# python -m spacy download en_core_web_lg
print('\nLoading word2vec matrix...')
sys.stdout.write("\r 10%")
import spacy
nlp = spacy.load("en_core_web_lg")
sys.stdout.write("\r 100%")
print('\nWord2vec matrix loaded successfully.')

lower_boundary = float(input("\nSimilarity lower boundary [0.0 - 1.0]. Recommended 0.65:\n"))
upper_boundary = float(input("\nSimilarity upper boundary [0.0 - 1.0]. Recommended 0.95:\n"))

##############################################################################

# Period A - Unique search values
df_report_searches = df_report_A.iloc[:,0].str.lower().unique()

# phrases = list(nlp.pipe(df_report_searches))
# Thought the below may improve the performance - it does but very little
phrases = list(nlp.pipe(df_report_searches, disable=['parser', 'tagger', 'ner']))

# Create empty variables/placeholders
data = []
df_similarity_A = pd.DataFrame()

print('\nPeriod A similarity scores are being calculated [It may take a couple of minutes] ...')
progress = 100/len(phrases)
i = 0

# Period A - Search phrases similarity comparison             
for idx, phrase1 in enumerate(phrases):
    data = [(phrase1.text, phrase2.text, phrase1.similarity(phrase2)) for phrase2 in phrases[idx+1:]
            if phrase1.vector_norm and phrase2.vector_norm and phrase1.similarity(phrase2)>lower_boundary 
            and phrase1.similarity(phrase2)<upper_boundary]
    
    df_similarity_A = df_similarity_A.append(data)
    i = round(i + progress,2)
    sys.stdout.write(f"\r {i}%")
    
sys.stdout.write(f"\r 100.00%")
    
df_similarity_A.rename(columns={0:'Search_Phrase1', 1:'Search_Phrase2', 2:'Similarity_Score'}, inplace = True)


# Index setup
df_similarity_A['Index'] = [i for i in range(len(df_similarity_A))]
df_similarity_A = df_similarity_A.set_index("Index")


# Similarities frequency collectively
df_similarity_A = df_similarity_A.drop(columns=['Similarity_Score'], axis=1)


df_similarity_A['Similar Terms Frequency Collectively'] = df_similarity_A.\
    Search_Phrase2.map(df_report_A_short.\
    set_index('Search term that caused a clicked')['Total Events'])


df_similarity_A = df_similarity_A.groupby(['Search_Phrase1'], as_index=False).\
    agg({'Search_Phrase2': ', '.join, 'Similar Terms Frequency Collectively': 'sum'})
    
##############################################################################

# Period B - Unique search values
df_report_searches = df_report_B.iloc[:,0].str.lower().unique()

phrases = list(nlp.pipe(df_report_searches))
# Thought the below may improve the performance - it does but very little
# phrases = list(nlp.pipe(df_report_searches, disable=['parser', 'tagger', 'ner']))

# Create empty variables/placeholders
data = []
#df_row = pd.DataFrame()
# df_similarity_B = pd.DataFrame(columns=["Search_Phrase1", "Search_Phrase2", "Similarity_Score"])
df_similarity_B = pd.DataFrame()

print('\nPeriod B similarity scores are being calculated [It may take a couple of minutes] ...')
progress = 100/len(phrases)
i = 0

# Period B - Search phrases similarity comparison             
for idx, phrase1 in enumerate(phrases):
    data = [(phrase1.text, phrase2.text, phrase1.similarity(phrase2)) for phrase2 in phrases[idx+1:]
            if phrase1.vector_norm and phrase2.vector_norm and phrase1.similarity(phrase2)>lower_boundary 
            and phrase1.similarity(phrase2)<upper_boundary]
    
    df_similarity_B = df_similarity_B.append(data)
    i = round(i + progress,2)
    sys.stdout.write(f"\r {i}%")
    
sys.stdout.write(f"\r 100.00%")

df_similarity_B.rename(columns={0:'Search_Phrase1', 1:'Search_Phrase2', 2:'Similarity_Score'}, inplace = True)


# Index setup
df_similarity_B['Index'] = [i for i in range(len(df_similarity_B))]
df_similarity_B = df_similarity_B.set_index("Index")


# Similarities frequency collectively
df_similarity_B = df_similarity_B.drop(columns=['Similarity_Score'], axis=1)


df_similarity_B['Similar Terms Frequency Collectively'] = df_similarity_B.\
    Search_Phrase2.map(df_report_B_short.\
    set_index('Search term that caused a clicked')['Total Events'])
        

df_similarity_B = df_similarity_B.groupby(['Search_Phrase1'], as_index=False).\
    agg({'Search_Phrase2': ', '.join, 'Similar Terms Frequency Collectively': 'sum'})
    
##############################################################################

# FINAL SUMMARY REPORT GENERATION

# Max A Period Terms to go into the report
max_top = len(df_report_A_short)
print("\n\nSUMMARY REPORT")
max_top = int(input(f"How many top trending search terms do you need? [Number must be <= {max_top}]:\n"))

# Make sure the number is not higher than Period A report length
while max_top>len(df_report_A_short):
    max_top = int(input(f"HEY! Number must be lower than {len(df_report_A_short)}:\n"))

# Columns Renaming
df_report_A_short.rename(columns={'Search term that caused a clicked':'Time Period Terms',\
                                  'Total Events':'Frequency'}, inplace=True)
df_report_B_short.rename(columns={'Search term that caused a clicked':'Time Period Terms',\
                                  'Total Events':'Frequency'}, inplace=True)
df_similarity_A.rename(columns={'Search_Phrase1':'Time Period Terms',\
                                'Search_Phrase2':'Similar Terms'}, inplace=True)
df_similarity_B.rename(columns={'Search_Phrase1':'Time Period Terms',\
                                'Search_Phrase2':'Similar Terms'}, inplace=True)

# Data preparation - Period A and Period B    
df_summary_A = pd.merge(df_report_A_short, df_similarity_A, on='Time Period Terms', how='left')
df_summary_B = pd.merge(df_report_B_short, df_similarity_B, on='Time Period Terms', how='left')

# Columns preparation
df_summary_A['Time Period A Terms'] = df_summary_A['Time Period Terms']
df_summary_B['Time Period B Terms'] = df_summary_B['Time Period Terms']

# Period A report narrow down to requested number of rows
df_summary_A = df_summary_A.iloc[:max_top,:]

# A + B Periods summary preparation
df_summary_AB = pd.merge(df_summary_A, df_summary_B, on='Time Period Terms', how='outer')
df_summary_AB = df_summary_AB.drop(columns=['Time Period Terms'], axis=1)

# Renaming columns
df_summary_AB.rename(columns={'Frequency_x':'Frequency A',\
                                  'Similar Terms_x':'Similar Terms A',\
                                      'Similar Terms Frequency Collectively_x':'Similar Terms A Frequency Collectively',\
                                          'Frequency_y':'Frequency B',\
                                              'Similar Terms_y':'Similar Terms B',\
                                                  'Similar Terms Frequency Collectively_y':'Similar Terms B Frequency Collectively'}, inplace=True)

# Proper order of columns
df_summary_AB = df_summary_AB[['Time Period A Terms', 'Frequency A', 'Similar Terms A', 'Similar Terms A Frequency Collectively',\
                               'Time Period B Terms', 'Frequency B', 'Similar Terms B', 'Similar Terms B Frequency Collectively']]

# Cleaning Period B - unwanted rows
df_summary_AB.drop(df_summary_AB[(df_summary_AB['Time Period A Terms'].isnull())\
                                 & (df_summary_AB['Frequency A'].isnull())\
                                     & (df_summary_AB['Frequency B'] < 5)].index, inplace = True)

# 'Trend' column generation
conditions = [
    (df_summary_AB['Frequency A'].isnull() == False) & (df_summary_AB['Frequency B'].isnull() == False) & ((df_summary_AB['Frequency B']/df_summary_AB['Frequency A'])-1<-0.05),
    (df_summary_AB['Frequency A'].isnull() == False) & (df_summary_AB['Frequency B'].isnull() == False) & ((df_summary_AB['Frequency B']/df_summary_AB['Frequency A'])-1>=-0.05) & ((df_summary_AB['Frequency B']/df_summary_AB['Frequency A'])-1<=0.05),
    (df_summary_AB['Frequency A'].isnull() == False) & (df_summary_AB['Frequency B'].isnull() == False) & ((df_summary_AB['Frequency B']/df_summary_AB['Frequency A'])-1>0.05),
    (df_summary_AB['Time Period A Terms'].isnull() == False) & (df_summary_AB['Time Period B Terms'].isnull()),
    (df_summary_AB['Time Period B Terms'].isnull() == False) & (df_summary_AB['Time Period A Terms'].isnull())]

choices = ['Decreasing', 'Constant', 'Increasing', 'Forgotten', 'NEW']

df_summary_AB['Trend'] = np.select(conditions, choices, default = 'Unspecified')
    
# Empty values cleaning
df_summary_AB.fillna('', inplace=True)

# Final renaming
dates_A = startDate_A+'/'+endDate_A
dates_B = startDate_B+'/'+endDate_B

df_summary_AB.rename(columns={'Time Period A Terms':'Time Period A Terms: '+dates_A,\
                              'Time Period B Terms': 'Time Period B Terms: '+dates_B}, inplace=True)
    
# PRINT OUT TO CSV
timestr = time.strftime("%Y%m%d-%H%M%S")
df_summary_AB.to_csv('SUMMARY REPORT_'+timestr+'.csv', index=False, sep = '|')



print("--- %s seconds ---" % (time.time() - start_time))
