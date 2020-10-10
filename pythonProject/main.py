# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


import pandas as pd
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="./keyBigQuery/testdata-292010-6ea007319786.json"


gcp_project = 'testdata-292010'
bq_datasets = 'datasetsTest'

queryCompletitudAdSchedule= """SELECT col_name, COUNT(1) nulls_count , 76231-count(1) not_nulls_count
 FROM testdata-292010.datasetsTest.adsSchedule  t,
UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t), r'"(\w+)":null')) col_name
GROUP BY col_name;
"""

queryCompletitudAnalytics= """
SELECT col_name, COUNT(1) nulls_count , 908148-count(1) not_nulls_count
 FROM testdata-292010.datasetsTest.analytics  t,
UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t), r'"(\w+)":null')) col_name
GROUP BY col_name;
"""

queryConsistenciaAdsSchedule= """select case when safe.PARSE_TIMESTAMP('%Y-%M-%d %H:%M:%S %Z',start_date) is not null then 'FORMATO CORRECTO' ELSE 'FORMATO INCORRECTO' END METRICA, 
COUNT(1) CANTIDAD 
FROM testdata-292010.datasetsTest.adsSchedule  t
where start_date is not null
GROUP BY case when safe.PARSE_TIMESTAMP('%Y-%M-%d %H:%M:%S %Z',start_date) is not null then 'FORMATO CORRECTO' ELSE 'FORMATO INCORRECTO' END
"""

queryConsistenciaAnalytics= """select case when (SAFE.PARSE_DATE('%Y%m%d',date) ) is not null then 'FORMATO CORRECTO' ELSE 'FORMATO INCORRECTO' END,
COUNT(1)
FROM testdata-292010.datasetsTest.analytics  t
GROUP BY case when (SAFE.PARSE_DATE('%Y%m%d',date) ) is not null then 'FORMATO CORRECTO' ELSE 'FORMATO INCORRECTO' END
"""

queryDuplicadosAdScheduled= """SELECT 
id,
medium,
support,
reference,
coverage,
start_date,
end_date,
format,
inversion,
rating,
channel,
calculated,
date_loaded,
count(1) cantidadVecesRepetido
FROM `testdata-292010.datasetsTest.adsSchedule` 
group by 
id,
medium,
support,
reference,
coverage,
start_date,
end_date,
format,
inversion,
rating,
channel,
calculated,
date_loaded
having count(1)>1
"""

queryDuplicadosAnalytics= """SELECT 
date,
source,
medium,
campaign,
adContent,
clientId,
operatingSystem,
browser,
language,
country,
city,
sessionQualityDim,
newVisits,
pageviews,
bounces,
count(1) cantidadVecesRepetido
FROM `testdata-292010.datasetsTest.analytics` 
group by 
date,
source,
medium,
campaign,
adContent,
clientId,
operatingSystem,
browser,
language,
country,
city,
sessionQualityDim,
newVisits,
pageviews,
bounces
having count(1)>1
"""

def calculateMetric(query, table_destination):
    client = bigquery.Client(project=gcp_project)
    client.delete_table(table_destination, not_found_ok=True)
    job_config = bigquery.QueryJobConfig(destination=table_destination)
    query_job = client.query(query, job_config=job_config)
    query_job.result()
    print("Query results loaded to the table {}".format(table_destination))


if __name__ == '__main__':
    calculateMetric(queryCompletitudAdSchedule, 'testdata-292010.datasetsTest.CompletitudAdSchedule')
    calculateMetric(queryCompletitudAnalytics, 'testdata-292010.datasetsTest.CompletitudAnalytics')
    calculateMetric(queryConsistenciaAdsSchedule, 'testdata-292010.datasetsTest.ConsistenciaAdsSchedule')
    calculateMetric(queryConsistenciaAnalytics, 'testdata-292010.datasetsTest.ConsistenciaAnalytics')
    calculateMetric(queryDuplicadosAdScheduled, 'testdata-292010.datasetsTest.DuplicadosAdScheduled')
    calculateMetric(queryDuplicadosAnalytics, 'testdata-292010.datasetsTest.DuplicadosAnalytics')

