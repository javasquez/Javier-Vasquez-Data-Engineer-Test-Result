from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="./dags/keyBigQuery/testdata-292010-6ea007319786.json"


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




args= {
    'owner' : 'javier_vasquez',
    'start_date' : days_ago(1)
}
dag = DAG(dag_id = 'dag_testDataEngineer', default_args = args, schedule_interval = None)

def calculateMetric(**kwargs):
    client = bigquery.Client(project=gcp_project)
    client.delete_table(kwargs['key2'], not_found_ok=True)
    job_config = bigquery.QueryJobConfig(destination=kwargs['key2'])
    query_job = client.query(kwargs['key1'], job_config=job_config)
    query_job.result()
    print("Query results loaded to the table {}".format(kwargs['key2']))


with dag:
    medir_completitudAdsSchedule = PythonOperator(
        task_id = 'completitudAdsSchedule',
        python_callable = calculateMetric,
        op_kwargs = {'key1': queryCompletitudAdSchedule, 'key2' :'testdata-292010.datasetsTest.CompletitudAdSchedule'},
        provide_context= True
        
    )
    
    medir_completitudAnalytics = PythonOperator(
        task_id = 'completitudAnalytics',
        python_callable = calculateMetric,
        op_kwargs = {'key1': queryCompletitudAnalytics, 'key2' :'testdata-292010.datasetsTest.CompletitudAnalytics'},
        provide_context= True
        
    )
    
    medir_consistenciaAdsSchedule = PythonOperator(
        task_id = 'consistenciaAdsSchedule',
        python_callable = calculateMetric,
        op_kwargs = {'key1': queryConsistenciaAdsSchedule, 'key2' :'testdata-292010.datasetsTest.ConsistenciaAdSchedule'},
        provide_context= True
        
    )
    
    medir_consistenciaAnalytics = PythonOperator(
        task_id = 'consistenciaAnalytics',
        python_callable = calculateMetric,
        op_kwargs = {'key1': queryConsistenciaAnalytics, 'key2' :'testdata-292010.datasetsTest.ConsistenciaAnalytics'},
        provide_context= True
        
    )
    
    medir_duplicadosAdsSchedule = PythonOperator(
        task_id = 'duplicadosAdsSchedule',
        python_callable = calculateMetric,
        op_kwargs = {'key1': queryDuplicadosAdScheduled, 'key2' :'testdata-292010.datasetsTest.DuplicadosAdScheduled'},
        provide_context= True
        
    )
    
    medir_duplicadosAnalytics = PythonOperator(
        task_id = 'duplicadosAnalytics',
        python_callable = calculateMetric,
        op_kwargs = {'key1': queryDuplicadosAdScheduled, 'key2' :'testdata-292010.datasetsTest.DuplicadosAdScheduled'},
        provide_context= True
        
    )
    medir_duplicadosAdsSchedule >> medir_duplicadosAnalytics
    medir_consistenciaAnalytics >> medir_duplicadosAdsSchedule
    medir_consistenciaAdsSchedule >> medir_consistenciaAnalytics
    medir_completitudAnalytics >> medir_consistenciaAdsSchedule
    medir_completitudAdsSchedule >> medir_completitudAnalytics 
    
    
    
    
    
    
 