# Test Data Engineer 

##  1) Procese los conjuntos de datos y realice el análisis usando SQL (Bigquery (Deseable), Mysql o Postgresql).

Para este punto utilice Google Cloud Plattform especificamente los servicios de Google Cloud Storage para cargar los dataset y BigQuery para ejecutar las siguientes consultas SQL sobre los datos y asi poder calcular las metricas solicitadas


![GCStorage](https://github.com/javasquez/testDataEngineerResolve/blob/main/CapturasPantalla/GCStorage.png)

![GCBigQuery](https://github.com/javasquez/testDataEngineerResolve/blob/main/CapturasPantalla/GCBigQuery.png)


Completitud en ambos archivos:

```sql
SELECT col_name, COUNT(1) nulls_count , 76231-count(1) not_nulls_count
 FROM testdata-292010.datasetsTest.adsSchedule  t,
UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t), r'"(\w+)":null')) col_name
GROUP BY col_name;
```

```sql
SELECT col_name, COUNT(1) nulls_count , 908148-count(1) not_nulls_count
 FROM testdata-292010.datasetsTest.analytics  t,
UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t), r'"(\w+)":null')) col_name
GROUP BY col_name;
```

Consistencia en ambos archivos:

Asumo que el formato esperado es %Y-%M-%d %H:%M:%S %Z
```sql
select case when safe.PARSE_TIMESTAMP('%Y-%M-%d %H:%M:%S %Z',start_date) is not null then 'FORMATO CORRECTO' ELSE 'FORMATO INCORRECTO' END METRICA, 
COUNT(1) CANTIDAD 
FROM testdata-292010.datasetsTest.adsSchedule  t
where start_date is not null
GROUP BY case when safe.PARSE_TIMESTAMP('%Y-%M-%d %H:%M:%S %Z',start_date) is not null then 'FORMATO CORRECTO' ELSE 'FORMATO INCORRECTO' END

```

Asumo que el formato esperado es %Y%m%d
```sql
select case when (SAFE.PARSE_DATE('%Y%m%d',date) ) is not null then 'FORMATO CORRECTO' ELSE 'FORMATO INCORRECTO' END,
COUNT(1)
FROM testdata-292010.datasetsTest.analytics  t
GROUP BY case when (SAFE.PARSE_DATE('%Y%m%d',date) ) is not null then 'FORMATO CORRECTO' ELSE 'FORMATO INCORRECTO' END
```

Registros duplicados en ambos archivos:

En la ultima columna se muestra la cantidad de veces que se repite cada fila
```sql
SELECT 
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
```

```sql
SELECT 
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
```

## Genere una nueva tabla SQL con el resultado del análisis para cada conjunto usando como lenguaje de programación Python o NodeJS.

En esta parte hice un una app en python que se conecta al bigquery utilizando la key que esta en el repositorio git, despues ejecuta las query desarrolladas previamente y almacena los resultados en tablas en BigQuery

Tambien considere hacer estos analisis usando pandas pero decidi reaprovechar el codigo anterior desarrollado

El codigo de la app esta en la carpeta **pythonProject** del repositorio


## Implementar Docker en el proyecto.

Para esta parte cree un docker con Airflow con el fin de crear una pipeline que ejecutara el analisis de las metricas solicitadas. 


El archivo **docker-composer.yml** con todas las dependencias y modulos  y el **DAG** estan dentro de la carpeta docker-airflow

Para ejecutar esto primero debemos clonar el proyecto a nuestra maquina, despues debemos asegurarnos de tener instalado docker en el equipo
despues debemos ir a la carpeta **docker-airflow** mediante la consola y ejecutar ```docker-composer up```

Una vez esto se haya ejecutado correctamente ejecutado deberiamos poder acceder airflow a http://localhost:8080 y ver y ejecutar la pipeline

![Airflow1](https://github.com/javasquez/testDataEngineerResolve/blob/main/CapturasPantalla/Airflow1.png)

![Airflow2](https://github.com/javasquez/testDataEngineerResolve/blob/main/CapturasPantalla/Airflow2.png)







