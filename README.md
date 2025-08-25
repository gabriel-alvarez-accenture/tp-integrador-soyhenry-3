# tp-integrador-soyhenry-3

Caso dataset: se carga el csv al bucket S3, y mediante una step function (usa 2 jobs) se hace el proceso de transformacion y carga hacia la base de datos en AWS RDS.


Caso api: se utiliza airbyte cloud para la extraccion de los datos desde una api publica, se carga el json en el bucket S3, y luego mediante una step function/job se realiza el proceso de transformacion y carga en la base de datos.

Ambos casos son orquestados mediante DAG's en airflow, pero el procesamiento se realiza en la nube (AWS y Airbyte)
