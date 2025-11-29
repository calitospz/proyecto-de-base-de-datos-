#!/usr/bin/env python
# coding: utf-8

# ## Notebook 1
# 
# New notebook

# In[3]:


get_ipython().system('pip install boto3')


# In[2]:


import pandas as pd
from io import BytesIO
import boto3
from pyspark.sql import SparkSession

# Configura tus credenciales y bucket
aws_access_key_id = ''
aws_secret_access_key = ''

region_name = 'us-east-2'  # por ejemplo 'us-east-1'
bucket_name = 'prueba-tcs'


# In[6]:


import pandas as pd
from io import BytesIO
import boto3
from pyspark.sql import SparkSession

# Configura tus credenciales y bucket
aws_access_key_id = 'AKIA6DPVE3I23KPFUR5T'
aws_secret_access_key = '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'

region_name = 'us-east-2'  # por ejemplo 'us-east-1'
bucket_name = 'prueba-tcs'


# In[7]:


file_key = 'c.csatalogo_agenciasv'

# Conexión a S3
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

# Obtener archivo desde S3
obj = s3.get_object(Bucket=bucket_name, Key=file_key)

# Leer el archivo CSV en un DataFrame de pandas
df_agencias = pd.read_csv(BytesIO(obj['Body'].read()))
print(df_agencias.head(10))


# In[8]:


file_key = 'catalogo_tipo_transaccion.csv'

# Conexión a S3
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

# Obtener archivo desde S3
obj = s3.get_object(Bucket=bucket_name, Key=file_key)

# Leer el archivo CSV en un DataFrame de pandas
df_tipo_trx = pd.read_csv(BytesIO(obj['Body'].read()))
print(df_tipo_trx.head(10))


# In[9]:


file_key = 'transacciones.csv'

# Conexión a S3
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

# Obtener archivo desde S3
obj = s3.get_object(Bucket=bucket_name, Key=file_key)

# Leer el archivo CSV en un DataFrame de pandas
df_trx = pd.read_csv(BytesIO(obj['Body'].read()))
print(df_trx.head(10))


# In[10]:


df_spark_agencias = spark.createDataFrame(df_agencias)
df_spark_tipo_trx = spark.createDataFrame(df_tipo_trx)
df_spark_trx = spark.createDataFrame(df_trx)


# In[11]:


df_spark_trx .write.mode("overwrite").saveAsTable("transacciones_full")


# In[22]:


df_spark_agencias.show()
df_spark_tipo_trx.show()
df_spark_trx.show()


# In[23]:


from pyspark.sql.functions import broadcast


# In[24]:


# 1. Quitar 'ciudad' de agencias
df_agencias = df_spark_agencias.drop("ciudad")
df_agencias.show()


# In[25]:


# 2. Renombrar columnas para evitar conflictos
df_spark_tipo_trx = df_spark_tipo_trx.withColumnRenamed("descripcion", "tipo_trx_desc")
df_spark_tipo_trx.show()


# In[26]:


# Join entre transacciones y agencias por agencia_id
df_step1 = df_spark_trx.join(
    broadcast(df_agencias),
    on="agencia_id",
    how="left"
)
df_step1.show()


# In[27]:


# Join entre transacciones y tipo transaccion  por tipo_transaccion_id
df_step2 = df_step1.join(
    broadcast(df_spark_tipo_trx),
    on="tipo_transaccion_id",
    how="left"
)
df_step2.show()


# In[28]:


df_limpio = df_step2.drop("tipo_transaccion_id",  "agencia_id")
df_limpio.show()


# In[29]:


from pyspark.sql.functions import to_date, col, count

df_con_fecha = df_limpio.withColumn("fecha", to_date(col("fecha_hora")))
df_con_fecha.show()


# In[30]:


from pyspark.sql.functions import count, sum, col

df_resumen = (
    df_con_fecha
    .groupBy(
        "fecha",
        "nombre_agencia",
        "latitud",
        "longitud",
        "tipo_trx_desc",
        "canal_id"
    )
    .agg(
        count("transaccion_id").alias("cantidad_transacciones"),
        sum("monto").alias("total_monto")
    )
    .orderBy("fecha", "nombre_agencia")
)

df_resumen.show(truncate=False)


# In[1]:


df = spark.sql("SELECT * FROM crpazminoz.transaccionalidad LIMIT 1000")
display(df)

