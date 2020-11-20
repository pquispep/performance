#!/usr/bin/env python
# coding: utf-8

# Se requiere realizar:
# 
# 1. Generar una sábana que nos permita denormalizar la información para futuros análisis.
# 2. Guardar dicha sábana en un archivo de texto.
# 3. Hacer un análisis  que nos indique el volúmen de ventas  que nos permita interactuar con los resultados (OLAP/Pivot en excel).
# 4. Hacer una segmentación de clientes por categoría indicando qué categoría será más probable que compre cada uno de los clientes y generar un archivo de texto con la siguiente estructura:
#    id_customer Int
#    category String
# 
# Requerimientos: 
#       1. Se requiere un (o varios) scripts que realicen la lectura de los archivos y su carga para su análisis.
# 	     Se dará prioridad si se realiza completamente en python (Jupyter Notebook o scripts convencionales).
# 	  2. Se lo más ordenado posible.
# 	  3. No es necesario justificar el método que se empleó en el punto 4 y eres libre de escoger el que sea más
# 	     adecuado a tus conocimientos.
# 	  3. Subir tus códigos a GitHub/Bitbucket

# In[ ]:


#Importamos las librerias y seteamos la ruta de trabajo
import pandas as pd
import os
z=os.getcwd()
os.chdir("/Users/pquispe/Downloads/Pruebas_Performance_Linio")


# In[ ]:


#Importamos la data de catalogo y asignamos las los headers
catalog_df = pd.read_csv("catalog.csv",header=None)
catalog_df.columns=["sku_code","category","timestamp"]
print("catlog ",catalog_df.shape)
catalog_df.head(2)


# In[ ]:


#eliminamos los registros duplicados de catalogo
catalog_unico_df=catalog_df.drop_duplicates()
print("catalog unico ",catalog_unico_df.shape)
catalog_unico_df.head(2)


# In[ ]:


#Importamos la data de customers y asignamos las los headers
customers_df = pd.read_csv("customers.csv",header=None)
customers_df.columns=["id_cliente","nombre","email","timestamp"]
print("customers ",customers_df.shape)
customers_df.head(2)


# In[ ]:


#Importamos la data de ventas y asignamos las los headers
ventas_2019_df = pd.read_csv("ventas_2019.csv",header=None)
ventas_2019_df.columns=["id_venta","id_customer","sku_code","numero_de_orden","precio_unitario","unidades","importe","timestamp"]
print("ventas ",ventas_2019_df.shape)
ventas_2019_df.head(2)


# In[ ]:


#unimos las fuentes para obtener la sabana desnomalizada
union_pre01 = pd.merge(left=ventas_2019_df,right=customers_df, how='left', left_on='id_customer', right_on='id_cliente')
print("union_pre01 ",union_pre01.shape)

union = pd.merge(left=union_pre01,right=catalog_unico_df, how='left', left_on='sku_code', right_on='sku_code')
print("union ",union.shape)


#exportamos el resultado de la union de fuentes
union.to_csv("salida.csv",index=False)

union.head(2)


# In[ ]:


#Generando data data excel
union['timestamp_x']=pd.to_datetime(union['timestamp_x'])
union['anio_v']=union['timestamp_x'].dt.year
union['mes_v']=union['timestamp_x'].dt.month
df1=pd.DataFrame(union.groupby(['category','nombre','anio_v','mes_v','timestamp_x'])[['unidades','importe']].sum())

union.to_csv("salida_para_excel.csv",index=False)

df1.head(10)


# In[ ]:


import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import train_test_split
encoder=LabelEncoder()

union['categoria']=encoder.fit_transform(union.category.values)
union.head(10)

x=union['id_customer']
y=union['categoria']




x = mean_data = np.array(x)
X=x[:,np.newaxis]



while True:
    X_train, X_test, y_train, y_test = train_test_split(X, y)
    mlr=MLPRegressor(solver="lbfgs",alpha=1e-5,hidden_layer_sizes=(3,3), random_state=1)
    mlr.fit(X_train, y_train)
    print(mlr.score(X_train, y_train))
    if mlr.score(X_train,y_train) > 0.45:
        break
 
print('Categoria para ')
print(mlr.predict(np.array(57475).reshape(1, 1)))

