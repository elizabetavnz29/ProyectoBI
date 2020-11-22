import codecs
import decimal
import json
import os
import re
from bs4 import BeautifulSoup
import ndjson
from plotnine.aes import reorder
from pymongo import MongoClient
import pprint
from decimal import Decimal
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from plotnine import *
import statistics as stat
import numpy as np

#CLASE PARA CONVERTIR EL PRECIO A DECIMAL EN EL JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)


#CONEXIÓN A LA BASE DE DATOS MONGODB

client = MongoClient('mongodb://localhost:27017')
#print(client)
db = client['prueba']
opiniones = db.opiniones
print(opiniones)

#PATH ENTRADA RAW - SALIDA JSON
currentPath = os.path.dirname(__file__)
rawPath = os.path.join(currentPath, '../data/raw.txt')


productList = []
#acceptedFields
agregar = ('brand', 'productgroup', 'title', 'manufacturer','binding')

#FUNCIÓN PARA AGREGAR LOS VALORES AL DICC DE DATOS

def agregando():
    try:
        key, value = word.split('->')

        if key in agregar:
            tempDictionary[key] = value
    except:
        key, value1, value2 = word.split('->')
        # if key in acceptedFields:
        tempDictionary[key] = value1 + value2

#EXTRACT
#with codecs.open(rawPath, 'r', encoding='utf-8', errors='ignore') as rawFile:
#      lines = rawFile.readlines()
#      tempDictionary = {}
#      total = 0
#CLEAN
#      for line in lines:
#          if 'BREAK-REVIEWED' in line:
#             tempDictionary = {}
#          elif re.search('[0-9]+\.[0-9]+', line):
#
#              tempDictionary['price'] = float(re.findall(
#                  '[0-9]+\.[0-9]+', line)[0])
#          else:
#            #Cleaning
#              cleanLine = line.split()
#              cleanLine = " ".join(cleanLine)
#              cleanLine = cleanLine.replace('<->', ' - ')
#              cleanLine = cleanLine.replace('ice>Link Plus -> ', '')
#              cleanLine = cleanLine.replace('\"', '')
#              cleanLine = cleanLine.replace('.', '')
#              cleanLine = cleanLine.replace("'", "")
#              soup = BeautifulSoup(cleanLine, "html.parser")
#              cleanLine = soup.get_text()
#              # if re.search('[0-9]+["] ', cleanLine):
#              # cleanLine = cleanLine.replace('"','')
#              cleanLine = cleanLine.lower()
#            #Find KeyValues
#              words = re.findall('\S+->.*?(?= \S+->|$)', cleanLine)
#
#              #Get Data
#              for word in words:
#
#                  agregando();
#
#              if(set(agregar).issubset(tempDictionary)):
#                  productList.append(tempDictionary.copy())

#GUARDAR LOS DATOS LIMPIOS EN UN JSON
    with open('backup.json','w') as f:
     ndjson.dump(productList, f,sort_keys=True, cls=DecimalEncoder)

#INSERTAR UN REGISTRO
#results = opiniones.insert_one(productList[6])
#RECORRIENDO
#for object_id in results.inserted_ids:
#    print("Se insertó. El id es: " + str(object_id))

#results = opiniones.insert_many(productList)
#RECORRIENDO
#for object_id in results.inserted_ids:
#     print("Se insertó. El id es: " + str(object_id))

#UPDATE -> AGREGAR CATEGORÍA A CADA PRODUCT GROUP

# opiniones.update_many({
#          'productgroup' : 'software'
#       }, {
#          '$set': {
#              'group' : 16
#          }
#      })

#CONSULTA PARA OBTENER GRUPO, PRECIO Y BRAND.
opinion = opiniones.find({},{'group':1,'price':1,'brand':1})
# db.opiniones.aggregate([
#      {
#          '$group': {
#              '_id' : '$group'
#          }
#      }
#  ])
#
#IMPRESIÓN DEL FIND
# for productList in opiniones:
#      pprint.pprint(productList)

#
#CREACIÓN DEL DATAFRAME CON LOS RESULTADOS DEL FIND A LA BD
df = pd.DataFrame(list(opinion))
del df['_id']

# ENCONTRAMOS CUÁL ES EL GRUPO MAYOR
# ENCONTRAMOS LA MARCA MÁS VENDIDA Y LA CANTIDAD VENDIDA
print("TABLA GRUPOS")
lis = df['group'].unique()
grupo = (df['group'].value_counts(ascending=False,normalize=True))
datos=pd.DataFrame(grupo,columns=['group'])
dat = pd.DataFrame(grupo,columns=['group']).iloc[0]
namesgrupos = grupo.index.tolist()
a=dat.name
# colores = ["#EE6055","#60D394","#AAF683","#FFD97D","#FF9B85"]
desfase = (0.1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
#CREAMOS UN GRÁFICO DE PIE PARA MOSTRAR LOS GRUPOS CON MÁS VENTAS
plt.pie(grupo, labels=namesgrupos, autopct="%1.1f%%", explode=desfase, shadow=True, startangle=90.0)
plt.legend()
plt.title("CATEGORÍA DE GRUPOS ", color="#EE6055")
plt.axis("equal")
plt.show()
print(datos)
#REALIZAMOS UNA CONSULTA A LA BD DE ÚNICAMENTE EL GRUPO MÁS VENDIDO
hola = opiniones.find({'group': a },{'group':1,'price':1,'brand':1})
#OBTENEMOS LA FRECUENCIA DE VENTAS AGRUPADO POR BRAND
freq_by_brand = (df
                     .groupby("brand")
                     .agg(frequency=("brand", "count"), precio=('price',"sum"))

                  )
freq_by_brand.sort_values(by=['frequency'],inplace=True, ascending=False)

maxbrand=freq_by_brand.iloc[0]
namebrand=maxbrand.name
cantidadbrand=maxbrand.frequency
#GUARDAMOS SOLO LOS VALORES
val=pd.DataFrame(freq_by_brand,columns=['frequency']).values
otro =  pd.DataFrame(freq_by_brand,columns=['frequency'])
print("EL PRODUCT GROUP CON MÁS VENTAS ES: " + str(a))
print("En total la brand más vendida es: " + str(namebrand) + " con una cantidad de ventas de: " + str(cantidadbrand) )
media = val.mean()
print("La media es: " + str(media))
varianza = val.var()
print("La varianza es: " +str(varianza))
desviacion = val.std()
print("La desviación estandar es: " + str(desviacion))


print("\t\n DATOS POR GRUPO\t\n ")
vecvarianza=[]
vecvarianzaprecio=[]
vecmedia=[]
vecmediaprecio=[]
vecdesv=[]
vecdesvprecio=[]
#VAMOS A MOSTRAR TODOS LOS GRUPOS
numgrupo= 1;
b=0
while numgrupo < 23:
    print("\nIMPRIMIENDO GRUPO : "+ str(numgrupo))
    consulta = opiniones.find({'group': numgrupo},{'brand':1,'price':1})
    # for productList in consulta:
    #     pprint.pprint(productList)
    df = pd.DataFrame(list(consulta))
    freq_by_brand = (df
                     .groupby('brand')
                     .agg(frequency=('brand', "count"), precio=('price',"sum"))
                     )
    freq_by_brand.sort_values(by=['frequency'], inplace=True, ascending=False)
    print(freq_by_brand)
    maxbrand = freq_by_brand.iloc[0]
    namebrand = maxbrand.name
    cantidadbrand = maxbrand.frequency
    # GUARDAMOS SOLO LOS VALORES
    val = pd.DataFrame(freq_by_brand, columns=['frequency']).values
    otro = pd.DataFrame(freq_by_brand, columns=['frequency'])
    valprecio = pd.DataFrame(freq_by_brand, columns=['precio']).values
    brands=[]
    for n in otro.head().itertuples():
         if n.frequency == cantidadbrand:
           nom=n.Index
           brands.append(nom)
    print("Mayor cantidad de ventas: " + str(
        cantidadbrand) + ", pertenecientes a la brand: " + str(brands))
    media = val.mean()
    vecmedia.append(media)
    mediap=valprecio.mean()
    vecmediaprecio.append(mediap)
#    print("La media de la frequency es: " + str(media))
    print("La media del precio es: " + str(mediap))
    varianza = val.var()
    vecvarianza.append(varianza)
    varianzap=valprecio.var()
    vecvarianzaprecio.append(varianzap)
#    print("La varianza de la frequency es: " + str(varianza))
    print("La varianza del precio es: " + str(varianzap))
    desviacion = val.std()
    vecdesv.append(desviacion)
    desvp=valprecio.std()
    vecdesvprecio.append(desvp)
#    print("La desviación estandar de la frequency es: " + str(desviacion))
    print("La desviación estandar del precio es: " + str(desvp))
    numgrupo=numgrupo+1
    if(numgrupo == 9):
        numgrupo = 10

# nombres=['1',' 2','Grupo 3','Grupo 4','Grupo 5','Grupo 6','Grupo 7','Grupo 8','Grupo 10','Grupo 11','Grupo 12','Grupo 13','Grupo 14', 'Grupo 15','Grupo 16','Grupo 17','Grupo 18','Grupo 19','Grupo 20', 'Grupo 21', 'Grupo 22']
nombres=['1',' 2','3','4','5','6','7','8','10','11','12','13','14','15','16','17','18','19','20','21','22']

x = np.arange(len(nombres))
width = 0.5
fig, ax = plt.subplots()
rects1 = ax.bar(x - width/3, vecmediaprecio, width, label='Media')
rects2 = ax.bar(x + width/3, vecvarianzaprecio, width, label='Varianza')
rects3 = ax.bar(x + width/3, vecdesvprecio, width, label='Desviación Estandar')

#Añadimos las etiquetas de identificacion de valores en el grafico
ax.set_ylabel('Grupo')
ax.set_title('Estadísticas por grupo')
ax.set_xticks(x)
ax.set_xticklabels(nombres)
#Añadimos un legen() esto permite mmostrar con colores a que pertence cada valor.
ax.legend()
#Añadimos las etiquetas de identificacion de valores en el grafico
ax.set_ylabel('Grupo')
ax.set_title('Estadísticas por grupo')
ax.set_xticks(x)
ax.set_xticklabels(nombres)
#Añadimos un legen() esto permite mmostrar con colores a que pertence cada valor.
ax.legend()
plt.show()







#print("El valor máximo es " + str(freq_by_brand.max()), df.brand)
#g = (ggplot(freq_by_brand, aes(x="brand", y="frequency")) + geom_bar(stat='identity'))

# # plt.matshow(df.corr())
#
# #
