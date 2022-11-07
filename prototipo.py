from http import client
import paho.mqtt.client as mqtt
import pymysql
import datetime

#Conexion con la base de datos
class DataBase:
    def insertDatos(fal,temperatura,humedad,co,amoniaco,luminosidad):
        
        selfconnection= pymysql.connect(host="localhost",user="root",password="",db="pollos")
        selfcursor=selfconnection.cursor()
        sql="INSERT INTO datos(temperatura,humedad,co,amoniaco,luminosidad) VALUES('{}','{}',{},'{}','{}')".format(temperatura,humedad,co,amoniaco,luminosidad)
        try:
            selfcursor.execute(sql)
            selfconnection.commit()
            print("Se ha insertado correctamente")
        except Exception as e:
            raise

#Enviar datos a la base de datos
database=DataBase()

#Conexion con MQTT
def on_connect(client,userdata,flags,rc):
      print('connectado(%s)'% client._client_id)
      client.subscribe(topic='datos/#')

def on_message(client,userdata,message):
      
      print('-----------------')
      print('Topico: %s'% message.topic)
      print('Mensaje: %s'% message.payload)
      txt=str(message.payload)
      lista1=txt.split('%')
      lista2=str(lista1[1])
      lista3=lista2.split('/')
      print(lista3[0])
      print(lista3[1])
      print(lista3[2])
      print(lista3[3])
      print(lista3[4])
      database.insertDatos(lista3[0],lista3[1],lista3[2],lista3[3],lista3[4])

#Día actual
today = datetime.date.today()
print(today)
present = datetime.datetime.now()
hora= present.hour
print(hora)


client=mqtt.Client(client_id="Henry",clean_session=False)
client.on_connect=on_connect
client.on_message=on_message
client.connect(host='192.168.10.1',port=1883)
client.loop_forever()