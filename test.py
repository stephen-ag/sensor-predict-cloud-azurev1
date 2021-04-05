
import os
import pandas as pd
import json
import pymongo
from mongoDBoperation import MongodbOperation
# importing mongodb file

#client = pymongo.MongoClient("mongodb+srv://{0}:{1}@cluster0.tpqna.mongodb.net/Projectdb?retryWrites=true&w=majority")
client = pymongo.MongoClient("mongodb+srv://test:test@cluster0.tpqna.mongodb.net/Projectdb?retryWrites=true&w=majority")
db = client.get_database('Wafer-sys')
mydb = client['Wafer-sys']
mycol = mydb["schema-training"]

#records = db.new1_db

#a=records.count_documents({})
#print(a)

""" for testing the class function"""


a=MongodbOperation()
#c=a.getDataBaseClientObject()
#d=a.createDatabase(c,'newentry')

#e=a.createCollectionInDatabase(d,'values')
record={"Sensor - 3":" float "}
record1={"name":"sherwyn"}
record3={"name":"leena","rollno": 3223,"dept": "plsql"}
record4={"name":"sangetha","rollno": 13223,"dept": "12plsql"}

#data1 = [{"code":"2","sum":"10"},{"local":"20"}]
#df = pd.DataFrame(data1)
#g=a.checkExistingCollection("values", d)
#print(g)

#d=a.checkDatabase(c,'Projectdb')
#i=a.getCollection("values", d)

#s=a.isRecordPresent("newentry", "values",record1)
#s=a.insertRecordInCollection("newentry2", "values2", record3)
#p=a.dropCollection("newentry2","values2")

#r=a.getDataFrameofCollection("Wafer-sys","schema_training")
#f= open("/home/gerald/Downloads/avnish_WaferFaultDetection_new/schema_training1.json", 'r+')
#r = f.read()
#df={r}

#MongodbOperation.insertRecordsInCollection("Wafer-sys", "schema-training", df)
#print(df)
os.scandir()
with open("schema_training1.json") as f:
    #file_data =f.readlines()
    file_data = json.load(f)
    #for i in file_data:
    #db.collection.insert_one(file_data)
    mycol.insert_one(file_data)
        #db.collection.insert_many(file_data)(collection = 'schema-training')
    #print(file_data)

#a.insertRecordsInCollection("Wafer-sys", "schema-training", file_data)

#r = a.getDataFrameofCollection("newentry", "values")
#r.to_csv('result.csv')
#print(file_object.read())
getDataFrameofCollection