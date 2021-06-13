from mongoengine import connect, DateTimeField, IntField, StringField, Document
import json
from datetime import datetime, timedelta
import time
from random import randint

connect("fyp_2021_busq")

# Defining documents

class pplno_south(Document):
    Time = DateTimeField(required=True)
    B91M = IntField()
    M11 = IntField()
    M104 = IntField()
    B91P = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

class pplno_north(Document):
    Time = DateTimeField(required=True)
    B91M = IntField()
    M11 = IntField()
    M104 = IntField()
    B91P = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

class qtd_boarded_91m(Document):
    Time = DateTimeField(required=True)
    T0_4 = IntField()
    T5_9 = IntField()
    T10_14 = IntField()
    T15_19 = IntField()
    T20 = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

class qtd_boarded_91m(Document):
    Time = DateTimeField(required=True)
    T0_4 = IntField()
    T5_9 = IntField()
    T10_14 = IntField()
    T15_19 = IntField()
    T20 = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

class qtd_boarded_11(Document):
    Time = DateTimeField(required=True)
    T0_4 = IntField()
    T5_9 = IntField()
    T10_14 = IntField()
    T15_19 = IntField()
    T20 = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

class qtd_boarded_104(Document):
    Time = DateTimeField(required=True)
    T0_4 = IntField()
    T5_9 = IntField()
    T10_14 = IntField()
    T15_19 = IntField()
    T20 = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

class qtd_boarded_north(Document):
    Time = DateTimeField(required=True)
    T0_4 = IntField()
    T5_9 = IntField()
    T10_14 = IntField()
    T15_19 = IntField()
    T20 = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

class qtd_boarded_91p(Document):
    Time = DateTimeField(required=True)
    T0_4 = IntField()
    T5_9 = IntField()
    T10_14 = IntField()
    T15_19 = IntField()
    T20 = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

class qtd_current_91m(Document):
    Time = DateTimeField(required=True)
    T0_4 = IntField()
    T5_9 = IntField()
    T10_14 = IntField()
    T15_19 = IntField()
    T20 = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

class qtd_current_11(Document):
    Time = DateTimeField(required=True)
    T0_4 = IntField()
    T5_9 = IntField()
    T10_14 = IntField()
    T15_19 = IntField()
    T20 = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

class qtd_current_104(Document):
    Time = DateTimeField(required=True)
    T0_4 = IntField()
    T5_9 = IntField()
    T10_14 = IntField()
    T15_19 = IntField()
    T20 = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

class qtd_current_91p(Document):
    Time = DateTimeField(required=True)
    T0_4 = IntField()
    T5_9 = IntField()
    T10_14 = IntField()
    T15_19 = IntField()
    T20 = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

class qtd_current_north(Document):
    Time = DateTimeField(required=True)
    T0_4 = IntField()
    T5_9 = IntField()
    T10_14 = IntField()
    T15_19 = IntField()
    T20 = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

class raw_data(Document):
    Time = DateTimeField(required=True)
    Target = StringField()
    Receiver = IntField()
    Strength = IntField()

    meta = {
        "indexes": [{'fields': ['Time'], 'expireAfterSeconds': 633600}],
        "ordering": ["-Time"]
    }

raw_data(Time=datetime.now(),Target="5pVuSA5E",Receiver=5,Strength=40).save()
pplno_south(Time=datetime.now(),B91M=randint(0,10),M11=randint(0,10),M104=randint(0,10),B91P=randint(0,10)).save()
pplno_north(Time=datetime.now(),B91M=randint(0,10),M11=randint(0,10),M104=randint(0,10),B91P=randint(0,10)).save()
qtd_boarded_91m(Time=datetime.now(),T0_4=randint(0,10),T5_9=randint(0,10),T10_14=randint(0,10),T15_19=randint(0,10),T20=randint(0,10)).save()
qtd_boarded_11(Time=datetime.now(),T0_4=randint(0,10),T5_9=randint(0,10),T10_14=randint(0,10),T15_19=randint(0,10),T20=randint(0,10)).save()
qtd_boarded_104(Time=datetime.now(),T0_4=randint(0,10),T5_9=randint(0,10),T10_14=randint(0,10),T15_19=randint(0,10),T20=randint(0,10)).save()
qtd_boarded_91p(Time=datetime.now(),T0_4=randint(0,10),T5_9=randint(0,10),T10_14=randint(0,10),T15_19=randint(0,10),T20=randint(0,10)).save()
qtd_boarded_north(Time=datetime.now(),T0_4=randint(0,10),T5_9=randint(0,10),T10_14=randint(0,10),T15_19=randint(0,10),T20=randint(0,10)).save()
qtd_current_91m(Time=datetime.now(),T0_4=randint(0,10),T5_9=randint(0,10),T10_14=randint(0,10),T15_19=randint(0,10),T20=randint(0,10)).save()
qtd_current_11(Time=datetime.now(),T0_4=randint(0,10),T5_9=randint(0,10),T10_14=randint(0,10),T15_19=randint(0,10),T20=randint(0,10)).save()
qtd_current_104(Time=datetime.now(),T0_4=randint(0,10),T5_9=randint(0,10),T10_14=randint(0,10),T15_19=randint(0,10),T20=randint(0,10)).save()
qtd_current_91p(Time=datetime.now(),T0_4=randint(0,10),T5_9=randint(0,10),T10_14=randint(0,10),T15_19=randint(0,10),T20=randint(0,10)).save()
qtd_current_north(Time=datetime.now(),T0_4=randint(0,10),T5_9=randint(0,10),T10_14=randint(0,10),T15_19=randint(0,10),T20=randint(0,10)).save()

import pymongo
DB_KEY = "mongodb://localhost:27017"
collections = ['raw_data','pplno_south','qtd_current_91m',
'qtd_current_11','qtd_current_104','qtd_current_91p','qtd_boarded_91m','qtd_boarded_11','qtd_boarded_104','qtd_boarded_91p','pplno_north','qtd_current_north','qtd_boarded_north']

for col in collections:
    collection = pymongo.MongoClient(DB_KEY)['fyp_2021_busq'][col]
    # collection.delete_many({})
