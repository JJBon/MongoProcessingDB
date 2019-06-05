# -*- coding: utf-8 -*-
"""
Created on Wed May 29 11:50:00 2019

@author: Cheil
"""
from pymongo import MongoClient, errors
import pandas as pd
from mongoengine import Document
from abc import ABC, abstractmethod, ABCMeta

#global Variables

client = MongoClient("localhost",27017) 
db = client.testMSOV1

productCollection = db.product
divisionCollection = db.division
brandCollection  = db.brand
specCollection   = db.spec
specTypeCollection = db.spec_type
countryCollection= db.country
cityCollection = db.city
zoneCollection = db.zone
customerCollection = db.customer
posCollection      = db.pos
saleIMCollection = db.sale_i_m
inventoryIMCollection = db.inventory_i_m
inventoryAVCollection = db.inventory_a_v
inventoryDACollection = db.inventory_d_a
wholeInventory = db.whole_inventory
priceCollection = db.price


def sampleCode():

    client = MongoClient("localhost",27017)
    db = client.testMSOV1
    
    wholeSales = db.whole_sale
    
    
    imSales = db.sale_i_m
    imSales.update_many({},{"$set" :{"div":"IM"}})
    
    
    avSales =  db.sale_a_v
    avSales.update_many({},{"$set" :{"div":"AV"}})
    
    daSales =  db.sale_d_a
    daSales.update_many({},{"$set" :{"div":"DA"}})
    
    
    
    db.command("cloneCollection", **{"cloneCollection": "testMSOV1" + ".SalesIM",
                                                  'collection': "testMSOV1" + ".SalesIM",
                                                  'from': "127.0.0.1" + ":27017"})

class mongoInterface():
    __metaclass__ = ABCMeta
    
    @classmethod
    def getRawData(self):
        return pd.DataFrame(list(self.collection.find()))
    
    @abstractmethod
    def insertBulk(self,collection,dicts):
        bulk = collection.initialize_ordered_bulk_op()
        for i,item in enumerate(dicts):
            bulk.insert(item)
        bulk.execute()
        
    
        
        
        
        
    

def insertAll(dicts):
    bulk = wholeInventory.initialize_ordered_bulk_op()
    for i,item in enumerate(dicts):
        print(item)
        bulk.insert(item)
    bulk.execute()

def updateAll(dicts):
    bulk = wholeSales.initialize_ordered_bulk_op()
    for item in dicts:
        try:
            print(item)
            bulk.find({"identifier":item["_id"]}).update({{"$set":{"quantity":item["quantity"],"quantity_b":item["quantity_b"]}}})
        except:
            continue
    bulk.execute()
    print("finish transaction")
        
def insertWhole(dicts):
    pass        

#insertAll(dicts)