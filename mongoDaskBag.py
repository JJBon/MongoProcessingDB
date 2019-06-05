#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 20 17:09:59 2019

@author: juanjosebonilla
"""
import dask
import dask.bag as bag
from mongoengine.queryset.visitor import Q
import pymongo
import json
#from mongoengine.queryset.visitor import Q


#Implementation with PyMongo
class MongoDaskBag():
    
    
    
    def __init__(self,db_name, collection_name,query=None,ormObj = None):
        self.db_name =db_name
        self.collection_name = collection_name
        self.bag = None
        self.query = query
        self.ormObj = ormObj
        
    def getBag(self, partition_size: int = 1000, partitions_num: int = None,update=False):
        if self.bag != None and not update:
            return self.bag
        else:
            
            with pymongo.MongoClient() as mongo_client:
                collection = mongo_client[self.db_name][self.collection_name]
                #local_collection = collection.find({'_id': 1})
                if self.query == None:
                    print("No query")
                    items = list(collection.find())
                else:
                    print("using query")
                    items = list(self.ormObj.objects(self.query))
                    #items = list(self.query)
                memSize = mongo_client[self.db_name].command("collstats", self.collection_name)["size"]
                print("Size is ",memSize)
    
            all_ids = [x['orderID'] for x in items]
            size = len(all_ids)
            partition_size = size//10 #memSize // 100
            partitions_num = 100
            #partition_size, partitions_num = adjust_partition_size(size, partitions_num, partition_size)
            print("size ",size)
            print("partition size ",partition_size)
    
            start_indexes = list(range(0, size, partition_size))
            start_ids = [all_ids[i] for i in start_indexes]
            end_ids = [all_ids[i - 1] for i in start_indexes[1:]] + [all_ids[-1]]
    
            partitions_requests = list(zip(start_ids, end_ids))
            print(partitions_requests)
            #logging.info(f"Data partitioned: {partition_size}x{len(partitions_requests)}")
            #b = (bag.from_sequence(all_ids))
            b = (bag.from_sequence(partitions_requests).map(self.read_whole).flatten())
            print(type(b))
    
    #        b = (dask.bag.from_sequence(partitions_requests)
    #                     .map(self.read_datetime_interval_from_collection)
    #                     .flatten())
   
    
            self.bag = b
            return b
           
            
    def toDataFrame(self):
        if self.bag != None:
            return self.bag.to_dataframe()
        else:
            self.getBag()
            return self.bag.to_dataframe()


    
    def read_whole(self,args):
        start_ts, end_ts = args
        print("mapping")
        with pymongo.MongoClient() as mongo_client:
            collection = mongo_client[self.db_name][self.collection_name]
            items = list(collection.find({'OrderID': {'$gte': start_ts, '$lte': end_ts},'Quantity':{'$gte':20}}))
        #newQuery = Q(orderID__gte = start_ts) & Q(orderID__lte = end_ts)
        
        #print(self.ormObj.objects(self.query & newQuery).explain())
        #print(self.query & newQuery)
        #items = self.ormObj.objects.filter((self.query) & (newQuery))
        #json_data = items.to_json()
        #dicts = json.loads(json_data)
#        print("fetched ", len(items))
        #items = list(self.query.find({'OrderID': {'$gte': start_ts, '$lte': end_ts}}))
        return items
    
    def obtainJson(self,obj):
        return obj.to_mongo()
        

    def read_datetime_interval_from_collection(self, args):
        start_ts, end_ts = args
        with pymongo.MongoClient() as mongo_client:
            collection = mongo_client[self.db_name][self.collection_name]
            items = list(collection.find({'ts': {'$gte': start_ts, '$lte': end_ts}}))
        return items
    
    
class MongoDaskBag2():
    
    
    
    def __init__(self,db_name, collection_name,query=None,ormObj = None):
        self.db_name =db_name
        self.collection_name = collection_name
        self.bag = None
        self.query = query
        self.ormObj = ormObj
        
    def getBag(self, partition_size: int = 1000, partitions_num: int = None,update=False):
        if self.bag != None and not update:
            return self.bag
        else:
            
            with pymongo.MongoClient() as mongo_client:
                collection = mongo_client[self.db_name][self.collection_name]
                #local_collection = collection.find({'_id': 1})
                if self.query == None:
                    print("No query")
                    items = list(collection.find())
                else:
                    print("using query")
                    items = list(self.ormObj.objects(self.query))
                    #items = list(self.query)
                memSize = mongo_client[self.db_name].command("collstats", self.collection_name)["size"]
                print("Size is ",memSize)
                
    
            dicts = [x.to_mongo() for x in items]
    
            #logging.info(f"Data partitioned: {partition_size}x{len(partitions_requests)}")
            b = (bag.from_sequence(dicts))
            #b = (bag.from_sequence(partitions_requests).map(self.read_whole).flatten())
            print(type(b))
    
    #        b = (dask.bag.from_sequence(partitions_requests)
    #                     .map(self.read_datetime_interval_from_collection)
    #                     .flatten())
   
    
            self.bag = b
            return self.bag
           
            
    def toDataFrame(self):
        if self.bag != None:
            return self.bag.to_dataframe()
        else:
            self.getBag()
            return self.bag.to_dataframe()


    
    def read_whole(self,args):
        start_ts, end_ts = args
        # add combined Query
#        with pymongo.MongoClient() as mongo_client:
#            collection = mongo_client[self.db_name][self.collection_name]
#            items = list(collection.find({'OrderID': {'$gte': start_ts, '$lte': end_ts}}))
        newQuery = Q(orderID__gte = start_ts) & Q(orderID__lte = end_ts)
        #print(self.ormObj.objects(self.query & newQuery).explain())
        print(self.query & newQuery)
        items = list(self.ormObj.objects((self.query) & (newQuery)))
#        print("fetched ", len(items))
        #items = list(self.query.find({'OrderID': {'$gte': start_ts, '$lte': end_ts}}))
        return items
    
    def obtainJson(self,obj):
        return obj.to_mongo()
        

    def read_datetime_interval_from_collection(self, args):
        start_ts, end_ts = args
        with pymongo.MongoClient() as mongo_client:
            collection = mongo_client[self.db_name][self.collection_name]
            items = list(collection.find({'ts': {'$gte': start_ts, '$lte': end_ts}}))
        return items
    
##Implementation with MongoEngine
#    
#trial = MongoDaskBag("Northwind","order-details")
#df = trial.toDataFrame()
#df2 = df.compute()
#bag  = trial.bag()
#print(bag.count().compute())
#orders = bag.pluck("OrderID")
#print(orders.compute())
#
#bag = bag.compute()
#print(bag)
#print(bag.count())
