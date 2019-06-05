# -*- coding: utf-8 -*-
"""
Created on Fri May 24 11:18:16 2019

@author: Cheil
"""

#from mongoengine import *
import ormV2
import MongoShell
from mongoengine import Document, StringField, IntField, connect, ListField, ReferenceField, FloatField,Q,DateTimeField, QuerySet
from mongoengine.base import BaseDocument
from mongoengine.context_managers import switch_collection
import pandas as pd
import threading
import time 
import traceback
from mongoengine.queryset import QuerySet
from abc import ABC, abstractmethod, ABCMeta
from json import JSONEncoder
from bson import json_util
import json
import datetime
from pymongo import UpdateOne
connect("testMSOV1")


class AlquemyInterface():
   __metaclass__ = ABCMeta
    
   @property  
   @classmethod
   def mapper(self):
       pass
   
   @property
   @classmethod
   def ormKeys(self):
       pass
   
   @classmethod
   def downloaderInit(cls,row):
       pass
   
 
   
   @classmethod
   def getView(cls,insert=True):
        if insert:
            json_data = cls.objects().to_json()
            dicts = json.loads(json_data)
            if "date" in dicts[0].keys():
                for x in dicts:
                    print(x)
                    x["date"] = x["date"]["$date"] 
                    
                for x in dicts:
                    if isinstance(x["date"],int):
                        if x["date"] != None:
                            date = x["date"]
                            date = str(date)
                            date = date[:10]
                            print(date)
                            date = int(date)
                            x["date"] =  datetime.datetime.utcfromtimestamp(date)
                            
                            
                            
                        else:
                            continue
                    
                    
            return pd.DataFrame(dicts)

   
   
   @classmethod
   def migrate(cls):
        
        ormID = ormV2.session.query(cls.mapper.id).all()
        #frame = pd.read_sql(ormID.statement, ormID.session.bind)
        #ormIDs = frame[cls.ormKeys["id"]].values()
        ormIDs = [x[0] for x in ormID]
        print(ormIDs)
        ormIDs = set(ormIDs)
        mongoIDs = set(cls.objects().distinct(field="id"))
        print(mongoIDs)
        newIDs = list(ormIDs - mongoIDs)
        print(newIDs)
        #salesOrm = ormV2.getObjects(ormV2.SaleIM).filter(ormV2.SaleIM.id.in_(list(salesIDs)))
        # break id by 5 

        
        objects = ormV2.session.query(cls.mapper).filter(cls.mapper.id.in_(list(newIDs)))

        frame = pd.read_sql(objects.statement, objects.session.bind)
        print(frame)
        
        print("objects to upload:",len(frame), " in class " +str(cls))
        
        if len(frame) > 0:
        
            mongoObj = []
            
            for index, row in frame.iterrows():
                print(index)
                obj = cls.downloaderInit(row)
                #obj = SaleIM(id = row["MarketSellOutIMID"],unitPrice=row["UnitPrice"],quantity=row["Sale"],date = row["Created_Date"],div="IM")
                mongoObj.append(obj)
                
            cls.objects.insert(mongoObj)
            
   @classmethod
   def updateOne(cls,field,Ref):
      try:
            
            ## unpack
            objects = cls.objects(Q(**{field+"__exists":False}) | Q(**{field+"__size":0}))
            if objects.count() > 0:
                #objects = cls.objects()
                print("objects to upload ",str(cls) + " ", objects.count() )
                #objectsIDs = [x.id for x in objects]
                objectsIDs = list(objects.scalar("id"))
                #objectsIDs = set(objectsIDs)
                print(objectsIDs)
            
                
                #objectsOrm = ormV2.session.query(cls.mapper).filter(cls.mapper.id.in_(objectsIDs))
                objectsOrm = ormV2.session.query(cls.mapper)
                frame = pd.read_sql(objectsOrm.statement, objectsOrm.session.bind)
                #print(frame.columns)
                subFrame = frame[[cls.ormKeys["id"], Ref.ormKeys["id"]]]
                mapping = [tuple(x) for x in subFrame.values]
                #mapping = [(x.id,getattr(x,field+'_id')) for x in objectsOrm]
                
                requests = []
                
                for item in mapping:
                    print(item[0],item[1])
                    operation = UpdateOne({"_id":int(item[0])},{"$set" :{field: int(item[1])}})
                    requests.append(operation)
                    
                print("performing bulk operation")    
                
                cls.collection.bulk_write(requests)
                
      except:
            print("updateOne failure")
            traceback.print_exc()
            
   @classmethod
   def updater(cls,updates,nulls=True):
        if nulls:
            # build composite key
            query = None
            for field in updates.keys():
                 newQuery =  Q(**{field+"__exists":False}) #| Q(**{field+"__size":0})
                 if query != None:
                     query | newQuery
                 else:
                     query = newQuery
            print("query",query)
            objectsMongo = cls.objects(query) 
            objectsIDs = [x.id for x in objectsMongo]
            objectsIDs = set(objectsIDs)
            print("objects to upload ",len(objectsIDs))
            objects = ormV2.getObjects(cls.mapper).filter(cls.mapper.id.in_(objectsIDs))
            frame = pd.read_sql(objects.statement,objects.session.bind)
            if len(frame) > 0:
                print("to update " , len(frame))
    
                for index, row in frame.iterrows():
                    #print(index)
                    try:
                        obj = objectsMongo.get(id = row[cls.ormKeys["id"]])
                    except:
                        continue
                    if obj != None:
                        for key , value in updates.items():
                            updates[key]= row[cls.ormKeys[key]]
                        obj.update(**updates)
                       
class InvInterface():
    
    @classmethod
    def combineFrames(cls):
        
        mainView = cls.getView()
        product = Product.getView()
        country = Country.getView()
        pos     = Pos.getView()
        customer = Customer.getView()
        zone     = Zone.getView()
        city = City.getView()
        brand = Brand.getView()
        productGroup = ProductGroup.getView()
        
        
        
        join1 = pd.merge(mainView,product[["_id","model","brand","productGroup"]],left_on=["product"],right_on=["_id"])
        join2 = join1.drop(columns=["product"])
        join1 = join1.rename(columns = {"_id_y":"product_id","_id_x":"identifier"})
        join2 = pd.merge(join1,brand[["_id","name"]], left_on=["brand"],right_on=["_id"], how="left")
        join2 = join2.drop(columns=["brand"])
        join2 = join2.rename(columns = {"_id":"brand_id","name":"brand"})
        join3 = pd.merge(join2,pos[["_id","city","code","country","zone","latitude","longitude","name"]],left_on=["pos"],right_on=["_id"],how="left")
        join3 = join3.drop(columns=["pos"])
        join3 = join3.rename(columns = {"_id":"pos_id","name":"pos"})
        join4 = pd.merge(join3,city[["_id","name"]],left_on=["city"],right_on=["_id"],how="left")
        join4 = join4.drop(columns=["city"])
        join4 = join4.rename(columns = {"_id":"city_id","name":"city"})
        join5 = pd.merge(join4,country[["_id","name"]],left_on=["country"],right_on=["_id"],how="left")
        join5 = join5.drop(columns=["country"])
        join5 = join5.rename(columns = {"_id":"country_id","name":"country"})
        join6 = pd.merge(join5,customer[["_id","name"]],left_on=["customer"],right_on=["_id"],how="left")
        join6 = join6.drop(columns=["customer"])
        join6 = join6.rename(columns = {"_id":"customer_id","name":"customer"})
        join7 = pd.merge(join6,zone[["_id","name"]],left_on=["zone"],right_on=["_id"],how="left")
        join7 = join7.drop(columns=["zone"])
        join7 = join7.rename(columns = {"_id":"zone_id","name":"zone"})
        join8 = pd.merge(join7,productGroup[["_id","name"]],left_on=["productGroup"],right_on=["_id"],how="left")
        join8 = join8.drop(columns=["productGroup"])
        join8 = join8.rename(columns = {"_id":"productGroup_id","name":"productGroup"})
        
        return join8
          
    
class SaleInterface():
    
    
    def moveToCollection(self,obj):
        print("interface Call")
        print(type(obj))
        with switch_collection(obj,"whole-sales") as obj:
            obj(identifier = self.id, div = self.__class__.__name__ ,product=self.product,
                pos=self.pos,customer=self.customer, country = self.country, unitPrice = self.unitPrice ,
                quantity = self.quantity, date=self.date).save()
    
    @classmethod
    def combineFrames(cls):
        
        mainView = cls.getView()
        product = Product.getView()
        country = Country.getView()
        pos     = Pos.getView()
        customer = Customer.getView()
        zone     = Zone.getView()
        city = City.getView()
        brand = Brand.getView()
        productGroup = ProductGroup.getView()
        
        
        
        join1 = pd.merge(mainView,product[["_id","model","brand","productGroup"]],left_on=["product"],right_on=["_id"])
        join2 = join1.drop(columns=["product"])
        join1 = join1.rename(columns = {"_id_y":"product_id","_id_x":"identifier"})
        join2 = pd.merge(join1,brand[["_id","name"]], left_on=["brand"],right_on=["_id"], how="left")
        join2 = join2.drop(columns=["brand"])
        join2 = join2.rename(columns = {"_id":"brand_id","name":"brand"})
        join3 = pd.merge(join2,pos[["_id","city","code","country","zone","latitude","longitude","name"]],left_on=["pos"],right_on=["_id"],how="left")
        join3 = join3.drop(columns=["pos"])
        join3 = join3.rename(columns = {"_id":"pos_id","name":"pos"})
        join4 = pd.merge(join3,city[["_id","name"]],left_on=["city"],right_on=["_id"],how="left")
        join4 = join4.drop(columns=["city"])
        join4 = join4.rename(columns = {"_id":"city_id","name":"city"})
        join5 = pd.merge(join4,country[["_id","name"]],left_on=["country"],right_on=["_id"],how="left")
        join5 = join5.drop(columns=["country"])
        join5 = join5.rename(columns = {"_id":"country_id","name":"country"})
        join6 = pd.merge(join5,customer[["_id","name"]],left_on=["customer"],right_on=["_id"],how="left")
        join6 = join6.drop(columns=["customer"])
        join6 = join6.rename(columns = {"_id":"customer_id","name":"customer"})
        join7 = pd.merge(join6,zone[["_id","name"]],left_on=["zone"],right_on=["_id"],how="left")
        join7 = join7.drop(columns=["zone"])
        join7 = join7.rename(columns = {"_id":"zone_id","name":"zone"})
        join8 = pd.merge(join7,productGroup[["_id","name"]],left_on=["productGroup"],right_on=["_id"],how="left")
        join8 = join8.drop(columns=["productGroup"])
        join8 = join8.rename(columns = {"_id":"productGroup_id","name":"productGroup"})
        
        return join8

    
    
    @classmethod
    def combineFrames2(cls):
        
        mainView = cls.getView()
        product = Product.getView()
        country = Country.getView()
        pos     = Pos.getView()
        customer = Customer.getView()
        zone     = Zone.getView()
        city = City.getView()
        brand = Brand.getView()
        
        return mainView, product, country, pos, customer, zone, city, brand

    
    @classmethod        
    def combineFrames3(cls):
        objects = cls.objects.aggregate(*[{
                '$lookup': {
                        'from': Product._get_collection_name(),
                        'localField': 'product',
                        'foreignField': '_id',
                        'as': 'productInfo'
                        }
                },
                {
                '$lookup': {
                        'from': City._get_collection_name(),
                        'localField': 'city',
                        'foreignField': '_id',
                        'as': 'cityInfo'
                        }
                },
                {
                '$lookup': {
                        'from': Country._get_collection_name(),
                        'localField': 'country',
                        'foreignField': '_id',
                        'as': 'countryInfo'
                        }
                },
                {
                '$lookup': {
                        'from': Zone._get_collection_name(),
                        'localField': 'zone',
                        'foreignField': '_id',
                        'as': 'zoneInfo'
                        }
                },
                {
                '$lookup': {
                        'from': Customer._get_collection_name(),
                        'localField': 'customer',
                        'foreignField': '_id',
                        'as': 'customerInfo'
                        }
                },
                {
                '$lookup': {
                        'from': Pos._get_collection_name(),
                        'localField': 'pos',
                        'foreignField': '_id',
                        'as': 'posInfo'
                        }
                }
                ])
        #return objects
     
        #documents = []
        for doc in objects:
            print(doc)
            #documents.append(doc)
        #return documents
            #{"identifier":self.id, div=cls.__class__.__name__,product = self.product.model,product_id = }
            
        
         
#    
            
    
    
    def getAmount(self):
        return self.unitPrice * self.quantity
    
    def varWarning(self):
        pass
    
        


class Product(Document, MongoShell.mongoInterface,AlquemyInterface):
    id              = IntField(primary_key = True)
    code            = StringField()
    model           = StringField()
    division        = ReferenceField("Division")
    productGroup    = ReferenceField("ProductGroup")
    types           = ListField(ReferenceField("SpecType"))
    brand           = ReferenceField("Brand")
    collectionName  = "product"
    collection      = MongoShell.productCollection
    mapper          = ormV2.Product
    ormKeys         = {"id":"ProductID","code":"CodEAN","model":"Model","divison":"Division","brand":"Brand"}
    
    
    salesIM         = ListField(ReferenceField("SaleIM"))
    salesAV         = ListField(ReferenceField("SaleAV"))
    salesDA         = ListField(ReferenceField("SaleDA"))
    
    @classmethod
    def downloaderInit(cls,row):
         return cls(id = row[cls.ormKeys["id"]],model=row[cls.ormKeys["model"]])

    
    def getCatSales(self,country=None,city=None,pos=None):
        
        if self.division.name == "IM":
            return self.salesIM
        if self.division.name == "AV":
            return self.salesAV
        if self.divison.name == "DA":
            return self.salesDA
        
    def getCatSales2(self,country=None,city=None,pos=None):
        
        query = None
        
        if self.division.name == "IM":
            query = self.salesIM
        if self.division.name == "AV":
            query = self.salesAV
        if self.division.name == "DA":
            query = self.salesDA
        
        if country != None:
            query = [x for x in query if x.country == country]
        
        if city != None:
            query = [x for x in query if x.pos.city == city]
            
        if pos != None:
            query = [x for x in query  if x.pos == pos]
            
        return query
    

    def updateTypesWhole():
        products = Product.objects()
        products = [product.updateTypes()[1] for product in products if product.updateTypes()[0]]
        [product.save() for product in products if product != None]
    
    def updateTypes(self):
        if self.types == None or len(self.types) == 0:
            types = SpecType.objects(product = self.id)
            if types != None:
                if types.count() > 0:
                    self.types = SpecType.objects(product = self.id)
                    print("updating Product")
                    return True, self
                else:
                    return False, None
            else:
                return False, None
        else:
            return False, None
            #self.update(types = SpecType.objects(product = self.id))
            
        
   
    def updateSalesWhole():
        products = Product.objects()
        [product.updateSales() for product in products]
            
    def updateSales(self):
        print("method in for")
        if self.division.name == "AV":
            print("object is AV")
            if self.salesAV != None and len(self.salesAV) > 0:
                if len(self.salesAV) == SaleAV.objects(product = self.id).count():
                    return None
                else:
                    self.update(salesAV = SaleAV.objects(product = self.id))
                    print("updateComplete")
            else:
                if SaleAV.objects(product = self.id).count() == 0:
                    return None
                else:
                    self.update(salesAV = SaleAV.objects(product = self.id))
                    print("updateComplete")
        
        if self.division.name == "IM":
            print("object is IM")
            if self.salesIM != None and len(self.salesIM) > 0:
                if len(self.salesIM) == SaleIM.objects(product = self.id).count():
                    return None
                else:
                    self.update(salesIM = SaleIM.objects(product = self.id))
                    print("updateComplete")
            else:
                if SaleIM.objects(product = self.id).count() == 0:
                    return None
                else:
                    self.update(salesIM = SaleIM.objects(product = self.id))
                    print("updateComplete")
                
        if self.division.name == "DA":
            print("object is DA")
            if self.salesDA != None and len(self.salesDA) > 0:
                if len(self.salesDA) == SaleDA.objects(product = self.id).count():
                    return None
                else:
                    self.update(salesDA = SaleDA.objects(product = self.id))
                    print("updateComplete")
            else:
                if SaleDA.objects(product = self.id).count() == 0:
                    return None
                else:
                    self.update(salesDA = SaleDA.objects(product = self.id))
                    print("updateComplete")
    
    def getSpecs(self):
        return [(x.name,x.spec.name) for x in self.types]
    
    @classmethod
    def updateAllOnes(cls):
        cls.updateOne("division",Division)
        cls.updateOne("brand",Brand)
        cls.updateOne("productGroup",ProductGroup)
        
    @classmethod
    def updateMany(cls):
        cls.updateTypesWhole()
        cls.updateSalesWhole()
    
class Price(Document,MongoShell.mongoInterface,AlquemyInterface):
    id              = IntField(primary_key= True)
    mapper          = ormV2.Price
    ormKeys         = {"id":"PriceEvidenceID","offerPrice":"OfferPrice","weekPrice":"WeekPrice","regularPrice":"RegularPrice","cardPrice":"CardPrice","date":"Created_Date"}
    offerPrice      = IntField()
    regularPrice    = IntField()
    cardPrice       = IntField()
    weekPrice       = IntField()
    date            = DateTimeField()
    brand           = ReferenceField("Brand")
    product         = ReferenceField("Product") 
    pos             = ReferenceField("Pos")
    customer        = ReferenceField("Customer")
    collection      = MongoShell.priceCollection
    
    @classmethod
    def combineFrames(cls):
        
        mainView = cls.getView()
        product = Product.getView()
        country = Country.getView()
        pos     = Pos.getView()
        customer = Customer.getView()
        zone     = Zone.getView()
        city = City.getView()
        brand = Brand.getView()
        productGroup = ProductGroup.getView()
        
        
        
        join1 = pd.merge(mainView,product[["_id","model","brand","productGroup"]],left_on=["product"],right_on=["_id"])
        join2 = join1.drop(columns=["product"])
        join1 = join1.rename(columns = {"_id_y":"product_id","_id_x":"identifier"})
        join2 = pd.merge(join1,brand[["_id","name"]], left_on=["brand"],right_on=["_id"], how="left")
        join2 = join2.drop(columns=["brand"])
        join2 = join2.rename(columns = {"_id":"brand_id","name":"brand"})
        join3 = pd.merge(join2,pos[["_id","city","code","country","zone","latitude","longitude","name"]],left_on=["pos"],right_on=["_id"],how="left")
        join3 = join3.drop(columns=["pos"])
        join3 = join3.rename(columns = {"_id":"pos_id","name":"pos"})
        join4 = pd.merge(join3,city[["_id","name"]],left_on=["city"],right_on=["_id"],how="left")
        join4 = join4.drop(columns=["city"])
        join4 = join4.rename(columns = {"_id":"city_id","name":"city"})
        join5 = pd.merge(join4,country[["_id","name"]],left_on=["country"],right_on=["_id"],how="left")
        join5 = join5.drop(columns=["country"])
        join5 = join5.rename(columns = {"_id":"country_id","name":"country"})
        join6 = pd.merge(join5,customer[["_id","name"]],left_on=["customer"],right_on=["_id"],how="left")
        join6 = join6.drop(columns=["customer"])
        join6 = join6.rename(columns = {"_id":"customer_id","name":"customer"})
        join7 = pd.merge(join6,zone[["_id","name"]],left_on=["zone"],right_on=["_id"],how="left")
        join7 = join7.drop(columns=["zone"])
        join7 = join7.rename(columns = {"_id":"zone_id","name":"zone"})
        join8 = pd.merge(join7,productGroup[["_id","name"]],left_on=["productGroup"],right_on=["_id"],how="left")
        join8 = join8.drop(columns=["productGroup"])
        join8 = join8.rename(columns = {"_id":"productGroup_id","name":"productGroup"})
        
        return join8
    
    
        
    @classmethod
    def downloaderInit(cls,row):
         return cls(id = row[cls.ormKeys["id"]],
                    offerPrice=row[cls.ormKeys["offerPrice"]],
                    regularPrice=row[cls.ormKeys["regularPrice"]],
                    cardPrice=row[cls.ormKeys["cardPrice"]],
                    weekPrice=row[cls.ormKeys["weekPrice"]],
                    date     =row[cls.ormKeys["date"]]
                    )
          
    @classmethod    
    def updateAllOnes(cls):
        #cls.updateOne("product",Product)
        #cls.updateOne("pos",Pos)
        #cls.updateOne("brand",Brand)
        cls.updateOne("customer",Customer)
        
    
    meta            = {"collection":"price"}
    
    
class Division(Document,MongoShell.mongoInterface):
    id              = IntField(primary_key= True)
    name            = StringField(max_length=100 , required = True)  
    products        = ListField(ReferenceField("Product"))
    collection      = MongoShell.divisionCollection
    
    def migrateDivision():
        divisions = ormV2.getObjects(ormV2.Division)
        for x in divisions:
            div = Division(id = x.id, name = x.name)
            div.save()
            
    def updateProducts():
        # buscar divisiones sin productos
        divisions = Division.objects(Q(products__exists=False) | Q(products__size=0))
        if divisions.count() > 0 :
            for division in divisions:
                # Search products ids in mySQL
                pass
            
    def updateOne():
        pass
   
    def updateMany():
        Division.updateProducts()
        
    
class Brand(Document,MongoShell.mongoInterface,AlquemyInterface):
    id              = IntField(primary_key=True)
    name            = StringField(required = True) 
    mapper          = ormV2.Brand
    ormKeys         = {"id":"BrandID","name":"BrandName"}
    products        = ListField(ReferenceField("Product"))
    collection      = MongoShell.brandCollection
    
    @classmethod
    def downloaderInit(cls,row):
         return cls(id = row[cls.ormKeys["id"]],name=row[cls.ormKeys["name"]])
    
    
class Spec(Document,MongoShell.mongoInterface,AlquemyInterface):
    id              = IntField(primary_key=True)
    name            = StringField(required = True)
    mapper          = ormV2.Spec
    ormKeys         = {"id":"SpecTypeID","name":"specType"}
    collection      = MongoShell.specCollection
    
    @classmethod
    def downloaderInit(cls,row):
         return cls(id = row[cls.ormKeys["id"]],name=row[cls.ormKeys["name"]])
    

class SpecType(Document,MongoShell.mongoInterface,AlquemyInterface):
    id              = IntField(primary_key=True)
    name            = StringField(required = True) 
    mapper          = ormV2.SpecType
    ormKeys         = {"id":"ProductSpecTypeID","name":"specType"}
    product         = ListField(ReferenceField("Product"))
    collection      = MongoShell.specTypeCollection
    #ProductID       = Column("ProductID",Integer,ForeignKey("Products.ProductID"))
    #specID          = Column("SpecTypeID",Integer,ForeignKey("SpecTypes.SpecTypeID"))
    spec            = ReferenceField("Spec")
    
    
    def getMSORaw():
        return ormV2.getObjects(ormV2.SpecType)
    
    @classmethod
    def downloaderInit(cls,row):
         return cls(id = row[cls.ormKeys["id"]],name=row[cls.ormKeys["name"]])
    @classmethod                        
    def updateAllOnes(cls):
        cls.updateOne("product",Product)
    
    
    
class Country(Document,MongoShell.mongoInterface,AlquemyInterface):
    id              = IntField(primary_key=True)
    name            = StringField("Name")
    customers       = ListField(ReferenceField("Customer"))
    mapper          = ormV2.Country
    ormKeys         = {"id":"CountryID","name":"Name"}
    collection      = MongoShell.countryCollection
    #zones           = relationship("Zone",back_populates="country")
    cities          = ListField(ReferenceField("City"))
    stores          = ListField(ReferenceField("Pos"))
    salesIM         = ListField(ReferenceField("SaleIM"))
    salesAV         = ListField(ReferenceField("SaleAV"))
    salesDA         = ListField(ReferenceField("SaleDA"))
    
    @classmethod
    def downloaderInit(cls,row):
         return cls(id = row[cls.ormKeys["id"]],name=row[cls.ormKeys["name"]])
    
#    

class City(Document,MongoShell.mongoInterface,AlquemyInterface):
    id              = IntField(primary_key=True)
    name            = StringField()
    mapper          = ormV2.City
    ormKeys         = {"id":"CityID","name":"CityName"}
    country         = ReferenceField("Country")
    customers       = ListField(ReferenceField("Customer",back_populates="cities"))
    stores          = ListField(ReferenceField("Pos"))
    collection      = MongoShell.cityCollection
    
    @classmethod
    def downloaderInit(cls,row):
         return cls(id = row[cls.ormKeys["id"]],name=row[cls.ormKeys["name"]])
    
    @classmethod         
    def updateAllOnes(cls):
        cls.updateOne("country",Country)
       
    
class Zone(Document,MongoShell.mongoInterface,AlquemyInterface):
    id              = IntField(primary_key=True)
    name            = StringField("ZoneName")
    mapper          = ormV2.Zone
    ormKeys         = {"id":"CountryZoneID","name":"ZoneName"}
    #country         = relationship("Country",back_populates="zones")
    customers       = ListField(ReferenceField("Customer"))
    stores          = ListField(ReferenceField("Pos"))
    collection      = MongoShell.zoneCollection
   # cities          = relationship("City",back_populates="zone")
   
    @classmethod
    def downloaderInit(cls,row):
         print(row)
         return cls(id = row[cls.ormKeys["id"]],name=row[cls.ormKeys["name"]])
   


class Customer(Document,MongoShell.mongoInterface,AlquemyInterface):
    id              = IntField(primary_key=True)
    name            = StringField("Name")
    mapper          = ormV2.Customer
    ormKeys         = {"id":"SiteStoreID","name":"Name"}
    cities          = ListField(ReferenceField("City"))
    country         = ReferenceField("Country")
    collection      = MongoShell.customerCollection
    
    @classmethod
    def downloaderInit(cls,row):
         return cls(id = row[cls.ormKeys["id"]],name=row[cls.ormKeys["name"]])
    
  
class Pos(Document,MongoShell.mongoInterface,AlquemyInterface):
    id              = IntField(primary_key=True)
    mapper          = ormV2.Pos
    ormKeys         = {"id":"SiteLocationID","name":"Name","latitude":"Latitude","longitude":"Longitude","code":"site_id"}
    latitude        = FloatField("Latitude")
    longitude       = FloatField("Longitude")
    name            = StringField("Name")
    code            = StringField("site_id")
    city            = ReferenceField("City")
    country         = ReferenceField("Country")
    zone            = ReferenceField("Zone")
    customer        = ReferenceField("Customer")
    salesIM         = ListField(ReferenceField("SaleIM"))
    salesAV         = ListField(ReferenceField("SaleAV"))
    salesDA         = ListField(ReferenceField("SaleDA"))
    
    collection      = MongoShell.posCollection 
    
    @classmethod
    def downloaderInit(cls,row):
         return cls(id = row[cls.ormKeys["id"]],name=row[cls.ormKeys["name"]])
    @classmethod    
    def updateAllOnes(cls):
        #cls.updateOne("country",Country)
        #cls.updateOne("city",City)
        #cls.updateOne("zone",Zone)
        cls.updateOne("customer",Customer)
        
        
class ProductGroup(Document,MongoShell.mongoInterface,AlquemyInterface):
    id              =  IntField(primary_key=True)
    mapper          =  ormV2.ProductGroup
    ormKeys         = {"id":"ProductGroupID","name":"productGroup"}
    name            =  StringField()
    products        =  ListField(ReferenceField("Product"))
    
    @classmethod
    def downloaderInit(cls,row):
         return cls(id = row[cls.ormKeys["id"]],name=row[cls.ormKeys["name"]])

    
class Sale(Document,MongoShell.mongoInterface):
    identifier      = IntField()
    div             = StringField()
    product         = ReferenceField("Product")
    pos             = ReferenceField("Pos")
    customer        = ReferenceField("Customer")
    country         = ReferenceField("Country")
    unitPrice       = IntField()
    quantity        = IntField()
    date            = DateTimeField()
    audited         = False
        
    
class SaleIM(Document, SaleInterface,MongoShell.mongoInterface,AlquemyInterface):
    id              = IntField(primary_key=True)
    mapper          = ormV2.SaleIM
    identifier      = IntField()
    div             = StringField()
    product         = ReferenceField("Product")
    pos             = ReferenceField("Pos")
    customer        = ReferenceField("Customer")
    country         = ReferenceField("Country")
    unitPrice       = IntField()
    quantity        = IntField()
    quantity_b      = IntField()
    date            = DateTimeField()
    audited         = False
    collection      = MongoShell.saleIMCollection 
    ormKeys         = {"id": "MarketSellOutIMID" ,"unitPrice":"UnitPrice","quantity":"SoldUnits","quantity_b":"Sale","date":"Created_Date"}
    
    
    @classmethod
    def downloaderInit(cls,row):
        return cls(id = row[cls.ormKeys["id"]],unitPrice=row[cls.ormKeys["unitPrice"]],quantity=row[cls.ormKeys["quantity"]],quantity_b = row[cls.ormKeys["quantity"]],date=row[cls.ormKeys["date"]],div="IM")
        

    @classmethod    
    def updateAllOnes(cls):
        cls.updateOne("product",Product)
        cls.updateOne("pos",Pos)
        cls.updateOne("customer",Customer)
            #[x.save() for x in sales]

class InventoryIM(Document,MongoShell.mongoInterface,AlquemyInterface,InvInterface):
    id              = IntField(primary_key=True)
    mapper          = ormV2.InventoryIM
    inventory       = IntField()
    ormKeys         = {"id": "MarketInventoryIMID" ,"inventory":"Inventory","date":"Created_Date"}
    product         = ReferenceField("Product")
    pos             = ReferenceField("Pos")
    customer        = ReferenceField("Customer")
    country         = ReferenceField("Country")
    date            = DateTimeField()
    div             = StringField()
    
    meta            ={"collection":"inventory_i_m"}
    collection      = MongoShell.inventoryIMCollection
    
    @classmethod
    def downloaderInit(cls,row):
        return cls(id = row[cls.ormKeys["id"]],inventory=row[cls.ormKeys["inventory"]],date=row[cls.ormKeys["date"]],div="IM")
    
    @classmethod    
    def updateAllOnes(cls):
        cls.updateOne("product",Product)
        cls.updateOne("pos",Pos)
        cls.updateOne("customer",Customer)
    

#class saleAVObjectManager(QuerySet):
#    
#    gef getBetweenDates()
#                
    
class SaleAV(Document, SaleInterface,AlquemyInterface):
    id              = IntField(primary_key=True)
    mapper          = ormV2.SaleAV
    ormKeys         = {"id": "MarketSellOutAVID" ,"unitPrice":"UnitPrice","quantity":"SoldUnits","quantity_b":"Sale","date":"Created_Date"}
    product         = ReferenceField("Product")
    div             = StringField()
    pos             = ReferenceField("Pos")
    customer        = ReferenceField("Customer")
    country         = ReferenceField("Country")
    unitPrice       = IntField()
    quantity        = IntField()
    quantity_b      = IntField()
    date            = DateTimeField()
    
    def moveToCollection(self):
        super().moveToCollection(self.__class__)
        
    @classmethod
    def downloaderInit(cls,row):
        return cls(id = row[cls.ormKeys["id"]],unitPrice=row[cls.ormKeys["unitPrice"]],quantity=row[cls.ormKeys["quantity"]],quantity_b = row[cls.ormKeys["quantity"]],date=row[cls.ormKeys["date"]],div="AV")
    
    
    @classmethod          
    def updateAllOnes(cls):
        cls.updateOne("product",Product)
        cls.updateOne("pos",Pos)
        cls.updateOne("customer",Customer)
        
class InventoryAV(Document,MongoShell.mongoInterface,AlquemyInterface,InvInterface):
    id              = IntField(primary_key=True)
    mapper          = ormV2.InventoryAV
    inventory       = IntField()
    ormKeys         = {"id": "MarketInventoryAVID" ,"inventory":"Inventory","date":"Created_Date"}
    product         = ReferenceField("Product")
    pos             = ReferenceField("Pos")
    customer        = ReferenceField("Customer")
    country         = ReferenceField("Country")
    date            = DateTimeField()
    div             = StringField()
    
    meta            ={"collection":"inventory_a_v"}
    collection      = MongoShell.inventoryAVCollection
    
    @classmethod
    def downloaderInit(cls,row):
        return cls(id = row[cls.ormKeys["id"]],inventory=row[cls.ormKeys["inventory"]],date=row[cls.ormKeys["date"]],div="AV")
    
    @classmethod    
    def updateAllOnes(cls):
        cls.updateOne("product",Product)
        cls.updateOne("pos",Pos)
        cls.updateOne("customer",Customer)

    
    
    
class SaleDA(Document, SaleInterface,AlquemyInterface):
    id              = IntField(primary_key=True)
    mapper          = ormV2.SaleDA
    ormKeys         = {"id": "MarketSellOutDAID" ,"unitPrice":"UnitPrice","quantity":"SoldUnits","quantity_b":"Sale","date":"Created_Date"}
    div             = StringField()
    product         = ReferenceField("Product")
    pos             = ReferenceField("Pos")
    customer        = ReferenceField("Customer")
    country         = ReferenceField("Country")
    unitPrice       = IntField()
    unitPrice_b     = IntField()
    quantity        = IntField()
    quantity_b      = IntField()
    date            = DateTimeField()
   
    @classmethod
    def downloaderInit(cls,row):
        return cls(id = row[cls.ormKeys["id"]],unitPrice=row[cls.ormKeys["unitPrice"]],quantity=row[cls.ormKeys["quantity"]],quantity_b = row[cls.ormKeys["quantity"]],date=row[cls.ormKeys["date"]],div="DA")
    
    def moveToCollection(self):
        super().moveToCollection(self.__class__)
    
    
    @classmethod          
    def updateAllOnes(cls):
        cls.updateOne("product",Product)
        cls.updateOne("pos",Pos)
        cls.updateOne("customer",Customer)
        
        
class InventoryDA(Document,MongoShell.mongoInterface,AlquemyInterface,InvInterface):
    id              = IntField(primary_key=True)
    mapper          = ormV2.InventoryDA
    inventory       = IntField()
    ormKeys         = {"id": "MarketInventoryDAID" ,"inventory":"Inventory","date":"Created_Date"}
    product         = ReferenceField("Product")
    pos             = ReferenceField("Pos")
    customer        = ReferenceField("Customer")
    country         = ReferenceField("Country")
    date            = DateTimeField()
    div             = StringField()
    
    meta            ={"collection":"inventory_d_a"}
    collection      = MongoShell.inventoryDACollection
    
    @classmethod
    def downloaderInit(cls,row):
        return cls(id = row[cls.ormKeys["id"]],inventory=row[cls.ormKeys["inventory"]],date=row[cls.ormKeys["date"]],div="DA")
    
    @classmethod    
    def updateAllOnes(cls):
        cls.updateOne("product",Product)
        cls.updateOne("pos",Pos)
        cls.updateOne("customer",Customer)

#Division.migrateDivision(divisions)
        
class MongoEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, BaseDocument):
            data = o.to_mongo()
            # might not be present if EmbeddedDocument
            o_id = data.pop('_id', None)
            if o_id:
                data['id'] = str(o_id['$oid'])
            data.pop('_cls', None)
            return data
        else:
            return JSONEncoder.default(self, o)

#SaleIM.migrate()

def updateTables():
    try:
        #Division.migrate()
        objects = [Product, Country, City, Customer, Brand, Pos, Spec,SpecType, SaleAV , SaleDA, SaleIM]
        [x.migrate() for x in objects]
    except():
        traceback.print_exc()
        
def updateOne():
    objects = [Product, SpecType, City, Pos, SaleIM, SaleAV, SaleDA]
    [x.updateAllOnes() for x in objects]
    
def updateMany():
    Product.updateMany()
        
#updateTables()
#updateOne()
#updateMany()

            
 
#Get json from all IMSale objects
def updateTable():    

    json_data = SaleDA.objects().to_json()
    dicts = json.loads(json_data)
    for x in dicts:
        x["date"] = x["date"]["$date"] 
        
    for x in dicts:
        if isinstance(x["date"],int):
            if x["date"] != None:
                date = x["date"]
                date = str(date)
                date = date[:10]
                print(date)
                date = int(date)
                x["date"] =  datetime.datetime.utcfromtimestamp(date)
            else:
                continue
    return dicts


##Create join

#updateTables()
#updateOne()
    
#Product.migrate() 
#SpecType.migrate() 
#City.migrate() 
#Pos.migrate() 
#Zone.migrate()
#Customer.migrate()    
#SaleIM.migrate() 
#SaleAV.migrate() 
#SaleDA.migrate()
#ProductGroup.migrate()
#InventoryIM.migrate()
#InventoryAV.migrate()
#InventoryDA.migrate()
#Price.migrate()
    
#Product.updateAllOnes() 
#SpecType.updateAllOnes() 
#City.updateAllOnes() 
#Pos.updateAllOnes()
#SaleIM.updateAllOnes()
#SaleAV.updateAllOnes()
#SaleDA.updateAllOnes()
#SaleIM.updateAllOnes()
#InventoryAV.updateAllOnes()
#InventoryDA.updateAllOnes()  
#Price.updateAllOnes()

#combinePrice = Price.combineFrames()


#combinedDA = SaleDA.combineFrames()
#combinedAV = SaleAV.combineFrames()
#combinedIM = SaleIM.combineFrames()
##
#combinedDAI = InventoryDA.combineFrames()
#combinedAVI = InventoryAV.combineFrames()
#combinedIMI = InventoryIM.combineFrames()
#combined = pd.concat([combinedDAI, combinedAVI, combinedIMI])  
#dicts = combined.to_dict("records")

#combined = pd.concat([combinedDA, combinedAV, combinedIM])
#print(combined.columns)
#dicts = combined.to_dict("records")
#print(type(dicts))
#sample = dicts[0]




#SaleDA.updater({"date":None})
#dicts = updateTable()
#SaleAV.migrate()
#SaleAV.updateOne()
#SaleDA.updateSales() 
#salesMSO = ormV2.getObjects(ormV2.SaleIM)



# for index, row in df.iterrows():
#     print(index)
#     obj = Brand(name = row["BrandName"], id=row["BrandID"])
#     mongoObj.append(obj)

# Brand.objects.insert(mongoObj)


# objSample = mongoObj[1]
# print(objSample.name)

# spec=Spec.objects(id = 2).first()
# print(spec.name)


    
# convert rows as a collection of dictionaries
#
mainView = Price.getView()
product = Product.getView()
country = Country.getView()
pos     = Pos.getView()
customer = Customer.getView()
zone     = Zone.getView()
city = City.getView()
brand = Brand.getView()
productGroup = ProductGroup.getView()



join1 = pd.merge(mainView,product[["_id","model","brand","productGroup"]],left_on=["product"],right_on=["_id"])
join1 = join1.drop(columns=["product","brand_y"])
join1 = join1.rename(columns = {"_id_y":"product_id","_id_x":"identifier","brand_x":"brand"})
join2 = pd.merge(join1,brand[["_id","name"]], left_on=["brand"],right_on=["_id"], how="left")
join2 = join2.drop(columns=["brand"])
join2 = join2.rename(columns = {"_id":"brand_id","name":"brand"})
join3 = pd.merge(join2,pos[["_id","city","code","country","zone","latitude","longitude","name"]],left_on=["pos"],right_on=["_id"],how="left")
join3 = join3.drop(columns=["pos"])
join3 = join3.rename(columns = {"_id":"pos_id","name":"pos"})
join4 = pd.merge(join3,city[["_id","name"]],left_on=["city"],right_on=["_id"],how="left")
join4 = join4.drop(columns=["city"])
join4 = join4.rename(columns = {"_id":"city_id","name":"city"})
join5 = pd.merge(join4,country[["_id","name"]],left_on=["country"],right_on=["_id"],how="left")
join5 = join5.drop(columns=["country"])
join5 = join5.rename(columns = {"_id":"country_id","name":"country"})
#join6 = pd.merge(join5,customer[["_id","name"]],left_on=["customer"],right_on=["_id"],how="left")
#join6 = join6.drop(columns=["customer"])
#join6 = join6.rename(columns = {"_id":"customer_id","name":"customer"})
#join7 = pd.merge(join6,zone[["_id","name"]],left_on=["zone"],right_on=["_id"],how="left")
#join7 = join7.drop(columns=["zone"])
#join7 = join7.rename(columns = {"_id":"zone_id","name":"zone"})
#join8 = pd.merge(join7,productGroup[["_id","name"]],left_on=["productGroup"],right_on=["_id"],how="left")
#join8 = join8.drop(columns=["productGroup"])
#join8 = join8.rename(columns = {"_id":"productGroup_id","name":"productGroup"})

        

    

 
    
    
