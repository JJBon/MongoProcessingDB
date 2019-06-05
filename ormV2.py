import pandas as pd
import numpy as numpy
#import types
import sys


from sqlalchemy import insert, create_engine,Table, MetaData,select, Column,Integer,String,ForeignKey,Float,Date,extract,update,DateTime
from sqlalchemy.sql import null
from sqlalchemy.orm import sessionmaker,relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect , and_


from abc import ABC, abstractmethod, ABCMeta


engine = create_engine("mysql://TableauUser:T4bl34uP4ssw0rd$@127.0.0.1:22/BD_MSO_Prod?charset=utf8")

Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

association_p_t = Table("ProductSpecTypes",Base.metadata, 
    Column("ProductID",Integer,ForeignKey("Products.ProductID")),
    Column("SpecTypeID",Integer,ForeignKey("SpecTypes.SpecTypeID")),
)

                
def getObjects(object):
    return session.query(object)

class Division(Base):
    __tablename__   = "Categories"
    id              = Column("CategoryID",primary_key=True)
    name            = Column("category",String)
    products        = relationship("Product",back_populates="division")

    

class Product(Base):
    __tablename__   = "Products"
    id              = Column('ProductID',primary_key=True)
    code            = Column('CodEAN',String)
    model           = Column('Model',String)
    brand           = relationship("Brand",back_populates="products")
    brand_id        = Column("BrandID",Integer,ForeignKey("Brands.BrandID"))
    #specs           = relationship("Spec",secondary=association_p_t,back_populates="products")
    types           = relationship("SpecType",back_populates="product")
    division_id     = Column("CategoryID",ForeignKey("Categories.CategoryID"))
    division         = relationship("Division",back_populates="products")
    productGroup_id  = Column("ProductGroupID",ForeignKey("ProductGroups.ProductGroupID"))
    prices          = relationship("Price",back_populates="product")
    
    
    salesIM         = relationship("SaleIM",back_populates="product")
    inventoryIM     = relationship("InventoryIM",back_populates="product")
    
    
    salesAV         = relationship("SaleAV",back_populates="product")
    inventoryAV     = relationship("InventoryAV",back_populates="product")
    
    salesDA         = relationship("SaleDA",back_populates="product")
    inventoryDA     = relationship("InventoryDA",back_populates="product")
    
    
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
        
        
    
    def getSpecs(self):
        return [(x.name,x.spec.name) for x in self.types]
    
class SaleInterface():
    
    #@abstractmethod
    
    
    def getAmount(self):
        return self.unitPrice * self.quantity
    
    def varWarning(self):
        pass
    
    


    
    
    
        
class SaleIM(Base, SaleInterface):
    __tablename__   = "MarketSellOutIM"
    id              = Column("MarketSellOutIMID",Integer,primary_key=True)
    product_id      = Column("ProductID",Integer,ForeignKey("Products.ProductID"))
    product         = relationship("Product",back_populates="salesIM")
    pos_id          = Column("SiteLocationID",ForeignKey("SiteLocations.SiteLocationID"))
    pos             = relationship("Pos",back_populates="salesIM")
    customer_id     = Column("SiteStoreID",ForeignKey("SiteStores.SiteStoreID"))
    customer        = relationship("Customer",back_populates="salesIM")
    country_id      = Column("CountryID",ForeignKey("Countries.CountryID"))
    country         = relationship("Country",back_populates="salesIM")
    unitPrice       = Column("UnitPrice",Integer)
    quantity_a      = Column("Sale",Integer)
    quantity        = Column("SoldUnits",Integer)
    date            = Column("Created_Date",DateTime)
    
    
class InventoryIM(Base):
    __tablename__   = "MarketInventoryIM"
    id              = Column("MarketInventoryIMID", Integer, primary_key=True)
    product_id      = Column("ProductID",Integer,ForeignKey("Products.ProductID"))
    product         = relationship("Product",back_populates="inventoryIM")
    pos_id          = Column("SiteLocationID",ForeignKey("SiteLocations.SiteLocationID"))
    pos             = relationship("Pos",back_populates="inventoryIM")
    customer_id     = Column("SiteStoreID",ForeignKey("SiteStores.SiteStoreID"))
    customer        = relationship("Customer",back_populates="inventoryIM")
    country_id      = Column("CountryID",ForeignKey("Countries.CountryID"))
    country         = relationship("Country",back_populates="inventoryIM")
    date            = Column("Created_Date",DateTime)
    inventory       = Column("Inventory",Integer)
    
    
    
class SaleAV(Base, SaleInterface):
    __tablename__   = "MarketSellOutAV"
    id              = Column("MarketSellOutAVID",primary_key=True)
    product_id      = Column("ProductID",ForeignKey("Products.ProductID"))
    product         = relationship("Product",back_populates="salesAV")
    pos_id          = Column("SiteLocationID",ForeignKey("SiteLocations.SiteLocationID"))
    pos             = relationship("Pos",back_populates="salesAV")
    customer_id     = Column("SiteStoreID",ForeignKey("SiteStores.SiteStoreID"))
    customer        = relationship("Customer",back_populates="salesAV")
    country_id      = Column("CountryID",ForeignKey("Countries.CountryID"))
    country         = relationship("Country",back_populates="salesAV")
    unitPrice       = Column("UnitPrice",Integer)
    quantity        = Column("SoldUnits",Integer)
    quantity_a      = Column("Sale",Integer)
    date            = Column("Created_Date",DateTime)
    
    
class InventoryAV(Base):
    __tablename__   = "MarketInventoryAV"
    id              = Column("MarketInventoryAVID", Integer, primary_key=True)
    product_id      = Column("ProductID",Integer,ForeignKey("Products.ProductID"))
    product         = relationship("Product",back_populates="inventoryAV")
    pos_id          = Column("SiteLocationID",ForeignKey("SiteLocations.SiteLocationID"))
    pos             = relationship("Pos",back_populates="inventoryAV")
    customer_id     = Column("SiteStoreID",ForeignKey("SiteStores.SiteStoreID"))
    customer        = relationship("Customer",back_populates="inventoryAV")
    country_id      = Column("CountryID",ForeignKey("Countries.CountryID"))
    country         = relationship("Country",back_populates="inventoryAV")
    date            = Column("Created_Date",DateTime)
    inventory       = Column("Inventory",Integer)
    
class SaleDA(Base, SaleInterface):
    __tablename__   = "MarketSellOutDA"
    id              = Column("MarketSellOutDAID",primary_key=True)
    product_id      = Column("ProductID",ForeignKey("Products.ProductID"))
    product         = relationship("Product",back_populates="salesDA")
    pos_id          = Column("SiteLocationID",ForeignKey("SiteLocations.SiteLocationID"))
    pos             = relationship("Pos",back_populates="salesDA")
    customer_id     = Column("SiteStoreID",ForeignKey("SiteStores.SiteStoreID"))
    customer        = relationship("Customer",back_populates="salesDA")
    country_id      = Column("CountryID",ForeignKey("Countries.CountryID"))
    country         = relationship("Country",back_populates="salesDA")
    unitPrice       = Column("UnitPrice",Integer)
    quantity        = Column("SoldUnits",Integer)
    quantity_a      = Column("Sale",Integer)
    date            = Column("Created_Date",DateTime)
    
    
class InventoryDA(Base):
    __tablename__   = "MarketInventoryDA"
    id              = Column("MarketInventoryDAID", Integer, primary_key=True)
    product_id      = Column("ProductID",Integer,ForeignKey("Products.ProductID"))
    product         = relationship("Product",back_populates="inventoryDA")
    pos_id          = Column("SiteLocationID",ForeignKey("SiteLocations.SiteLocationID"))
    pos             = relationship("Pos",back_populates="inventoryDA")
    customer_id     = Column("SiteStoreID",ForeignKey("SiteStores.SiteStoreID"))
    customer        = relationship("Customer",back_populates="inventoryDA")
    country_id      = Column("CountryID",ForeignKey("Countries.CountryID"))
    country         = relationship("Country",back_populates="inventoryDA")
    date            = Column("Created_Date",DateTime)
    inventory       = Column("Inventory",Integer)
    
     

class Brand(Base):
    __tablename__   = "Brands"
    id              = Column("BrandID",primary_key=True)
    name            = Column("BrandName",String)
    products        = relationship("Product",back_populates="brand")
    prices          = relationship("Price",back_populates="brand")
    
class Price(Base):
    __tablename__   = "PriceEvidence"
    id              = Column("PriceEvidenceID",Integer,primary_key=True)
    offerPrice      = Column("OfferPrice",Integer)
    regularPrice    = Column("RegularPrice",Integer)
    cardPrice       = Column("CardPrice",Integer)
    weekPrice       = Column("WeekPrice",Integer)
    brand_id        = Column("BrandID",Integer, ForeignKey("Brands.BrandID"))
    brand           = relationship("Brand",back_populates="prices")
    product_id      = Column("ProductID",Integer, ForeignKey("Products.ProductID"))
    product         = relationship("Product",back_populates="prices")
    pos_id          = Column("SiteLocationID",Integer,ForeignKey("SiteLocations.SiteLocationID"))
    pos             = relationship("Pos",back_populates="prices")
    customer_id     = Column("SiteStoreID",Integer,ForeignKey("SiteStores.SiteStoreID"))
    customer        = relationship("Customer",back_populates="prices")
    
    date            = Column("Created_Date",DateTime)
    

class Customer(Base):
    __tablename__   = "SiteStores"
    id              = Column('SiteStoreID',primary_key=True)
    name            = Column('Name',String)
    cities          = relationship("City",back_populates="customers")
    city_id         = Column("CityID",Integer,ForeignKey("Cities.CityID"))
    country         = relationship("Country",back_populates="customers")
    country_id      = Column("CountryID",Integer,ForeignKey("Countries.CountryID"))
    zone            = relationship("Zone",back_populates="customers")
    zone_id         = Column("CountryZoneID",Integer,ForeignKey("CountryZones.CountryZoneID"))
    
    prices          = relationship("Price",back_populates="customer")
    stores          = relationship("Pos",back_populates="customer")
    
    salesIM         = relationship("SaleIM",back_populates="customer")
    salesAV         = relationship("SaleAV",back_populates="customer")
    salesDA         = relationship("SaleDA",back_populates="customer")
    
    
    inventoryIM     = relationship("InventoryIM",back_populates="customer")
    inventoryAV     = relationship("InventoryAV",back_populates="customer")
    inventoryDA     = relationship("InventoryDA",back_populates="customer")
    
class Pos(Base):
    __tablename__   = "SiteLocations"
    id              = Column("SiteLocationID",primary_key=True)
    latitude        = Column("Latitude",Float)
    longitude       = Column("Longitude",Float)
    name            = Column("Name",String)
    code            = Column("site_id",String)
    city_id         = Column("CityID", ForeignKey("Cities.CityID"))
    city            = relationship("City",back_populates="stores")
    country_id      = Column("CountryID", ForeignKey("Countries.CountryID"))
    country         = relationship("Country",back_populates="stores")
    zone_id         = Column("CountryZoneID",ForeignKey("CountryZones.CountryZoneID"))
    zone            = relationship("Zone",back_populates="stores")
    customer_id     = Column("SiteStoreID",ForeignKey("SiteStores.SiteStoreID"))
    customer        = relationship("Customer",back_populates="stores")
    
    
    prices          = relationship("Price",back_populates="pos")
    salesIM         = relationship("SaleIM",back_populates="pos")
    salesAV         = relationship("SaleAV",back_populates="pos")
    salesDA         = relationship("SaleDA",back_populates="pos")
    
    inventoryIM     = relationship("InventoryIM",back_populates="pos")
    inventoryAV     = relationship("InventoryAV",back_populates="pos")
    inventoryDA     = relationship("InventoryDA",back_populates="pos")
    
    

class City(Base):
    __tablename__   = "Cities"
    __table_args__ = {'extend_existing': True} 
    id              = Column("CityID",primary_key=True)
    name            = Column("CityName",String)
    country         = relationship("Country",back_populates="cities")
    country_id      = Column("CountryID",Integer,ForeignKey("Countries.CountryID"))
    customers       = relationship("Customer",back_populates="cities")
    stores          = relationship("Pos",back_populates="city")


class Country(Base):
    __tablename__   = "Countries"
    id              = Column("CountryID",primary_key=True)
    name            = Column("Name",String)
    customers       = relationship("Customer",back_populates="country")
    #zones           = relationship("Zone",back_populates="country")
    cities          = relationship("City",back_populates="country")
    stores          = relationship("Pos",back_populates="country")
    salesIM         = relationship("SaleIM",back_populates="country")
    salesAV         = relationship("SaleAV",back_populates="country")
    salesDA         = relationship("SaleDA",back_populates="country")
    
    inventoryIM     = relationship("InventoryIM",back_populates="country")
    inventoryAV     = relationship("InventoryAV",back_populates="country")
    inventoryDA     = relationship("InventoryDA",back_populates="country")
    

class Zone(Base):
    __tablename__   = "CountryZones"
    __table_args__ = {'extend_existing': True} 
    id              = Column("CountryZoneID",primary_key=True)
    name            = Column("ZoneName",String)
    #country         = relationship("Country",back_populates="zones")
    customers       = relationship("Customer",back_populates="zone")
    stores          = relationship("Pos",back_populates="zone")



class Spec(Base):
    __tablename__   = "SpecTypes"
    id              = Column("SpecTypeID",primary_key=True)
    name            = Column("specType",String)
    #products        = relationship("Product",secondary=association_p_t,back_populates="specs")
    

class SpecType(Base):
    __tablename__   = "ProductSpecTypes"
    __table_args__ = {'extend_existing': True} 
    id              = Column("ProductSpecTypeID",primary_key=True)
    name            = Column("specType",String)
    product         = relationship("Product",back_populates="types")
    #ProductID       = Column("ProductID",Integer,ForeignKey("Products.ProductID"))
    #specID          = Column("SpecTypeID",Integer,ForeignKey("SpecTypes.SpecTypeID"))
    spec            = relationship("Spec",backref="SpecTypes")
    
class ProductGroup(Base):
    __tablename__   = "ProductGroups"
    id              = Column("ProductGroupID",primary_key=True)
    name            = Column("productGroup",String)
   
    
    
    

    
#session.close()
#sales = session.query(Price)
#inventory  = sales[0]

#print(inventory.date)

#sales = session.query(SaleAV).filter(and_(SaleAV.date >= "2019-05-27" , SaleAV.date <= "2019-06-03" ))
#frame = pd.read_sql(sales.statement, sales.session.bind)
#
#stores = session.query(Pos)
#frameStores = pd.read_sql(stores.statement,stores.session.bind)
#
#products = session.query(Product)
#frameProducts = pd.read_sql(products.statement,products.session.bind)
#
#print(frame.columns)
#print(frameStores.columns)
#
#merge1 = pd.merge(frame, frameStores , on=["SiteLocationID"], how="left")
#merge2 = pd.merge(merge1,frameProducts , on=["ProductID"],how="left")
#print(sales.count())

#def getProducts():
#    print("inspecting")
#    print(type(Product))
#    insp = inspect(Product)
#    print(insp.columns)
#    print(list(insp.columns))
#    insp_b = inspect(Brand)
#    print(insp_b.columns)
#    print(list(insp_b.columns))
#    productos = session.query(Product)
# 
#    return productos
#try:
#    productos = getProducts()
#except:
#    print("db error")
#    print(sys.exc_info())
#    session.close()
# 
##########################Test de Clases
#
#print(type(productos))
#producto1 = productos[0]
#print(type(producto1))
#print(producto1.id)
#print(producto1.model)
#prodNames = [x.model for x in productos]
#specs = producto1.types
#namesId = [(x.name,x.spec.name) for x in specs]
#spec = specs[9]
#print(spec.name)
#typeNames = [(x.name,x.spec.name) for x in spec.types]
#
#print(type(productos))
#producto2 = productos[1000]
#print(type(producto2))
#print(producto2.brand.name)
#
#brands = session.query(Brand)
#brand = brands[2]
#products = brand.products
#brand_name = brand.name
#prod_names = [x.model for x in products]
#
#stores = session.query(Stores)
#print(customers.count())
#store = stores[100]
#store_name = store.name
#store_country = store.country.name
#store_city = store.city.name ## null presence
#
##Country
#
#countries = session.query(Country)
#country_names = [(x.name) for x in countries]
#
##Colombia Registered Pos
#
#colombia = countries[16]
#pos_colombia = [(x.name, x.code) for x in colombia.stores]
#
#products = session.query(Product).filter(Product.model.like("J2%"))
#products = session.query(Product).filter(Product.model == "GALAXY J2 CORE")
#print(products.count())
#product = products.first()
#sales = product.getCatSales2(country=colombia)
#salesData = [(x.pos.code, x.pos.name, x.quantity,x.unitPrice,x.getAmount()) for x in sales]
#specs = product.getSpecs()
#specs = product.types
#namesId = [(x.name,x.spec.name) for x in specs]
#
#typeNames = [(x.name,x.spec.name) for x in spec.types]
#
############Consolidado
#products = session.query(Product)
#sales = [x.getCatSales2(country=colombia) for x in product if isinstance(x,SaleInterface)]
#
#divisions = session.query(Division)











