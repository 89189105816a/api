from sqlalchemy import text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.automap import automap_base
from typing import Optional, Any, Dict
from base.background_tasks.database.database import DecBase, engine
import datetime

Base = DecBase
AutomapBase = automap_base()
AutomapBase.prepare(autoload_with=engine)


# will only work in python 3.9+
# intpk = Annotated[int, mapped_column(primary_key=True, autoincrement=True)]
# dateLoadDefault = Annotated[datetime.datetime, mapped_column(server_default=text("TIMEZONE('utc-3', now())"))]


# TODO sqlalchemy postgres dialect autoincrement not working with declaration below, fix please
class StocksCalculations(Base):
    __tablename__ = 'stocks_calculated'
    index: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    mpId: Mapped[Optional[int]]
    calculationDate: Mapped[datetime.datetime] = mapped_column(server_default=text("TIMEZONE('utc-3', now())"))
    warehouseName: Mapped[str]
    barcode: Mapped[str]
    userId: Mapped[int]
    supplierName: Mapped[str]
    costPriceN: Mapped[float]
    costPriceS: Mapped[float]
    stocksDate: Mapped[datetime.date]
    quantity: Mapped[int]
    m2: Mapped[float]
    priceS: Mapped[int]
    quantityToClient: Mapped[int]
    quantityFromClient: Mapped[int]
    m6: Mapped[float]
    m9: Mapped[float]
    saleCount: Mapped[int]
    sumSaleCount60Days: Mapped[int]


class ModelPass:
    pass


class ReflectedModels:
    nomenclatureOld = AutomapBase.classes.nomenclature_wb
    reportsOld = AutomapBase.classes.reportdetailbyperiod_wb_full


class OrmStocksWb(ModelPass):
    __tablename__ = 'orm_stocks_wb'
    index: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    mpId: Mapped[int]
    tableId: Mapped[int]
    warehouseName: Mapped[str]
    supplierArticle: Mapped[str]
    nmId: Mapped[int]
    barcode: Mapped[str]
    quantity: Mapped[int]
    inWayToClient: Mapped[int]
    inWayFromClient: Mapped[int]
    quantityFull: Mapped[int]
    category: Mapped[str]
    subject: Mapped[str]
    brand: Mapped[str]
    techSize: Mapped[str]
    price: Mapped[int]
    discount: Mapped[int]
    isSupply: Mapped[bool]
    isRealization: Mapped[bool]
    SCCode: Mapped[str]
    dateLoad: Mapped[datetime.datetime] = mapped_column(server_default=text("TIMEZONE('utc-3', now())"))
    lastChangeDate: Mapped[str]


class OrmStocksWbCalcs(ModelPass):
    __tablename__ = 'orm_stocks_wb_calcs'
    index: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    mpId: Mapped[int]
    warehouseName: Mapped[str]
    tableId: Mapped[int]
    date: Mapped[str]
    price: Mapped[int]
    discount: Mapped[int]
    calc_stocks_basic: Mapped[int]
    calc_stocks_basic_cost: Mapped[int]
    calc_sell_price_with_discount: Mapped[int]


class NomenclatureTableBase(ModelPass):
    __tablename__ = 'orm_nomenclature_wb_base'
    index: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    mpId: Mapped[int]
    nmId: Mapped[int]
    techSize: Mapped[str]
    photos: Mapped[Dict[str, Any]]
    characteristics: Mapped[Dict[str, Any]]
    updateAt: Mapped[str]
    createdAt: Mapped[str]
    vendorCode: Mapped[str]
    brand: Mapped[str]
    subjectName: Mapped[str]
    tableId: Mapped[int]











