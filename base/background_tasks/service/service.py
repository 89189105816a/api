from sqlalchemy import text

from base.background_tasks.database.database import session_factory
from base.background_tasks.models.models import ReflectedModels


class SyncMethodsORM:

    @staticmethod
    def select_data(*args, **kwargs):
        with session_factory() as session:
            print(ReflectedModels.nomenclatureOld)
            # query = select(ReflectedModels.nomenclatureOld)
            # result = session.execute(query)
            # result = result.scalars().fetchall()
            # return result

    @staticmethod
    def update_data():
        pass

    """
    
        with helper_query_1 as (
        select *, SUM(saleCount) over (partition by barcode, "warehouseName" order by stocksDate range between current row and '60 days' following) as sumSaleCount60Days
        FROM
        (select s."warehouseName",
             s.barcode,
             s."userId",
             s.name as supplierName,
             CAST(n."cost_price" as real) as costPriceN,
             CAST(s."cost_price" as real) as costPriceS,
             cast(LEFT(s."date", 10) as date) AS stocksDate,
             s.quantity,
             CAST(n."cost_price"::real * s.quantity as real) as m2,
             s."Price" as priceS,
             s."inWayToClient" as quantityToClient,
             s."inWayFromClient" as quantityFromClient,
             CAST(s."inWayToClient" * s."cost_price"::real as real) as m6,
             CAST((s."inWayToClient" + s."inWayFromClient" + s."quantity") * (s."costPrice"::real) as real)  as m9,
             (
                SELECT COUNT(*)
                FROM reportdetailbyperiod_wb_full r
                WHERE
                    r.barcode = s.barcode
                    AND r."office_name" = s."warehouseName"
                    AND LEFT(r.sale_dt, 10) = LEFT(s."date", 10)
                    and r."supplier_oper_name" = 'Продажа'
             ) AS saleCount
        from stocks_day_report_wb s
        left join nomenclature_wb n
        on s."nmId"::text = n."nmID"
        where s."user_id" = 1001
        order by stocksDate desc) helper_query 
        )
        select * from helper_query_1 
    """

    @staticmethod
    def calculate_stocks():
        pass


class SQLAlchemyCoreMethods:

    @staticmethod
    def stocks_calculations():
        with session_factory() as session:
            try:
                smt = """
                        insert into stocks_calculated
                        select *, SUM(saleCount) over (partition by barcode, "warehouseName" order by stocksDate range between current row and '60 days' following) as sumSaleCount60Days
                        FROM
                        (select s."warehouseName",
                             s.barcode,
                             s."user_id" as userId,
                             s.name as supplierName,
                             CAST(n."cost_price" as real) as costPriceN,
                             CAST(s."cost_price" as real) as costPriceS,
                             cast(LEFT(s."date", 10) as date) AS stocksDate,
                             s.quantity,
                             CAST(n."cost_price"::real * s.quantity as real) as m2,
                             s."Price" as priceS,
                             s."inWayToClient" as quantityToClient,
                             s."inWayFromClient" as quantityFromClient,
                             CAST(s."inWayToClient" * s."cost_price"::real as real) as m6,
                             CAST((s."inWayToClient" + s."inWayFromClient" + s."quantity") * (s."cost_price"::real) as real)  as m9,
                             (
                                SELECT COUNT(*)
                                FROM reportdetailbyperiod_wb_full r
                                WHERE
                                    r.barcode = s.barcode
                                    AND r."office_name" = s."warehouseName"
                                    AND LEFT(r.sale_dt, 10) = LEFT(s."date", 10)
                                    and r."supplier_oper_name" = 'Продажа'
                             ) AS saleCount
                        from stocks_day_report_wb s
                        left join nomenclature_wb n
                        on s."nmId"::text = n."nmID"
                        where s."user_id" = 1001
                        order by stocksDate desc) helper_query 
                    """
                session.execute(text("TRUNCATE stocks_calculated"))
                result = session.execute(text(smt))

            except Exception as e:
                return e

            return result




