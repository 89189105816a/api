

class InitialData:
    # todo надо все это вынести одельно в конфиг наверное..

    # configuration data
    reportdetailed_limit = 100000
    reportdetailed_rrid = 0

    wildberries_request_body = {
        "settings": {
            "cursor": {
                "limit": 1000
            },
            "filter": {
                "withPhoto": -1
            }
        }
    }
    wildberries_request_url = 'https://suppliers-api.wildberries.ru/content/v1/get/cards/list'
    wb_reportsdetailbyperiod_url = 'https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod'
    wb_orders_url = 'https://statistics-api.wildberries.ru/api/v1/supplier/orders'
    wb_stocks_url = 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks'
    wb_sales_url = 'https://statistics-api.wildberries.ru/api/v1/supplier/sales'
    proxy_manager_addr = 'http://127.0.0.1:8000/proxy_list/get_proxy'

