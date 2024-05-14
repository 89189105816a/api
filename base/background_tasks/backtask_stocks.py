

from service.service import SQLAlchemyCoreMethods
from celery import Celery
from conf import BROKER_ENGINE, BROKER_ADDRESS, BROKER_USER, BROKER_PORT

app = Celery('background-tasks', broker=f"{BROKER_ENGINE}://{BROKER_USER}@{BROKER_ADDRESS}:{BROKER_PORT}")


@app.task
def calculate_stocks_for_bi():
    result = SQLAlchemyCoreMethods.stocks_calculations()
    return result
