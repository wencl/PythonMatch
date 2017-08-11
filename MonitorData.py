import config
from pymongo import MongoClient
from SendMail import sedMessage
import numpy as np
import matplotlib.pyplot as plt

host = config.data_host
port = config.data_port

conn = MongoClient(host, port)
db = conn.mydata
table = db["Z3_MIN_REALTIME"]
"""
    "current_px": 当前价, 
    "volumn": 成交量,
    "amount": 成交额,
    "avg_px": 均价,
    --------------> 以上数据如果为0则为错误数据
    "chg": 涨跌,
    "chg_pct": 涨跌幅
"""
def find_miss_data():
    pipeline = [
        {"$unwind": "$data"},
        {"$match": {"$or": [{"data.current_px": 0}, {"data.avg_px": 0}]}}
    ]
    data = table.aggregate(pipeline)
    set_data = set()
    for item in data:
        set_data.add(item["_id"])
    total = len(set_data)
    message = str(total)+"个:"+'\n'.join(set_data)+"股票数据有0值"
    sedMessage(message)
    print(str(total)+"个:"+','.join(set_data)+"股票数据有0值")

def find_error_data(time):
    pipeline2 = [
        {"$unwind": "$data"},
        {"$match": {"$or": [{"data.trade_min": time}, {"data.trade_min": time-1}]}}
    ]
    data = table.aggregate(pipeline2)
    pass

def showCharts():
    stksData = table.find({"_id": "300174.SZ"})
    for stkdata in stksData:
        mins = [stk["trade_min"] for stk in stkdata["data"]]
        price = [stk["current_px"] for stk in stkdata["data"]]
        chngPct = [stk["chg_pct"] for stk in stkdata["data"]]
        avg_px = [stk["avg_px"] for stk in stkdata["data"]]
        print(mins)
        print(price)
        plt.figure()
        plt.plot(mins, price)
        plt.plot(mins, avg_px)
        plt.savefig("./picture/300174.jpg")
        plt.show()


if __name__ == '__main__':
    find_miss_data()
    # showCharts()