import config
from pymongo import MongoClient
from SendMail import sedMessage
from bson.son import SON
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
import shutil
import datetime,time
from apscheduler.schedulers.blocking import BlockingScheduler
from pyecharts import Line, Overlap, Page

host = config.data_host
port = config.data_port
minutes = config.minutes


conn = MongoClient(host, port)
#  链接数据库
# db = conn.z3dbus
db = conn.mydata
table = db["Z3_MIN_REALTIME"]

"""
    "current_px": 当前价, 
    "avg_px": 均价,(很少可能为0)
    --------------> 以上数据如果为0则为错误数据
    "volumn": 成交量,
    "amount": 成交额,
    "chg": 涨跌,
    "chg_pct": 涨跌幅
"""

def find_error_data(min_time):
    index_one = minutes.index(min_time)
    next_time = minutes[index_one-1]
    # 当前时间数据
    pipeline2 = [
        {"$unwind": "$data"},
        {"$match": {"$or": [{"data.trade_min": min_time}]}},
        {"$sort": SON([("_id", 1)])}
    ]

    # 当前 前一分钟时间
    pipeline3 = [
        {"$unwind": "$data"},
        {"$match": {"$or": [{"data.trade_min": next_time, "type": 0}]}},
        {"$sort": SON([("_id", 1)])}
    ]
    data2 = table.aggregate(pipeline2)
    list_data2 = [{"_id": item["_id"], "name":item["name"], "avg_px":item["data"]["avg_px"], "current_px":item["data"]["current_px"],
                   "type":item["type"]}for item in data2]
    df2 = pd.DataFrame(list_data2)
    error_data = {}
    # 找出name为空的股票
    name_data = df2[df2["name"] == ""]
    null_name = list(name_data["_id"].values)

    error_data["null_name"] = null_name
    # 找出时间点为0的股票
    df3 = df2[df2["name"] != ""]
    a_list = df3["_id"].values
    zero_time = list(set(ids) ^ set(a_list))

    error_data["zero_time_data"] = zero_time

    # 对指数判断 指数的均价都是0 当前价不可以为0
    index_zero_data = df2[(df2["current_px"] == 0) & (type == 9)]
    index_data = list(index_zero_data["_id"].values)
    # error_data.extend(index_data)
    error_data["index_data"] = index_data
    # 对股票作判断
    # 找出时间点不为0 但 均价或者当前价 为0的数据
    b_data = df2[((df2["avg_px"] == 0) | (df2["current_px"] == 0)) & (df2["type"] == 0)]
    stk_data = list(b_data["_id"].values)
    # error_data.extend(stk_data)
    error_data["stk_data"] = stk_data
    """
    找出波动比较大的股票
    """
    if min_time > 930:
        data3 = table.aggregate(pipeline3)
        list_data3 = [{"_id": item["_id"], "avg_px":item["data"]["avg_px"], "current_px":item["data"]["current_px"],
                       "type": item["type"]}for item in data3 if item["_id"] not in error_data]
        df3 = pd.DataFrame(list_data3)
        df = pd.merge(df2, df3, on="_id")
        df["chg"] = (abs((df["current_px_x"]-df["current_px_y"])/df["current_px_x"])*100)
        df4 = df[df["chg"] > 2]
        # 波动较大的股票
        e_list = list(df4["_id"].values)
        #error_data.extend(e_list)
        error_data["vol_data"] = e_list
    return error_data



def showCharts(innercodes,last_time):
    # 先删除文件
    # shutil.rmtree("picture")
    # 在创建文件
    # os.mkdir("picture")
    page = Page()
    # 最多生成三张图片
    codes = innercodes[:3] if len(innercodes) > 3 else innercodes
    limit = minutes.index(last_time)+1
    mins = [str(x)[:-2] + ":" + str(x)[-2:] for x in minutes[:limit]]
    for code in codes:
        print(code)
        pipeline4 = [
            {"$unwind": "$data"},
            {"$match": {"_id": code}},
            {"$limit": limit}
        ]
        stks_data = table.aggregate(pipeline4)
        price = []
        avg_px = []
        chg_pct = []
        for stk in stks_data:
            price.append(round(stk["data"]["current_px"], 2))
            avg_px.append(round(stk["data"]["avg_px"], 2))
            chg_pct.append(stk["data"]["chg_pct"])

        last_value = min(price+avg_px)

        line = Line(code)
        line.add("cur_price", mins, price, yaxis_min=last_value, tooltip_axispointer_type='cross')
        line.add("avg_price", mins, avg_px, yaxis_min=last_value, tooltip_axispointer_type='cross')

        line1 = Line()
        line1.add("chg_pct", mins, chg_pct, tooltip_axispointer_type='cross')

        overlap = Overlap()
        overlap.add(line)
        overlap.add(line1, yaxis_index=1, is_add_yaxis=True)
        page.add(line)
    page.render()
        # plt.figure()
        # plt.plot(mins, price, label="cur_price")
        # plt.plot(mins, avg_px, label="avg_price")
        # # plt.savefig("./picture/%i.jpg" % i)
        # i += 1
        # plt.legend()
        # plt.show()

def my_job():
    now = datetime.datetime.now()
    print("定时任务执行时间"+str(now))
    minute = str(now.time().minute) if now.time().minute >= 10 else "0"+str(now.time().minute)
    now_time = int(str(now.time().hour)+minute)

    if now_time in minutes:
        error_data = find_error_data(now_time)
        send_data = []
        for data in error_data.values():
            send_data.extend(data)
        name_data = error_data["null_name"]
        if(len(send_data)>0):
            message = "null_name:代表股票名称为空的股票\n" \
                      "index_data：代表指数的当前价为0的数据\n" \
                      "zero_time_data:代表有股票的时间为0的数据\n" \
                      "stk_data:代表股票的均价或者当前价为0的数据\n" \
                      "vol_data:代表俩秒之间波动大于2%的数据\n" \
                      "========================================\n"
            message += str(now_time)+"有问题的数据:\n"
            for k in error_data:
                message += k + ":" + ",".join(error_data[k]) + "\n"
            print(message)
            # 名称为null的股票 不画图
            charts_data = list(set(send_data) ^ set(name_data))
            print(charts_data)
            if len(charts_data) > 0:
                showCharts(charts_data, now_time)
            sedMessage(message+u"查看图示文件请复制此路径:D:\\PythonMatch\\render.html")
    else:
        print(str(now_time)+"闭市时间")

# 获取当天没有停牌的股票
def find_stocks_ids():
    global ids
    names = table.find({"name": {"$ne": ""}}, {"name": 1}).sort("_id")
    ids = [name["_id"] for name in names]

if __name__ == '__main__':
    find_stocks_ids()
    scheduler = BlockingScheduler(daemonic=False)
    # hour是闭区间
    scheduler.add_job(my_job, 'cron', hour='9-11,13-16', second="30")
    try:
        scheduler.start()
    except(KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("Stop Job!")
