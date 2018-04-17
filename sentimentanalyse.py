from textblob import TextBlob
import MySQLdb
import re
import jieba
import time
import math
from multiprocessing import Pool,Manager
def setiment(q,index):
    conn = MySQLdb.connect(host='localhost', user='root', password='root', db='twittersql', charset='utf8')
    cursor = conn.cursor()
    Process_id = 'Process-' + str(index)
    while not q.empty():
        list = q.get()
        tweet_id=list[0]
        eachresult = list[1]
        test=TextBlob(eachresult)
        polarity=test.sentiment.polarity
        subjectivity=test.sentiment.subjectivity
        cursor.execute("INSERT INTO sentimentcome(tweet_id,polarity,subjectivity) VALUES (%s,%s,%s)",(tweet_id,polarity,subjectivity))
        print(Process_id, q.qsize())
    cursor.close()
    conn.commit()
    conn.close()
if __name__ == '__main__':
    start=time.time()
    allData = 6612279
    # 每个批次多少条数据
    dataOfEach = 661300
    # 批次
    batch = math.ceil(allData / dataOfEach)

    BATctrl = 0
    while BATctrl < batch:
        conn3 = MySQLdb.connect(host='localhost', user='root', password='root', db='twittersql', charset='utf8')
        cursor3 = conn3.cursor()
        sql = 'select tweet_id,text from outcome_stopword limit ' + str(dataOfEach * BATctrl) + ',' + str(dataOfEach) + ';'
        cursor3.execute(sql)
        print('select:' + str(dataOfEach))
        results = cursor3.fetchall()
        results = list(results)
        BATctrl += 1

        manager = Manager()
        workQueue = manager.Queue(20000)
        for result in results:
            workQueue.put(result)
        pool = Pool()
        for i in range(4):
            pool.apply(setiment, args=(workQueue, i))
        print("Started processes")
        pool.close()
        pool.join()
        cursor3.close()
        conn3.commit()
        conn3.close()

    end = time.time()
    print('Pool + Queue多进程爬虫的总时间为：', end - start)
    print('Main process Ended')