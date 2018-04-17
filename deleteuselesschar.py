import MySQLdb
import re
import jieba
import time
from multiprocessing import Pool,Manager
import math
def stopwordsdelete(stopwordsetence):#停用词进行剔除
    stopwords = {}.fromkeys(['RT', '-', '#', '', ';', '…', ':', '@',"'d","'s"])
    segs = jieba.cut(stopwordsetence, cut_all=False)
    final = ''
    for seg in segs:
        if seg not in stopwords:
            final += seg
    return final
def stopwordstest(q,index):
    Process_id='Process-'+str(index)
    conn2=MySQLdb.connect(host='localhost',user='root',password='root',db='test',charset='utf8')
    cursor2=conn2.cursor()
    while not q.empty():
        list=q.get()
        #print(list)
        twitter_id=list[0]
        matchingstr=list[1]
        m_findall = re.findall('https://t.co/[A-Za-z0-9]+',matchingstr )
        pstring=''
        if m_findall:
            for eachone in m_findall:
                eachstring = matchingstr.replace(eachone, '')
                matchingstr = eachstring
                pstring=stopwordsdelete(matchingstr)
            cursor2.execute("INSERT INTO outcome6(twitter_id,text) VALUES (%s,%s)",(twitter_id,pstring))
            print(Process_id,q.qsize())
                        #print(pstring)
        else:
            pstring = stopwordsdelete(matchingstr)
            cursor2.execute("INSERT INTO outcome6(twitter_id,text) VALUES (%s,%s)", (twitter_id,pstring ))
            print(Process_id, q.qsize())
                    #print(pstring)
    cursor2.close()
    conn2.commit()
    conn2.close()
if __name__=='__main__':
    start = time.time()

    # conn1 = MySQLdb.connect(host='localhost', user='root', password='root', db='twittersql', charset='utf8')
    # cursor1 = conn1.cursor()
    #
    #     # 总共多少数据
    # sql1 = 'select  *  from tweet_english' + ';'
    # allData = cursor1.execute(sql1)
    allData=3000000
        # 每个批次多少条数据
    dataOfEach =200000
        # 批次
    batch = math.ceil(allData / dataOfEach)

    BATctrl = 0
    # cursor1.close()
    # conn1.commit()
    # conn1.close()

    while BATctrl < batch:
        conn3 = MySQLdb.connect(host='localhost', user='root', password='root', db='twittersql', charset='utf8')
        cursor3 = conn3.cursor()
        sql = 'select tweet_id,text from tweet_english limit ' + str(dataOfEach * BATctrl) + ',' + str(dataOfEach) + ';'
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
            pool.apply(stopwordstest, args=(workQueue, i))
        print("Started processes")
        pool.close()
        pool.join()
        cursor3.close()
        conn3.commit()
        conn3.close()



    end = time.time()
    print('Pool + Queue多进程爬虫的总时间为：', end - start)
    print('Main process Ended')