from multiprocessing import Pool, Manager
import time
import nltk
import nltk.data
import MySQLdb
import math
def nlptest(q, index):
    Process_id = 'Process-' + str(index)
    conn2 = MySQLdb.connect(host='localhost', user='root', password='root', db='test', charset='utf8')
    cursor2 = conn2.cursor()
    while not q.empty():
        list = q.get()
        print(list)
        twitter_id = list[0]
        allsetences = list[1]
        allsetences = allsetences.lower()
        setences = nltk.sent_tokenize(allsetences)
        output_list = ''
        for eachsetence in setences:
            text = nltk.word_tokenize(eachsetence)
            eachsentences = nltk.pos_tag(text)
            grammar = "NP:{<JJ|NN><NN>+}"
            cp = nltk.RegexpParser(grammar)  # 生成规则
            result = cp.parse(eachsentences)
            substring = []
            finalstring = ''
            for subtree in result.subtrees():
                if subtree.label() == 'NP':
                    substring.append(subtree)
            for each in substring:
                length = len(each)
                for i in range(0, length):
                    finalstring += each[i][0] + ' '
                finalstring += ','
            output = ''
            output += finalstring
            output_list += output  # 存放最后的分句
        list1 = output_list.split(',')
        n = len(list1)
        r = []
        for eachone in list1:
            if eachone != '':
                r.append(eachone)
        j = 0
        extra = {}
        for i in range(0, n - 1):  # dict storage
            x = r.count(r[i])
            extra[r[i]] = x
        for eachone in extra:  # eachone is the dict key
            cursor2.execute("INSERT INTO outcome(twitter_id,nounphrase,count) VALUES (%s,%s,%s)",
                            (twitter_id, eachone, extra[eachone]))
            print(Process_id, q.qsize())
    cursor2.close()
    conn2.commit()
    conn2.close()


if __name__ == '__main__':
    start = time.time()
    start = time.time()
    allData = 3000000#numbrs of twitter
    dataOfEach = 200000#number of every time analysized twitter
    batch = math.ceil(allData / dataOfEach)
    BATctrl = 0
    while BATctrl < batch:
        conn3 = MySQLdb.connect(host='localhost', user='root', password='root', db='first', charset='utf8')
        cursor3 = conn3.cursor()
        sql = 'select twitter_id,text from stage limit ' + str(dataOfEach * BATctrl) + ',' + str(dataOfEach) + ';'
        cursor3.execute(sql)
        print('select:' + str(dataOfEach))
        results = cursor1.fetchall()
        results = list(results)
        BATctrl += 1
        manager = Manager()
        workQueue = manager.Queue(20000)
        for result in results:
            workQueue.put(result)
        pool = Pool()
        for i in range(4):
            pool.apply(nlptest, args=(workQueue, i))
        print("Started processes")
        pool.close()
        pool.join()
        cursor3.close()
        conn3.commit()
        conn3.close()
    end = time.time()
    print('Pool + Queue多进程爬虫的总时间为：', end - start)
    print('Main process Ended')