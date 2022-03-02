import os
import sqlite3 as sq
import sys
import json
import csv


# 用作寫入資料庫。
class Wdb:
    def __init__(self, x):
        d = x.split(',')
        self.name = d[1]
        self.path = d[0]
        # 若有相符名稱，建立資料庫。
        self.crdb()


    # 定義存入SQLite 時的資料型態。
    # type.json 檔案儲存ThinkRP 基本column 資料型態。
    # !!! GCR類，的OnScript無法正確定義資料型態，以text存。
    def gettype(self):
        self.type = {}
        # !!! 需要讀取資料型態json檔，先放在絕對路徑
        with open('C:\\ThinkRP\\Data\\DB\\type.json', 'r') as t:
            p = json.load(t)
            for i in range(len(self.column)):
                if self.column[i] in p:
                    print(self.column[i], p[self.column[i]])
                    self.type[self.column[i]] = p[self.column[i]]
                else:
                    self.type[self.column[i]] = 'text'

    def crdb(self):
        dirlist = os.listdir(self.path)

        # 第一筆資料讀到時，建立資料庫及欄位。
        flag = True
        for filenm in dirlist:
            if self.name == filenm[:-9]:
                # 若名稱符合，建立資料庫，寫入Column。
                if flag:

                    # !!! 用LabView建立時，需要絕對路徑
                    # self.conn = sq.connect(f'{self.path}\\{self.name}.db3')
                    # 改先放到絕對路徑 path='C:\\ThinkRP\\Data\\DB'
                    self.mdbdir('C:\\ThinkRP\\Data\\DB')
                    self.conn = sq.connect(f'C:\\ThinkRP\\Data\\DB\\{self.name}.db3')


                    # !!! 先刪掉舊的TABLE
                    try:
                        s = f'drop table "{self.name}"'
                        self.conn.execute(s)
                    except:
                        print('dedbt')

                    with open(f'{self.path}\\{filenm}', 'r') as csvfile:
                        data = [d for d in csv.reader(csvfile)]
                        self.column = data[0]
                        self.data = data[1:]
                    self.type = {}

                    self.gettype()
                    # 主KEY Id，其他欄位資料型態照type.json
                    s = f'''CREATE TABLE "{self.name}" (ID INTEGER PRIMARY KEY AUTOINCREMENT, 
                               {", ".join(f'"{data}" {self.type[data]}' for data in self.column)} );'''
                    print(s)
                    self.conn.execute(s)
                    self.conn.commit()
                    flag = False
                    self.wrdata(f'{self.path}\\{filenm}')
                else:
                    self.wrdata(f'{self.path}\\{filenm}')

            # 若有寫入資料庫，用來回傳OK。
            # str暫代OK狀態。
            self.__str__ = 'OK'


        if flag:
            print('找不到此CSV')
            raise Exception('Can\'t find this csv file.')



    # CSV資料寫入DB。
    def wrdata(self, csvpath):
        with open(csvpath, 'r') as csvfile:
            # 轉成list，去掉第一行欄位，會影響效能嗎?
            data = list(csv.reader(csvfile))[1:]
            print(data)
            s = ''
            # 計算寫入筆數
            c = 0
            for line in data:
                print(line)
                line1 = [f'"{d}"' for d in line]
                # 補""，避免長度不相符
                while len(self.column) > len(line1):
                    line1.append('""')
                s2 = ",".join(line1)
                if c == 0:
                    col = [f'"{ll}"' for ll in self.column]
                    s1 = ",".join(col)
                    print(s1)
                    s += f'INSERT INTO "{self.name}" ({s1}) VALUES ({s2})'
                    c += 1
                elif c <= 8000:
                    s += f',({s2})'
                    c += 1
                else:
                    s += f',({s2})'
                    self.conn.execute(s)
                    self.conn.commit()
                    s = ''
                    c = 0
            self.conn.execute(s)
            self.conn.commit()


    def mdbdir(self, path='C:\\ThinkRP\\Data\\DB'):
        if not os.path.isdir(path):
            os.makedirs(path)

class Rdb:
    # 連接資料庫
    def __init__(self, x):
        d = x.split(',')
        self.path = d[0]
        self.name = d[1]

        self.startID = d[2]
        # 若沒起始點，預設為0。
        if self.startID == '':
            self.startID = '0'
        if os.path.isfile(f'{self.path}\\{self.name}.db3'):
            self.conn = sq.connect(f'{self.path}\\{self.name}.db3')
        else:
            print(f'{self.path}\\{self.name}.db3')
            raise Exception('NO DB')

    def db(self):
        get = self.conn.execute(f'select * from {self.name} limit 20;').fetchall()
        a = [aa for aa in get]
        sss = ",".join([str(aa) for aa in a[0]])
        print(sss)
        return sss

    # 篩選讀取條件，ff字串為需要的欄位。
    # args、kwargs用作篩選條件?
    def filt(self, ff='*', *arg, **kwargs):
        # 條件
        print(arg)
        fi = ff.split(',')
        # 加入引號""，移除空白。
        # !!!如果欄位中有空白也會被去掉。
        fii = [f'"{a.replace(" ", "")}"' for a in fi]

        if arg:
            s = f'''select {', '.join(fii)} from {self.tn} WHERE {' AND '.join(arg)} limit 5'''
            print(s)
            li = [','.join(map(str, a)) for a in self.conn.execute(s).fetchall()]
            # print(li)

        else:
            s = f'''select {', '.join(fii)} from {self.tn} limit 5000'''
            print(s)
            li = [','.join(map(str, a)) for a in self.conn.execute(s).fetchall()]
            # print(li)

        return '\n'.join(li)

    # 讀取資料庫欄位，字串以逗號分割。
    def rcolumn(self):
        s = f'''select * from {self.tn} limit 1'''
        li = [d[0] for d in self.conn.execute(s).description]
        print(li)
        return ','.join(li) + '\n'

    # 讀取資料，預設為5000筆，字串以逗號分割，行與行已\n分隔。
    def rdata(self, nrow=5000):
        s = f'''select * from {self.name} limit {self.startID}, {nrow}'''
        li = [','.join(map(str, a)) for a in self.conn.execute(s).fetchall()]
        return '\n'.join(li)



# 從LabView傳來字串 路徑,檔名
def cdb(x):
    xx = Wdb(x)
    # 若回傳"OK" LabView亮燈
    return xx.__str__


def rdb(x):
    # x 目前為 路徑,名稱,讀取index
    xx = Rdb(x)
    print(xx.rdata())
    return xx.rdata()


if __name__ == '__main__':
    # print(cdb('D:\\labview\\test2\\CSV,test 3'))
    print(rdb('C:\\ThinkRP\\Data\\DB,test2,'))
