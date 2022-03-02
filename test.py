import sqlite3 as sq
import sys


class DB:
    # 連接資料庫
    def __init__(self, tn='test00'):
        self.conn = sq.connect('C:\\ThinkRP\\test2\\testDB2.db')
        self.tn = tn

    def db(self):
        get = self.conn.execute('select * from test00 limit 5;').fetchall()
        # print(get)
        a = [aa for aa in get]
        sss = ",".join([str(aa) for aa in a[0]])
        print(sss)
        return sss

    # 篩選讀取條件，ff字串為需要的欄位。
    # args、kwargs用作篩選條件?
    def filt(self, ff='*', *arg, **kwargs):


        """ 欄位篩選


        # 未檢查是否有該欄位。
        #
        fi = ff.split(',')
        # 加入引號""，移除空白。
        # !!!如果欄位中有空白也會被去掉。
        fii = [f'"{a.replace(" " , "")}"' for a in fi]
        s = f'''select {', '.join(fii)} from {self.tn} limit 5000'''
        print(s)
        li = [','.join(map(str, a)) for a in self.conn.execute(s).fetchall()]
        # print(li)
        return '\n'.join(li)



        """

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
        s = f'''select * from {self.tn} limit {nrow}'''
        li = [','.join(map(str, a)) for a in self.conn.execute(s).fetchall()]
        return '\n'.join(li)


def teststring(x='Index, Action, Current(mA)', *args):
    a = DB()

    # 使用 filt，return
    # return x + '\n' + a.filt(x, '"Voltage(mV)">= 3.6', '"Action"="DISCHARGE"')
    return x + '\n' + a.filt(x, *args)

    # 未用filt，return
    # return a.rcolumn() + a.rdata(5)


def testarray(x=['11', '22', '33']):
    return x

def rc():
    a = DB()
    return a.rcolumn()


if __name__ == '__main__':
    t = DB()
    # t.filt('Index, Action, Voltage(mV), Current(mA)', '"Voltage(mV)">= 3.6', '"Action"="DISCHARGE"')
    print(teststring('Index, Action, Voltage(mV), Current(mA)', '"Voltage(mV)">= 3.6', '"Action"="DISCHARGE"'))
    # print(teststring('Index, Action, Voltage(mV), Current(mA)'))
