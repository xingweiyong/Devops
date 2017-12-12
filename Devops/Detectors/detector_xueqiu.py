#coding:utf-8

from Devops.DB.MySQLDB import MySql
import time

class detector_xueqiu():
    def __init__(self):
        self.data_period = time.strftime('%Y-%m-%d', time.localtime(time.time() - 24*3600))
        self.data_period_pre = time.strftime('%Y-%m-%d', time.localtime(time.time() - 48*3600))
        self.data_period_day = time.strftime('%Y-%m-%d', time.localtime(time.time() - 30*24 * 60 * 60))

        self.res_info = {}
        self.res_info['flag'] = self.data_period
        self.res_info['db_name'] = 'xueqiu'


    def run(self):
        print u'xueqiu,开始查询...'

        db = MySql('36.110.128.75', 3306, 'root', 'Bigdata1234', 'crawler_db')
        records_count_curr = db.select("SELECT count(*) FROM xueqiu WHERE crawl_time like '%s%%'"%self.data_period)[0][0]
        records_count_pre = db.select("SELECT count(*) FROM xueqiu WHERE crawl_time like '%s%%'" % self.data_period_pre)[0][0]
        records_count_day = db.select("SELECT count(*) FROM xueqiu WHERE crawl_time like '%s%%'" % self.data_period_day)[0][0]

        db.close()

        if None not in [records_count_curr,records_count_pre,records_count_day]:
            self.res_info['records_count'] = {'total_num': records_count_curr,
                'ratio_h':float(records_count_curr - records_count_pre) / records_count_pre,
                'ratio_t':float(records_count_curr - records_count_day) / records_count_day}
        else:
            print u'no records_count...'

        if len(self.res_info.keys()) > 2:
            print u'保存查询结果...'
            db = MySql('10.13.38.11', 3307, 'crawler', 'crawlerQaz', 'chaser')
            for k,v in self.res_info.items():
                if k not in ['flag', 'db_name']:
                    temp = {}
                    temp.update(v)
                    temp.update({'flag':self.res_info['flag']})
                    temp.update({'db_name': self.res_info['db_name']})
                    temp.update({'item_name':k})
                    #print temp
                    db.insert_detector(temp)

            db.close


if __name__ == '__main__':
    dt = detector_xueqiu()
    dt.run()
