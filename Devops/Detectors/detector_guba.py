#coding:utf-8

from Devops.DB.MySQLDB import MySql
import time

class detector_guba():
    def __init__(self):
        self.curr_hour = int(time.strftime('%H', time.localtime(time.time())))
        if self.curr_hour in [11,12,15,16]:
            self.data_period_pre = time.strftime('%Y-%m-%d %H', time.localtime(time.time() - 2 * 60 * 60))
        elif self.curr_hour == 10:
            self.data_period_pre = time.strftime('%Y-%m-%d', time.localtime(time.time() - 24 * 60 * 60))+' 15'
        elif self.curr_hour == 14:
            self.data_period_pre = time.strftime('%Y-%m-%d', time.localtime(time.time() - 24 * 60 * 60)) + ' 11'
        self.data_period = time.strftime('%Y-%m-%d %H', time.localtime(time.time() - 3600))
        self.data_period_day = time.strftime('%Y-%m-%d %H', time.localtime(time.time() - 25 * 60 * 60))

        self.res_info = {}
        self.res_info['flag'] = self.data_period
        self.res_info['db_name'] = 'guba'


    def run(self):
        print u'guba,开始查询...'

        db = MySql('id', 3306, 'root', 'pwd', 'db')
        records_count_curr = db.select("SELECT count(*) FROM guba WHERE crawl_time like '%s%%'"%self.data_period)[0][0]
        records_count_pre = db.select("SELECT count(*) FROM guba WHERE crawl_time like '%s%%'" % self.data_period_pre)[0][0]
        records_count_day = db.select("SELECT count(*) FROM guba WHERE crawl_time like '%s%%'" % self.data_period_day)[0][0]

        num_sum_curr = db.select("SELECT sum(num) FROM guba WHERE crawl_time like '%s%%'"%self.data_period)[0][0]
        num_sum_pre = db.select("SELECT sum(num) FROM guba WHERE crawl_time like '%s%%'" % self.data_period_pre)[0][0]
        num_sum_day = db.select("SELECT sum(num) FROM guba WHERE crawl_time like '%s%%'" % self.data_period_day)[0][0]

        db.close()

        if None not in [records_count_curr,records_count_pre,records_count_day]:
            self.res_info['records_count'] = {'total_num': records_count_curr,
                'ratio_h':float(records_count_curr - records_count_pre) / records_count_pre,
                'ratio_t':float(records_count_curr - records_count_day) / records_count_day}
        else:
            print u'no records_count...'

        if None not in [num_sum_curr,num_sum_pre,num_sum_day]:
            self.res_info['num_sum'] = {'total_num': num_sum_curr,
                'ratio_h':float(num_sum_curr - num_sum_pre) / num_sum_pre,
                'ratio_t':float(num_sum_curr - num_sum_day) / num_sum_day}
        else:
            print u'no num_sum...'

        if len(self.res_info.keys()) > 2:
            print u'保存查询结果...'
            db = MySql('ip', 3307, 'user', 'pwd', 'db')
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
    dt = detector_guba()
    dt.run()
