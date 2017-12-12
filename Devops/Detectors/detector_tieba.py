#coding:utf-8

from Devops.DB.MySQLDB import MySql
import time

class detector_tieba():
    def __init__(self):
        self.data_period = time.strftime('%Y-%m-%d', time.localtime(time.time() - 24*3600))
        self.data_period_pre = time.strftime('%Y-%m-%d', time.localtime(time.time() - 48*3600))
        self.data_period_day = time.strftime('%Y-%m-%d', time.localtime(time.time() - 30*24 * 60 * 60))

        self.res_info = {}
        self.res_info['flag'] = self.data_period
        self.res_info['db_name'] = 'tieba'

    def get_data(self,q_res):
        temp = 0
        if q_res != None and len(q_res) > 0:
            temp = q_res[0][0]
        return temp

    def run(self):
        print u'tieba,开始查询...'

        db = MySql('36.110.128.75', 3306, 'root', 'Bigdata1234', 'crawler_db')
        records_count_curr = self.get_data(db.select("SELECT count(*) FROM tieba WHERE flag = '%s'"%self.data_period))
        records_count_pre = self.get_data(db.select("SELECT count(*) FROM tieba WHERE flag = '%s'"% self.data_period_pre))
        records_count_day = self.get_data(db.select("SELECT count(*) FROM tieba WHERE flag = '%s'" % self.data_period_day))

        db.close()

        self.res_info['records_count'] = {'total_num': records_count_curr,
                                          'ratio_h': float(records_count_curr - records_count_pre) /
                                                     records_count_pre if records_count_pre != 0 else records_count_pre,
                                          'ratio_t': str(float(records_count_curr - records_count_day) /
                                                         records_count_day) if records_count_day != 0 else records_count_day}

        print u'保存查询结果...'
        db=MySql('10.13.38.11',3307, 'crawler', 'crawlerQaz', 'chaser')
        for k,v in self.res_info.items():
            if k not in  ['flag','db_name']:
                temp = {}
                temp.update(v)
                temp.update({'flag':self.res_info['flag']})
                temp.update({'db_name': self.res_info['db_name']})
                temp.update({'item_name':k})
                #print temp
                db.insert_detector(temp)

        db.close

if __name__ == '__main__':
    dt_tieba = detector_tieba()
    dt_tieba.run()

