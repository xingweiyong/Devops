#coding:utf-8

from Devops.DB.MySQLDB import MySql
import time

class detector_yy():

    def __init__(self):
        self.curr_hour = int(time.strftime('%H', time.localtime(time.time())))

        self.data_period = time.strftime('%Y-%m-%d_%H', time.localtime(time.time() - 3600))
        self.data_period_pre = time.strftime('%Y-%m-%d_%H', time.localtime(time.time() - 2 * 60 * 60))
        self.data_period_day = time.strftime('%Y-%m-%d_%H', time.localtime(time.time() - 25 * 60 * 60))


        self.res_info = {}

        self.res_info['flag'] = self.data_period
        self.res_info['db_name'] = 'zb_yy'

    def get_data(self,q_res):
        temp = 0
        if q_res != None and len(q_res) > 0:
            temp = q_res[0][1]
        return temp

    def run(self):
        print u'yy,开始查询...'
        #make_info(res_info,records_count,'records_count')

        db=MySql('10.13.38.11',3307, 'crawler', 'crawlerQaz', 'chaser')

        records_count = self.get_data(db.select("SELECT data_period,count(data_period) from zb WHERE plat = 'yy' and data_period in ('%s') GROUP BY data_period"%(self.data_period)))
        records_count_pre = self.get_data(db.select(
            "SELECT data_period,count(data_period) from zb WHERE plat = 'yy' and data_period in ('%s') GROUP BY data_period" % (
            self.data_period_pre)))
        records_count_day = self.get_data(db.select(
            "SELECT data_period,count(data_period) from zb WHERE plat = 'yy' and data_period in ('%s') GROUP BY data_period" % (
            self.data_period_day)))

        num_count = self.get_data(db.select("SELECT data_period,count(data_period) from zb WHERE plat = 'yy' AND num != 'none'and data_period in ('%s') GROUP BY data_period"%(self.data_period)))
        num_count_pre = self.get_data(db.select(
            "SELECT data_period,count(data_period) from zb WHERE plat = 'yy' AND num != 'none'and data_period in ('%s') GROUP BY data_period" % (
            self.data_period_pre)))
        num_count_day = self.get_data(db.select(
            "SELECT data_period,count(data_period) from zb WHERE plat = 'yy' AND num != 'none'and data_period in ('%s') GROUP BY data_period" % (
            self.data_period_day)))

        db.close()


        self.res_info['records_count'] = {'total_num': records_count,
                                              'ratio_h': float(records_count - records_count_pre) /
                                                         records_count_pre if records_count_pre != 0 else records_count_pre,
                                              'ratio_t': str(float(records_count - records_count_day) /
                                                         records_count_day) if records_count_day != 0 else records_count_day}
        self.res_info['num_count'] = {'total_num': num_count,
                                      'ratio_h': float(num_count - num_count_pre) /
                                                     num_count_pre if num_count_pre != 0 else  num_count_pre,
                                      'ratio_t': float(num_count - num_count_day) /
                                                     num_count_day if num_count_day != 0 else  num_count_day}


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
    dt_yy = detector_yy()
    dt_yy.run()