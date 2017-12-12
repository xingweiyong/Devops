#coding:utf-8

from Devops.DB.MySQLDB import MySql
import time

class detector_momo():

    def __init__(self):
        self.curr_hour = int(time.strftime('%H', time.localtime(time.time())))
        # if self.curr_hour < 18:
        #     self.data_period = time.strftime('%Y-%m-%d', time.localtime(time.time())) + '_13'
        #     self.data_period_pre = time.strftime('%Y-%m-%d', time.localtime(time.time() - 24 * 60 * 60)) + '_20'
        #     self.data_period_day = time.strftime('%Y-%m-%d', time.localtime(time.time() - 24 * 60 * 60)) + '_13'
        # else:
        #     self.data_period = time.strftime('%Y-%m-%d', time.localtime(time.time())) + '_20'
        #     self.data_period_pre = time.strftime('%Y-%m-%d', time.localtime(time.time())) + '_13'
        #     self.data_period_day = time.strftime('%Y-%m-%d', time.localtime(time.time() - 24 * 60 * 60)) + '_20'

        self.data_period = time.strftime('%Y-%m-%d_%H', time.localtime(time.time() - 3600))
        self.data_period_pre = time.strftime('%Y-%m-%d_%H', time.localtime(time.time() - 2 * 60 * 60))
        self.data_period_day = time.strftime('%Y-%m-%d_%H', time.localtime(time.time() - 25 * 60 * 60))

        self.res_info = {}

        self.res_info['flag'] = self.data_period
        self.res_info['db_name'] = 'zb_momo'

    def run(self):
        print u'momo,开始查询...'

        db=MySql('10.13.38.11',3307, 'crawler', 'crawlerQaz', 'chaser')

        records_count = db.select("SELECT data_period,count(data_period) from zb WHERE plat = 'momo' and data_period in ('%s','%s','%s') GROUP BY data_period"%(self.data_period,self.data_period_pre,self.data_period_day))
        num_sum = db.select("SELECT data_period,sum(num) from zb WHERE plat = 'momo' and data_period in ('%s','%s','%s') GROUP BY data_period"%(self.data_period,self.data_period_pre,self.data_period_day))
        star_count = db.select("SELECT data_period,count(star) from zb WHERE plat = 'momo' and data_period in ('%s','%s','%s') and star != 'none' GROUP BY data_period"%(self.data_period,self.data_period_pre,self.data_period_day))
        follow_count = db.select("SELECT data_period,count(follow) from zb WHERE plat = 'momo' and data_period in ('%s','%s','%s') and follow != 'none' GROUP BY data_period"%(self.data_period,self.data_period_pre,self.data_period_day))

        db.close()

        if len(records_count) == 3:
            self.res_info['records_count'] = {'total_num': records_count[2][1],
                                              'ratio_h': float(records_count[2][1] - records_count[1][1]) /
                                                         records_count[1][1],
                                              'ratio_t': float(records_count[2][1] - records_count[0][1]) /
                                                         records_count[0][1]}
            self.res_info['num_sum'] = {'total_num': num_sum[2][1],
                                        'ratio_h': float(num_sum[2][1] - num_sum[1][1]) / num_sum[1][1],
                                        'ratio_t': float(num_sum[2][1] - num_sum[0][1]) / num_sum[0][1]}
            self.res_info['star_count'] = {'total_num': star_count[2][1],
                                           'ratio_h': float(star_count[2][1] - star_count[1][1]) / star_count[1][1],
                                           'ratio_t': float(star_count[2][1] - star_count[0][1]) / star_count[0][1]}
            self.res_info['follow_count'] = {'total_num': follow_count[2][1],
                                             'ratio_h': float(follow_count[2][1] - follow_count[1][1]) / follow_count[1][1],
                                             'ratio_t': float(follow_count[2][1] - follow_count[0][1]) / follow_count[0][1]}


        else:
            print u'没有符合条件的记录'

        if len(self.res_info.keys()) > 2:
            print u'保存查询结果...'
            db=MySql('10.13.38.11',3307, 'crawler', 'crawlerQaz', 'chaser')
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
    dt_momo = detector_momo()
    dt_momo.run()