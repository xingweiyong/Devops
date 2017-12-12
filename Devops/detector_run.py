#coding:utf-8

'''
扫描数据库，判断预警条件，选择发送邮件通知
'''

import os,sys
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from SMTP.smtp import smtp
from DB.MySQLDB import MySql
import time
from Detectors.detector_momo import detector_momo
from Detectors.detector_yy import detector_yy
from Detectors.detector_guba import detector_guba
from Detectors.detector_xueqiu import detector_xueqiu
from Detectors.detector_tieba import detector_tieba
import settings

def sendmsg(q_res):
    detect_items = []
    for each_tuple in q_res:
        if each_tuple[1] not in detect_items:
            detect_items.append(each_tuple[1])

    for each_item in detect_items:
        message = ''
        for each_tuple1 in q_res:
            message += '**' + '\n'
            if each_tuple1[1] == each_item:
                if abs(each_tuple1[5]) <= 2 and abs(each_tuple1[6]) <= 2 and each_tuple1[4]!=0:
                    for i in range(1,7):
                        message += str(each_tuple1[i]) + '\n'
                    message += '>>>>> ok!!'+'\n\n'
                elif ((abs(each_tuple1[5]) > 2 and abs(each_tuple1[5])) <= 5 or (abs(each_tuple1[6]) > 2 and abs(each_tuple1[6]) <= 5)) and each_tuple1[4]!=0:
                    for i in range(1, 7):
                        message += str(each_tuple1[i]) + '\n'
                    message += '>>>>> warining!!'+'\n\n'
                else:
                    for i in range(1, 7):
                        message += str(each_tuple1[i]) + '\n'
                    message += '>>>>> critical!!'+'\n\n'

        TO = []
        #TO.extend(settings.db_map['leader'])
        TO.extend(settings.db_map[each_item])
        if 'ok' in message or 'warining' in message or 'critical' in message:
            sm = smtp()
            sm.send(TO, message)


# **********************************************************
# 判断时是否进行监控行为
curr_day = time.strftime('%d',time.localtime(time.time()))
curr_hour = time.strftime('%H',time.localtime(time.time()))

# momo 每小时一次
# if curr_hour in ['17','23']:
try:
    dt_momo = detector_momo()
    dt_momo.run()
except Exception,e:
    print 'momo error: ',e
# yy 每小时执行一次
try:
    dt_yy = detector_yy()
    dt_yy.run()
except Exception,e:
    print 'yy error: ',e

# guba 每天启动detector六次
try:
    if curr_hour in ['10','11','12','14','15','16']:
        dt_vip = detector_guba()
        dt_vip.run()
except Exception, e:
    print 'guba error: ', e

# xueqiu spider 每天一次
try:
    if curr_hour in ['08']:
        dt_xueqiu = detector_xueqiu()
        dt_xueqiu.run()
except Exception,e:
    print 'guba error: ',e

# tieba 每天一次 八点执行一次detector 查询上一批次数据
try:
    if curr_hour in ['08']:
        dt_tieba = detector_tieba()
        dt_tieba.run()
except Exception,e:
    print 'guba error: ',e

time.sleep(5)

# **********************************************************
# 判断是否是否超过预警值，选择负责人，发送邮件
db=MySql('10.13.38.11',3307, 'crawler', 'crawlerQaz', 'chaser')
q_res = db.select('SELECT * FROM detector WHERE status = 0')
db.update('UPDATE detector SET STATUS = 1 WHERE STATUS = 0')
db.close()


sendmsg(q_res)