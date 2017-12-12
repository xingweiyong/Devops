#coding:utf-8


'''
发送电子邮件模块
'''


import smtplib
from email.mime.text import MIMEText
import string
import time

class smtp():
    def __init__(self,HOST = 'smtp.163.com',SUBJECT = 'DATA BASE DETECTOR',FROM = '13920852362@163.com'):
        self.HOST = HOST
        self.SUBJECT = SUBJECT
        self.FROM = FROM
        self.BODY = ''
        #self.TO = ['xingweiyong@jd.com']
        #self.text = ''
        
    def send(self,TO,text):
        #self.TO.extend(TO)
        #self.text += text
        self.BODY=string.join((
            'From: %s' %self.FROM,
            'To: %s' %TO,
            'Subject: %s' %self.SUBJECT,
            '',
            text
            ),'\r\n')

        try:
            server = smtplib.SMTP()
            server.connect(self.HOST,'25')
            server.starttls()
            server.login('13920852362@163.com','cauc787')
            for item in TO:
                server.sendmail(self.FROM,item,self.BODY)
                time.sleep(0.5)
            print u'邮件发送成功~'
        except Exception,e:
            print u'发送失败，err: ',e
            
        finally:
            server.quit()
if __name__ == '__main__':
    mail = smtp()
    mail.send(['xingweiyong@jd.com'],'test')
