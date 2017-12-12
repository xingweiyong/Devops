#coding:utf-8
import MySQLdb
class MySql():
    def __init__(self,host,port,user,password,db):
        self.conn = MySQLdb.connect(host=host,port=port,user=user,passwd=password,db=db,charset="utf8")
        self.cursor = self.conn.cursor()
    def insert(self,dic):
        try:
            self.cursor.execute("""INSERT IGNORE INTO zb(title,
            num, data_period,cat_url,cat_name,plat,room_id,room_url)
         VALUES ('%s', '%s', '%s','%s','%s','%s','%s','%s')"""%(dic['title'],dic['num'],dic['crawl_time'],dic['cat_url'],dic['cat_name'],
                dic['plat'],dic['room_id'],dic['room_url']))
            self.conn.commit()
            #self.conn.close()
        except Exception,e:
            self.cursor.execute("""INSERT INTO zb(title,
               num, data_period,cat_url,cat_name,plat,room_id,room_url)
            VALUES ('%s', '%s', '%s','%s','%s','%s','%s','%s')""" % (
            'emoji_name', dic['num'], dic['crawl_time'], dic['cat_url'], dic['cat_name'],
            dic['plat'], dic['room_id'],dic['room_url']))
            self.conn.commit()
            #self.conn.close()
            print 'insert err: ',e
    def insert_momo_item(self,dic):
        try:
            self.cursor.execute("""INSERT IGNORE INTO zb(title,
            num, data_period,room_id,plat,star,follow,room_url)
         VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')"""%(dic['title'],dic['num'],dic['crawl_time'],
            dic['room_id'],dic['plat'],dic['star'],dic['follow'],dic['room_url']))
            self.conn.commit()
        except Exception,e:
            self.cursor.execute("""INSERT INTO zb(title,
            num, data_period,room_id,plat,star,follow,room_url)
             VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')"""%('emoji_name',dic['num'],dic['crawl_time'],
            dic['room_id'],dic['plat'],dic['star'],dic['follow'],dic['room_url']))
            self.conn.commit()

            print 'insert err: ',e
    def insert_momo_item_full(self,dic):
        try:
            self.cursor.execute("""INSERT IGNORE INTO zb_momofull(title,
            num, data_period,room_id,plat,star,follow,room_url)
         VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')"""%(dic['title'],dic['num'],dic['crawl_time'],
            dic['room_id'],dic['plat'],dic['star'],dic['follow'],dic['room_url']))
            self.conn.commit()
        except Exception,e:
            self.cursor.execute("""INSERT INTO zb_momofull(title,
            num, data_period,room_id,plat,star,follow,room_url)
             VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')"""%('emoji_name',dic['num'],dic['crawl_time'],
            dic['room_id'],dic['plat'],dic['star'],dic['follow'],dic['room_url']))
            self.conn.commit()

            print 'insert err: ',e
    def insert_momoid_list(self,dic):
        #print "insert_momoid_list",dic
        if not self.select('select * from momoid_list WHERE momoid = %s'%dic['momoid']):
            try:
                self.cursor.execute("""INSERT IGNORE INTO momoid_list(momoid)
                 VALUES ('%s')""" % (
                dic['momoid']))
                self.conn.commit()
                #self.conn.close()
            except Exception,e:
                print 'insert err: ',e
        #else:
            #print 'id exists...'

    def insert_detector(self,dic):
        try:
            self.cursor.execute("""insert into detector(flag,item_name,total_num,ratio_h,ratio_t,db_name) values ('%s','%s','%s','%s','%s','%s')"""%(dic['flag'],dic['item_name'],dic['total_num'],dic['ratio_h'],dic['ratio_t'],dic['db_name']))
            self.conn.commit()
        except Exception,e:
            print 'insert err: ',e
    
    def select(self,sel_str):
        try:
            self.cursor.execute(sel_str)
            data = self.cursor.fetchall()
            return data
        except Exception,e:
            print 'select err: ',e
            return None
    def update(self,up_str):
        try:
            self.cursor.execute(up_str)
            self.conn.commit()
            #print 'update success'
        except Exception,e:
            print 'update err: ',e

    def close(self):
        self.conn.close()

if __name__ == '__main__':
    db = MySql('ip',3307, 'user', 'pwd', 'db')
    dic = {'title':'eee','num':'233','crawl_time':'2016-09-06 11:17:32.000000'}
    #db.insert(dic)
    #db.insert_momoid_list({'momoid':'11'})
    #print db.select('select * from momoid_list WHERE momoid = 11')
    db.insert_momoid_list({'momoid':'395786651'})
