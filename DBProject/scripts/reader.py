import boto.sqs as sqs
import pymysql
import datetime
import json

class dbutils:
    host = 'aline-cnu-insights-dev-cluster.cluster-czuocyoc6awe.us-east-1.rds.amazonaws.com'
    port = 3306
    user = 'mgor'
    password = 'mgor'
    db = 'cnu2016_mgor'

    def insert(audit_log):
        conn = pymysql.connect(host=dbutils.host, port=dbutils.port, user=dbutils.user, passwd=dbutils.password, db=dbutils.db)
        curr = conn.cursor()
        jobj = json.loads(audit_log)
        print(jobj)
        time =  int(jobj['timestamp']);
        dt = datetime.datetime.fromtimestamp(time//1000)
        print(dt)
        tup = (str(dt),jobj['url'],jobj['ipAddress'],jobj['responseCode'],jobj['data'])
        print(tup)
        query = "insert into audit_log(time_created,url,ip_address,response_code,params) values" + str(tup)
        # query = "insert into audit_log(time_created,url,ip_address,response_code,params) values(" + str(dt) + "," + jobj['url'] + ","  + jobj['ipAddress'] + ", " + str(jobj['responseCode']) + ","+ jobj['data'] + ")";
        print(query)
        curr.execute(query);
        conn.commit()
        curr.close()
        conn.close()

queue_name = "cnu2016_mgor"

conn = sqs.connect_to_region(region_name="us-east-1")
queue = conn.get_queue(queue_name)
while(True):
    msg = queue.read(30)
    if(msg != None):
        print(msg.get_body())
        dbutils.insert(msg.get_body())
        queue.delete_message(msg)