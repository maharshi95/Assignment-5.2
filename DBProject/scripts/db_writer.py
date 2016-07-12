import pymysql
import json
class dbutils:
    host = 'aline-cnu-insights-dev-cluster.cluster-czuocyoc6awe.us-east-1.rds.amazonaws.com'
    port = 3306
    user = 'mgor'
    password = 'mgor'
    db = 'cnu2016_mgor'

    def insert(self,audit_log):
        conn = pymysql.connect(host=dbutils.host, port=dbutils.port, user=dbutils.user, passwd=dbutils.password, db=dbutils.db)
        print(conn)
        curr = conn.cursor()
        jobj = json.loads(audit_log)
        print(jobj)
        tup = (jobj['timestamp'],jobj['url'],jobj['ipAddress'],jobj['responseCode'],jobj['data'])
        print(tup)
        # curr.execute("insert into audit_log(time_created,url,ipAddress,response_code,params) values + tup");
        conn.commit()
        curr.close()
        conn.close()