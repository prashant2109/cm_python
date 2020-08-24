import os;
import sys;
import MySQLdb;

def db_link():
    conn = MySQLdb.connect("172.16.20.229", "root", "tas123", "project_company_mgmt_db_test")
    cur = conn.cursor()
    return conn , cur


def insert_into_tables():
    conn , cur = db_link()
    sql_sel = "select row_id,account_standard  from company_mgmt"
    cur.execute(sql_sel)
    res_all = cur.fetchall()
    for res in res_all:
        #print res
    #    print res[1]
        #continue
        #sys.exit()
        if res[1] == None or res[1] == '':
           continue
        sql_insert = "insert into company_accounting_standard(company_id ,account_st_id) values(%s, '%s')"%(res[0], res[1])
        print sql_insert
        cur.execute(sql_insert)
    conn.commit()




if __name__ == '__main__':
   insert_into_tables()
