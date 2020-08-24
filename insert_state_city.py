import os;
import sys;
import MySQLdb;

def get_connection():
    conn = MySQLdb.connect("172.16.20.229", "root", "tas123", "project_company_mgmt_db_test")
    curs   = conn.cursor()
    return conn, curs

def insert_data():
    conn, curs = get_connection()
    f = open('data_cit1.txt', 'r')
    res = f.read()
    vals = res.strip().split('\n')
    for keys , vals in eval(vals[0]).items():
        print keys , vals 
        sql_sel_cuntry = "select id, country from country where country = '%s'"%(keys)
        curs.execute(sql_sel_cuntry)
        res_d = curs.fetchall()
        print res_d
        for d in res_d:
            country = d[0]
            for k_st, v_st in vals.items():
                sql_insert_state = """insert into state(country, state) values(%s, '%s')"""%(country, k_st)
                curs.execute(sql_insert_state)
                print sql_insert_state
                sql_row_id = "SELECT LAST_INSERT_ID()";
                curs.execute(sql_row_id)
                res11 = curs.fetchone()
                row_id = res11[0] 
                print "row ids::::::::::::::::::", row_id
                for cty in v_st:
                    sql_insert_city = """insert into city(state, city)values(%s, '%s')"""%(row_id, cty)
                    curs.execute(sql_insert_city)
                    print "city data::::::::::", sql_insert_city
    conn.commit() 
    conn.close() 



if __name__ == '__main__':
   insert_data()
