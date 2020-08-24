import os;
import sys;
import MySQLdb;

def db_link():
    conn = MySQLdb.connect("172.16.20.229", "root", "tas123", "project_company_mgmt_db_test")
    cur = conn.cursor()
    return conn , cur


def insert_into_tables():
    conn , cur = db_link()
    entity_type = ['', 'Public Company', 'Private Company', 'Education', 'Non-Profit or Social Organization', 'Government', 'Uncategorized']
    for entity in entity_type:
        print entity
        #"""
        #sql_in = "insert into Languages(language, code) values ('%s', '%s')"%(str(mon[0]), str(mon[1]))
        #sql_in = "insert into Months(month) values ('%s')"%(mon)
        #sql_in = "insert into Accounting_Standard(accounting_standard) values ('%s')"%(mon)
        sql_in = "insert into entity_type(ent_type) values ('%s')"%(entity)
        #sql_in = """insert into industrytype(industryName) values ("%s")"""%(mon)
        print sql_in
        cur.execute(sql_in)
        conn.commit() 
        """
        sql_in = "insert into Currency(currency, code) values("%s", "%s")"%(str(mon), str(val))
        print sql_in
        cur.execute(sql_in)
        """


if __name__ == '__main__':
   insert_into_tables()
