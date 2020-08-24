import os;
import sys;
import MySQLdb

def get_connections():
    conn = MySQLdb.connect("172.16.20.229", "root", "tas123", "wealthx_common")
    cur = conn.cursor()
    return conn, cur
   
def select_insert_states():
    conn, cur = get_connections()
    sql_Sel_country = "select id, country from country"
    cur.execute(sql_Sel_country)
    ress = cur.fetchall()
    sdicts_val = {}
    sdicts_state = {}
    full_vals ={}
    if ress:
       for data in ress:
           #sdicts_val[int(data[0])] = data[1]
           sdicts_val[str(data[1])] = str(data[0])
           sql_sel_state = "select id, state from state where country = %s"%(data[0])
           cur.execute(sql_sel_state)
           ress_data = cur.fetchall()
           #print ress_data
           if ress_data:
              sdicts_states ={}   
              for data_val in ress_data:
                       #print "statr::::::::", data[1], data[0], data_val[0], data_val[1]
                       sel_city = "select id, city from city where state = %s"%(data_val[0])
                       cur.execute(sel_city)
                       res_city = cur.fetchall()
                       if res_city:
                          lists_city = []
                          for city in res_city:
                              lists_city.append(city[1])
                              #print "statr::::::::", data[1], data[0], data_val[0], data_val[1], city[0], city[1]
                          sdicts_states[data_val[1]] = lists_city
              #print "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n"
              #print sdicts_states
              full_vals[str(data[1])] = sdicts_states
    print full_vals
    sys.exit()
                  
    print sdicts_val
    print len(sdicts_val)


if __name__ == '__main__':
  select_insert_states()
