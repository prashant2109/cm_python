import os, sys, json, MySQLdb
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import config

class DocStatus(object):
    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur
    def status_update(self, ijson):
        project_id = str(ijson['project_id']) 
        company_row_id  = str(ijson['company_id'])
        doc_ids    = str(ijson['doc_id'])
        user_name  = str(ijson.get('user', 'demo'))
        status      = ijson.get('status', 'Y')
        if not user_name:
            user_name = 'demo'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        update_stmt = """ UPDATE document_mgmt SET status='%s'  WHERE doc_id='%s' AND project_id='%s' """%(status, doc_ids, project_id)
        m_cur.execute(update_stmt)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]        

if __name__ == '__main__':
    d_Obj = DocStatus()
    ijson = sys.argv[1]
    try:
        ijson = json.loads(ijson)
        res = d_Obj.status_update(ijson)
        res = json.dumps(res)
        print res
    except Exception as e:
        print e
        
    
         
        

         
