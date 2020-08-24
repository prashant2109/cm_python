import os, sys
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import config

class DocStatus(object):
    def status_update(self, ijson):
        project_id = str(ijson['project_id']) 
        #doc_ids    = ', '.join(map(str, ijson['doc_id']))
        doc_ids    = str(ijson['doc_id'])
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        update_stmt = """ UPDATE document_mgmt SET status='Y'  WHERE doc_id='%s' AND project_id='%s' """%(doc_ids, project_id)
        m_cur.execute(update_stmt)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]        
