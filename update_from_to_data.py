#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os, datetime, json 
import compute_period_and_date
ph_Obj = compute_period_and_date.PH()

def disableprint():
    sys.stdout = open(os.devnull, 'w')
    return

def enableprint():
    sys.stdout = sys.__stdout__
    return

class Company_Info(object):
    def mysql_connection(self, db_data_lst):
        import MySQLdb
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def connect_to_sqlite(self, db_path):
        import sqlite3
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        return conn, cur
    
    def replace_company_id(self):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'upload_company_info']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT row_id FROM company_mgmt; """ 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        
        otn_cid_map = {}
        otn_cid_lst = []
        for new_cid, row in enumerate(t_data, 1001):
            o_cid = int(row[0])
            otn_cid_map[o_cid] = new_cid
            otn_cid_lst.append((new_cid, o_cid))

        u_Filing_Frequency = """ UPDATE Filing_Frequency SET company_row_id=%s WHERE company_row_id=%s; """
        m_cur.executemany(u_Filing_Frequency, otn_cid_lst)

        u_Ticker = """ UPDATE Ticker SET company_id=%s WHERE company_id=%s """
        m_cur.executemany(u_Ticker, otn_cid_lst)

        u_client_company_mgmt = """ UPDATE client_company_mgmt SET company_id=%s WHERE company_id=%s """
        m_cur.executemany(u_client_company_mgmt, otn_cid_lst)
            
        u_client_details = """ UPDATE client_details SET company_id=%s WHERE company_id=%s """
        m_cur.executemany(u_client_details, otn_cid_lst)
            
        u_company_accounting_standard = """ UPDATE company_accounting_standard SET company_id=%s WHERE company_id=%s """
        m_cur.executemany(u_company_accounting_standard, otn_cid_lst)
    
        u_company_address = """ UPDATE company_address SET company_id=%s WHERE company_id=%s """
        m_cur.executemany(u_company_address, otn_cid_lst)
        
        u_company_currency = """ UPDATE company_currency SET company_id=%s WHERE company_id=%s """
        m_cur.executemany(u_company_currency, otn_cid_lst)

        u_company_mgmt = """ UPDATE company_mgmt SET row_id=%s WHERE row_id=%s """
        m_cur.executemany(u_company_mgmt, otn_cid_lst)
             
        u_company_website = """ UPDATE company_website SET company_id=%s WHERE company_id=%s """
        m_cur.executemany(u_company_website, otn_cid_lst)

        u_document_master = """ UPDATE document_master SET company_id=%s WHERE company_id=%s """
        m_cur.executemany(u_document_master, otn_cid_lst)

        holding_and_subsidary_company = """ UPDATE holding_and_subsidary_company SET company_id=%s WHERE company_id=%s """
        m_cur.executemany(holding_and_subsidary_company, otn_cid_lst)
        
        m_conn.commit()
        m_conn.close()
        return 
    
    def check_assension_no_existence(self):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT cm.row_id, cm.company_display_name, tk.ticker FROM company_mgmt AS cm INNER JOIN client_details AS cd ON cm.row_id=cd.company_id INNER JOIN Ticker AS tk ON cm.row_id=tk.company_id WHERE cd.project_id=1; """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        
        tickert_info = {}
        for row_data in t_data:
            row_id, company_display_name, ticker = row_data
            ticker = ':'.join(map(lambda x:x.strip(), ticker.split(':')))
            tickert_info[ticker] = (company_display_name, row_id)

        db_data_lst = ['172.16.20.52', 'root', 'tas123', 'AECN_INC']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT doc_id, meta_data FROM batch_mgmt_upload; """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
    
        company_wise_dct_dct = {}
        for r_d in t_data:
            doc_id, meta_data = r_d
            if not meta_data:continue
            meta_data = eval(meta_data)    
            get_tkr = meta_data.get('Ticker', '')
            if not get_tkr:continue
            get_tkr = ':'.join(map(lambda x:x.strip(), get_tkr.split(':')))
            #SEC Accession No 
            g_comp = tickert_info.get(get_tkr, ())
            if not g_comp:continue
            user_a = meta_data.get('SEC Accession No')
            crawl_a = meta_data.get('TAS_SEC_ACC_No')

            if not user_a and crawl_a:
                user_a  = crawl_a

            if not user_a:
                cdn, rid = g_comp
                company_wise_dct_dct.setdefault((cdn, int(rid), get_tkr), set()).add(str(doc_id)) 
        
        f = open('/var/www/html/assension_existence.txt', 'w')
        for comp_tup, doc_set in company_wise_dct_dct.iteritems():
            comp_str = str(comp_tup)
            doc_str = ', '.join(doc_set)
            comp_doc_str = '\n'.join([comp_str, doc_str])
            f.write(comp_doc_str + '\n' + '*'*50 + '\n')
        f.close()
    
    def update_aecn_inc_from_to_data(self, dbname=None, doclst=[]):
        if dbname:
            db_data_lst = ['172.16.20.52', 'root', 'tas123', dbname]
        else:
            db_data_lst = ['172.16.20.52', 'root', 'tas123', 'AECN_INC']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        if doclst:
            read_qry = """ SELECT doc_id, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s)"""%(','.join(map(lambda x:str(x), doclst)))
        else:
            read_qry = """ SELECT doc_id, meta_data FROM batch_mgmt_upload""" # WHERE doc_id in (4284, 4285, 4286, 4287, 4288, 4289, 4290, 4291, 4292, 4293, 4294, 4295, 4296, 4297, 4298, 4299, 4300, 4301, 4302, 4303, 4304, 4305, 4306, 4307, 4308, 4309, 4310, 4311, 4312, 4313, 4314, 4315, 4316, 4317, 4318, 4319, 4320, 4321, 4322, 4323, 4324, 4325, 4326, 4427, 4430, 4431, 4432); """ 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()   
        
        res_dct = {}
        update_rows = []
        for row_data in t_data[:]:
            doc_id, meta_data = row_data
            #if str(doc_id) not in ['5313', '5316']:continue
            meta_data = eval(meta_data) 
            tas_sec_no = meta_data.get('TAS_SEC_ACC_No')
            #if not tas_sec_no:continue
            from_date = meta_data.get('Document From')
            to_date = meta_data.get('Document To')
            pt = meta_data.get('periodtype')
            year = meta_data.get('Year')    
            cik = meta_data.get('Ticker')
            if (not cik):continue
            cik = cik.split(':')
            if len(cik) < 2:
                cik = cik[0].strip()
            else:
                cik = cik[1].strip()
            #if cik not in ('PEBO', ''):continue
            try:
                fye =  int(meta_data.get('FYE', 0))
            except:continue
            if (not pt) or (not year) or (not fye):continue
            #res_dct[doc_id] = [from_date, to_date, fye, pt, year]
            ## {doc_id:[from_date, to_date, fye, period_type, year]}
            try:
                from_date = datetime.datetime.strptime(from_date, '%m/%d/%Y')
            except:from_date = ''
            try:
                to_date = datetime.datetime.strptime(to_date, '%m/%d/%Y')
            except:to_date = ''
        
            print (fye, pt, year, from_date, to_date)
            update_flg, f_date, t_date = ph_Obj.get_start_end_data_from_ph(fye, pt, year, from_date, to_date)
            #print (update_flg, f_date, t_date)
            f_date = f_date.strftime("%m/%d/%Y")
            t_date = t_date.strftime("%m/%d/%Y")
            print [f_date, t_date, doc_id, update_flg, meta_data]
            if update_flg:
                meta_data["Document From"]  = f_date 
                meta_data["Document To"]    = t_date
                meta_data = json.dumps(meta_data)
                dt = (meta_data, int(doc_id))
                update_rows.append(dt)
        #print update_rows, len(update_rows)
        #sys.exit()
        if update_rows:
            update_stmt = """ UPDATE batch_mgmt_upload SET meta_data=%s WHERE doc_id=%s; """
            m_cur.executemany(update_stmt, update_rows)
            m_conn.commit()
        m_conn.close()
            
        return res_dct 
            
        
if __name__ == '__main__':
    obj = Company_Info()
    obj.update_aecn_inc_from_to_data()
    ##print obj.check_assension_no_existence()
    ##print obj.replace_company_id()
