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
    
    def update_aecn_inc_from_to_data(self):
        db_data_lst = ['172.16.20.52', 'root', 'tas123', 'AECN_INC']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT doc_id, meta_data FROM batch_mgmt_upload WHERE doc_id in (5313, 5316); """ 
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
            fye =  int(meta_data.get('FYE', 0))
            if (not pt) or (not year) or (not fye):continue
            #res_dct[doc_id] = [from_date, to_date, fye, pt, year]
            ## {doc_id:[from_date, to_date, fye, period_type, year]}
            try:
                from_date = datetime.datetime.strptime(from_date, '%m/%d/%Y')
            except:from_date = ''
            try:
                to_date = datetime.datetime.strptime(to_date, '%m/%d/%Y')
            except:to_date = ''
        
            #print (fye, pt, year, from_date, to_date)
            update_flg, f_date, t_date = ph_Obj.get_start_end_data_from_ph(fye, pt, year, from_date, to_date)
            #print (update_flg, f_date, t_date)
            f_date = f_date.strftime("%m/%d/%Y")
            t_date = t_date.strftime("%m/%d/%Y")
            print [f_date, t_date, doc_id, update_flg, meta_data]
            if update_flg:
                meta_data["Document From"]  = f_date 
                meta_data["Document To"]    = t_date
                meta_data = json.dumps(meta_data)
                dt = (meta_data, doc_id)
                update_rows.append(dt)
        #print update_rows
        if 0:#update_rows:
            update_stmt = """ UPDATE batch_mgmt_upload SET meta_data='%s' WHERE doc_id=%s; """
            m_cur.executemany(update_stmt, update_rows)
            m_conn.commit()
        m_conn.close()
        '''
        HBCP:1398 
        TBBK:1384 
        '''
        return res_dct 
        
    def doc_wo_TAS_sec(self, tikr, m_conn, m_cur):
        read_qry = """ select doc_id, meta_data from batch_mgmt_upload where meta_data like '%{0}%' AND status='10N'; """.format(tikr)
        #read_qry = """ select doc_id, meta_data from batch_mgmt_upload where meta_data like '%{0}%'; """.format(tikr)
        print read_qry
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()       
        doc_lst = []
        for row_data in t_data:
            doc_id, meta_data = row_data
            mt_dat = eval(meta_data) 
            TAS_SEC = mt_dat.get('TAS_SEC_ACC_No')
            if TAS_SEC:continue
            doc_lst.append(doc_id)
        doc_str = ', '.join(map(str, doc_lst))
        return doc_lst, doc_str 
        
    def prep_txt_file(self):
        tikr_dct = {1132:'CARO', 1113:'CFR', 1100:'FBP', 1123:'USB',
                    1119:'CMTV', 1375:'UCBI', 1395:'FMNB', 1088:'WEBK', 1371:'EWBC'}
        tikr2_dct = {1380:'FCF', 1135:'PEBO', 1099:'BBT', 1389:'FNB'}
        tikr_dct.update(tikr2_dct)
        db_data_lst = ['172.16.20.52', 'root', 'tas123', 'AECN_INC']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        s = '*'*50
        #f = open('doc_file.txt', 'w')
        #f = open('doc_file_left4.txt', 'w')
        #f = open('doc_file_2.txt', 'w')
        f = open('doc_file_all_2.txt', 'w')
        for cid, tk in tikr_dct.iteritems():
            d_lst, d_str = self.doc_wo_TAS_sec(tk, m_conn, m_cur) 
            d_lst_str = str(d_lst)
            f.write(str(cid)+':'+tk+'\n')
            f.write(d_lst_str+'\n')
            f.write(d_str+'\n')
            f.write(s+'\n\n')
        f.close() 
        m_conn.close()

if __name__ == '__main__':
    obj = Company_Info()
    obj.prep_txt_file()
    ##obj.doc_wo_TAS_sec('TBBK')
    ##obj.update_aecn_inc_from_to_data()
    ##print obj.check_assension_no_existence()
    ##print obj.replace_company_id()
