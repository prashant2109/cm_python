#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os, sets, hashlib, binascii
import lmdb, copy, json, ast 
import datetime, sqlite3, MySQLdb
import config
import json
import shutil
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import pr_company_docTablePh_details_tableType as Company_docTablePh_details 
cObj = Company_docTablePh_details.Company_docTablePh_details()

def disableprint():
    sys.stdout = open(os.devnull, 'w')
    return

def enableprint():
    sys.stdout = sys.__stdout__
    return

class PYAPI():
    def __init__(self):
        self.month_map  = {'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12}
        #self.abr_map  = {'Jan':'January', 'Feb':'February', 'Mar':'March', 'Apr':'April', 'May':'May', 'Jun':'June', 'Jul':'July', 'Aug':'August', 'Sep':'September', 'Oct':'October', 'Nov':'November', 'Dec':'December'}
        self.abr_map  = {'jan':'January', 'feb':'February', 'mar':'March', 'apr':'April', 'may':'May', 'jun':'June', 'jul':'July', 'aug':'August', 'sep':'September', 'oct':'October', 'nov':'November', 'dec':'December'}

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
        
    def read_company_meta_info_txt(self, path):
        f = open(path)
        txt_data = f.readlines()
        f.close()
        header, comp_info = txt_data[0], txt_data[1]
        header_lst, comp_info_lst = header.split('\t'), comp_info.split('\t')
        map_dct = {}
        for idx, row in enumerate(header_lst): 
            map_dct[row.strip()] = comp_info_lst[idx].strip()
        pts = ['Q1', 'Q2', 'Q3', 'Q4', 'H1', 'H2', 'FY', 'M9']
        
        pt_dct = {}
        for pt in pts:
            pt_data = map_dct.get(pt)    
            if not pt_data:continue
            pt_dct[pt]  = pt_data
        
        industry_type = map_dct.get('Industry', '')
        ticker = map_dct.get('Ticker', '')      
        acc_std = map_dct.get('Accounting Standards', '')
        from_month = map_dct.get('From Year', '')
        to_month   = map_dct.get('To Year', '') 
        return pt_dct, industry_type, ticker, acc_std, from_month, to_month
            
            
    def check_industry_type_and_add(self, m_conn, m_cur, industry_type):
        if not industry_type:
            return 0
        insert_stmt = """ INSERT INTO industrytype(industryName) VALUES('%s')  """%(industry_type)
        m_cur.execute(insert_stmt)
        m_conn.commit()
        read_qry = """ SELECT ID  FROM industrytype ORDER BY ID DESC LIMIT 1  """
        m_cur.execute(read_qry)
        it_id = m_cur.fetchone()
        it_id = it_id[0]
        return int(it_id) 

    def check_acc_std_and_add(self, m_conn, m_cur, acc_std):
        if not acc_std:
            return 0
        insert_stmt = """ INSERT INTO Accounting_Standard(accounting_standard) VALUES('%s') """%(acc_std)
        m_cur.execute(insert_stmt)
        m_conn.commit()
        read_qry = """ SELECT id FROM Accounting_Standard ORDER BY ID DESC LIMIT 1; """
        m_cur.execute(read_qry)
        as_id = m_cur.fetchone()
        as_id = as_id[0]
        return int(as_id)

    def read_industry_type(self, m_conn, m_cur):
        read_qry = """ SELECT ID, industryName FROM industrytype """ 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        in_t_dct = {}
        for row in t_data:
            row_id, i_name = row
            i_name = str(i_name)
            if not i_name:continue
            i_name = ''.join(i_name.split())
            i_name = i_name.lower()
            in_t_dct[i_name] = int(row_id)
        return in_t_dct

    def read_account_standard(self, m_conn, m_cur):
        read_qry = """ SELECT id, accounting_standard FROM Accounting_Standard """ 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        in_t_dct = {}
        for row in t_data:
            row_id, i_name = row
            i_name = str(i_name)
            if not i_name:continue
            i_name = i_name.lower()
            in_t_dct[i_name] = int(row_id)
        return in_t_dct
    
    def insert_filing_info(self, m_conn, m_cur, filing_dct):
        cn_str = ', '.join({'"'+str(e)+'"' for e in filing_dct})
        read_qry = """ SELECT row_id, company_name FROM company_mgmt  WHERE company_name in (%s) """%(cn_str)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        rid_cn_map = {row[1]:row[0]  for row in t_data}
        crid_str = ', '.join(str(es) for es in rid_cn_map.values())
        del_qry  =  """ DELETE FROM Filing_Frequency WHERE company_row_id in (%s)  """%(crid_str)
        m_cur.execute(del_qry)
        m_conn.commit()
        insert_rows = []
        for company_name, info_dct in filing_dct.iteritems():
            crid = rid_cn_map.get(company_name, 0)
            if not crid:continue
            f_str, f_dct = info_dct['f_str'], info_dct['f_dct'] 
            for r in f_str.split(', '):
                sp_lst = f_dct.get(r, '').split('-')
                fm, tm = '', ''
                try:
                    if len(sp_lst) > 1:
                        fm, tm = sp_lst
                        if fm and tm:
                                fm = self.abr_map[fm.lower().strip()] 
                                tm = self.abr_map[tm.lower().strip()]
                except:continue
                if (not fm) or (not tm):continue
                dt_tup = (company_name, crid, r, fm, tm)
                insert_rows.append(dt_tup)
        if insert_rows:
            insert_stmt = """ INSERT INTO Filing_Frequency(company_id, company_row_id, filing_type, from_month, to_month) VALUES(%s, %s, %s, %s, %s) """
            m_cur.executemany(insert_stmt, insert_rows)
            m_conn.commit()
        return
        
    def read_company_info_comapny_mgmt(self):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test']
        tm_conn, tm_cur = self.mysql_connection(db_data_lst)
        read_qry = """  SELECT company_display_name, company_name FROM company_mgmt; """
        tm_cur.execute(read_qry)
        t_data = tm_cur.fetchall()
        tm_conn.close()
        res_set = set()        
        cn_set  = set()
        for rd in t_data:
            res_set.add(rd[0])
            cn_set.add(rd[1]) 
        return res_set, cn_set 
 
    def read_data_builder_company_list(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db' 
        conn, cur  = self.connect_to_sqlite(db_path)    
        read_qry = """ SELECT company_name, company_display_name, project_id, toc_company_id, type_of_company, industry_type, reporting_year, filing_frequency, ticker FROM company_info WHERE project_id='20'; """
        cur.execute(read_qry)
        t_data =cur.fetchall()
        conn.close()
         
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
            
        in_t_dct = self.read_industry_type(m_conn, m_cur) 
        as_t_dct = self.read_account_standard(m_conn, m_cur) 
        
        filing_dct = {}
        insert_rows = []
        res_set, cn_set = self.read_company_info_comapny_mgmt() 
        #f = open('/var/www/html/ne_companies.txt', 'w')
        ticker_dct = {}
        for row in t_data[:]:
            company_name, company_display_name, project_id, toc_company_id, type_of_company, industry_type, reporting_year, filing_frequency, ticker = row 
            if (company_display_name in res_set) or (company_name in cn_set):continue
                #f.write(company_display_name+'\n') 
            #continue
            company_id = '{0}_{1}'.format(project_id, toc_company_id)
            comp_txt_path = '/mnt/eMB_db/{0}/{1}/company_meta_info.txt'.format(company_name, project_id)
            pt_dct, industry_type, ticker, acc_std, from_month, to_month = self.read_company_meta_info_txt(comp_txt_path)
            
            ############## INDUSTRY TYPE ###############
            it_lower = ''.join(industry_type.split()).lower()
            it_id = in_t_dct.get(it_lower, 0)
            if not it_id:
                it_id = self.check_industry_type_and_add(m_conn, m_cur, industry_type)
                in_t_dct[it_lower] = it_id

            ############## TICKER ###############
            as_lower = acc_std.lower()
            as_id = as_t_dct.get(as_lower, 0)
            if not as_id:
                as_id = self.check_acc_std_and_add(m_conn, m_cur, acc_std)
                as_t_dct[as_lower] = as_id
            filing_dct[company_name] = {'f_str':filing_frequency, 'f_dct':pt_dct} 
            meta_data = json.dumps({})
            print as_lower, it_lower
            insert_time = str(datetime.datetime.now())
            dt_tup = (company_name, company_display_name, meta_data, 'TAS-System', insert_time, ticker, it_id, as_id, from_month, to_month, company_id)
            insert_rows.append(dt_tup)
            ticker_dct[company_display_name] = ticker
        
        for rw in insert_rows:
            print rw, '\n' 
            
        #print len(insert_rows) 
        #print filing_dct
        #print 'TTTTTT', ticker_dct 
        #sys.exit()
        tik_len = len(insert_rows)
        if insert_rows:
            insert_stmt = """ INSERT INTO company_mgmt(company_name, company_display_name, meta_data, user_name, insert_time, ticker, industry, account_standard, financial_year_start, financial_year_end, DB_id) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            m_cur.executemany(insert_stmt, insert_rows)
            m_conn.commit()
        if filing_dct:
            self.insert_filing_info(m_conn, m_cur, filing_dct)
        if ticker_dct:
            self.insert_ticker_info(ticker_dct, m_cur, m_conn, tik_len)
        m_conn.close()
        return 
        
    def insert_ticker_info(self, ticker_dct, m_cur, m_conn, tik_len):
        read_qry = """ SELECT row_id, company_display_name FROM company_mgmt ORDER BY row_id DESC LIMIT %s;  """%(tik_len)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        check_data = {es[1]:es[0] for es in t_data}
        
        insert_rows = []
        for cdn, ticker in ticker_dct.iteritems():
            cid = check_data[cdn]
            insert_rows.append((cid, ticker))
        if insert_rows:
            insert_stmt = """ INSERT INTO Ticker(company_id, ticker) VALUES(%s, %s); """
            m_cur.executemany(insert_stmt, insert_rows)
            m_conn.commit()
        return 
        
    def read_project_20_id(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db'
        conn, cur  = self.connect_to_sqlite(db_path)
        read_qry = """ SELECT company_name, company_display_name FROM company_info WHERE project_id='20'; """
        cur.execute(read_qry)
        t_data =cur.fetchall()
        conn.close()
        res_dct = {} 
        for row in t_data[:]:
            company_name, company_display_name = row
            res_dct[company_name] = company_display_name
        return res_dct
    
    def read_data_builder_company_list_1(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db' 
        conn, cur  = self.connect_to_sqlite(db_path)    
        read_qry = """ SELECT company_name, company_display_name, project_id, toc_company_id, type_of_company, industry_type, reporting_year, filing_frequency, ticker FROM company_info WHERE project_id='1'; """
        cur.execute(read_qry)
        t_data =cur.fetchall()
        conn.close()

        check_comp = self.read_project_20_id()
         
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
            
        in_t_dct = self.read_industry_type(m_conn, m_cur) 
        as_t_dct = self.read_account_standard(m_conn, m_cur) 
        
        filing_dct = {}
        insert_rows = []
        for row in t_data[:]:
            company_name, company_display_name, project_id, toc_company_id, type_of_company, industry_type, reporting_year, filing_frequency, ticker = row 
            c_name = check_comp.get(company_name, '')
            if c_name:continue
            #print company_name
            #continue
            company_id = '{0}_{1}'.format(project_id, toc_company_id)
            comp_txt_path = '/mnt/eMB_db/{0}/{1}/company_meta_info.txt'.format(company_name, project_id)
            pt_dct, industry_type, ticker, acc_std, from_month, to_month = self.read_company_meta_info_txt(comp_txt_path)
            
            ############## INDUSTRY TYPE ###############
            it_lower = ''.join(industry_type.split()).lower()
            it_id = in_t_dct.get(it_lower, 0)
            if not it_id:
                it_id = self.check_industry_type_and_add(m_conn, m_cur, industry_type)
                in_t_dct[it_lower] = it_id

            ############## TICKER ###############
            as_lower = acc_std.lower()
            as_id = as_t_dct.get(as_lower, 0)
            if not as_id:
                as_id = self.check_acc_std_and_add(m_conn, m_cur, acc_std)
                as_t_dct[as_lower] = as_id
            filing_dct[company_name] = {'f_str':filing_frequency, 'f_dct':pt_dct} 
            meta_data = json.dumps({})
            insert_time = str(datetime.datetime.now())
            dt_tup = (company_name, company_display_name, meta_data, 'TAS-System', insert_time, ticker, it_id, as_id, from_month, to_month)
            insert_rows.append(dt_tup)
        #sys.exit()
        if insert_rows:
            insert_stmt = """ INSERT INTO company_mgmt(company_name, company_display_name, meta_data, user_name, insert_time, ticker, industry, account_standard, financial_year_start, financial_year_end) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            m_cur.executemany(insert_stmt, insert_rows)
            m_conn.commit()
        if filing_dct:
            self.insert_filing_info(m_conn, m_cur, filing_dct)
        m_conn.close()
        return 
        
            
    def update_currency(self):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        read_qry = """ SELECT id, currency, code FROM Currency; """ 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        
        insert_rows = []    
        update_rows = []
        for row in t_data[1:]:
            rid, currency, code = row
            code_lst = eval(code)   
            if len(code_lst) > 1:
                ilst = [(str(currency), str(r)) for r in code_lst[1:]]
                insert_rows.extend(ilst)
                dt_tup = (str(code_lst[0]), int(rid))
                update_rows.append(dt_tup)                
            elif len(code_lst) == 1:
                cd = code_lst[0]
                dt_tup = (str(cd), int(rid))
                update_rows.append(dt_tup)
        print insert_rows
        print update_rows 
        #sys.exit()
        if insert_rows:
            insert_stmt = """ INSERT INTO Currency(currency, code) VALUES(%s, %s) """
            m_cur.executemany(insert_stmt, insert_rows)
        if update_rows:
            update_stmt = """ UPDATE Currency SET code=%s WHERE id=%s """
            m_cur.executemany(update_stmt, update_rows)
        m_conn.commit()
        m_conn.close()
        return 

    def read_comp_row_id_map(self, m_cur, m_conn):
        read_qry = """ SELECT row_id, company_display_name FROM company_mgmt WHERE row_id > 1555; """ 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
            
        comp_cid_dct = {}
        for row in t_data:
            cid, company_name = row
            comp_cid_dct[company_name] = int(cid)
        return comp_cid_dct
        
    def insert_document_master(self, m_conn, m_cur, db_path, cid):
        conn, cur  = self.connect_to_sqlite(db_path)
        read_qry = """ SELECT doc_id, document_type, filing_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to, doc_download_date, doc_prev_release_date, doc_next_release_date FROM company_meta_info; """ 
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        insert_rows = []
        for row in t_data:
            doc_id, document_type, filing_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to, doc_download_date, doc_prev_release_date, doc_next_release_date = row
            if doc_release_date and '-' in doc_release_date:
                drd = doc_release_date
                drd = '-'.join(drd.split('-')[::-1])
            else:   
                drd = '1970-01-01'

            if doc_from and '-' in doc_from:
                df = doc_from
                df = '-'.join(df.split('-')[::-1])
            else:   
                df = '1970-01-01'

            if doc_to and '-' in doc_to:
                dt = doc_to
                dt = '-'.join(dt.split('-')[::-1])
            else:   
                dt = '1970-01-01'

            if doc_download_date and '-' in doc_download_date:
                ddd = doc_download_date
                ddd = '-'.join(ddd.split('-')[::-1])
            else:
                ddd = '1970-01-01' 

            if doc_prev_release_date and '-' in doc_prev_release_date:
                dprd = doc_prev_release_date
                dprd = '-'.join(dprd.split('-')[::-1])
            else:
                dprd = '1970-01-01' 

            if doc_next_release_date and '-' in doc_next_release_date:
                dnrd = doc_next_release_date
                dnrd = '-'.join(dnrd.split('-')[::-1])
            else:
                dnrd = '1970-01-01'
            
            dt_tup = (cid, document_type, filing_type, period, reporting_year, doc_name, drd, df, dt, ddd, dprd, dnrd, 'TAS-System')
            insert_rows.append(dt_tup)
        
        e_cnt = 0
        for k in insert_rows:
            print k, '\n'
            insert_stmt = """ INSERT INTO document_master(company_id, document_type, filing_type, period, year, document_name, document_release_date, document_from, document_to, document_download_date, previous_release_date, next_release_date, user_name) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') """%(k)
            try:
                m_cur.execute(insert_stmt)
            except:
                print 'SSSSSSSS'    
                e_cnt += 1
                #continue
            m_conn.commit()
        #if insert_rows:
        #    insert_stmt = """ INSERT INTO document_master(company_id, document_type, filing_type, period, year, document_name, document_release_date, document_from, document_to, document_download_date, previous_release_date, next_release_date, user_name) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
        #    m_cur.executemany(insert_stmt, insert_rows)
        #    m_conn.commit()
        return 
         
    def insert_doc_info_db_20(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db' 
        conn, cur  = self.connect_to_sqlite(db_path)    
        read_qry = """ SELECT company_name, company_display_name, project_id FROM company_info WHERE project_id='20'; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
         
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        comp_cid_dct  = self.read_comp_row_id_map(m_cur, m_conn)
        
        filing_dct = {}
        insert_rows = []
        for row in t_data[:]:
            cn, company_name, project_id  = row 
            db_path = '/mnt/eMB_db/{0}/{1}/tas_company.db'.format(cn, project_id)
            cid = comp_cid_dct.get(company_name)
            if not cid:continue
            print cid
            self.insert_document_master(m_conn, m_cur, db_path, cid)
        m_conn.close()

    def insert_doc_info_db_1(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db' 
        conn, cur  = self.connect_to_sqlite(db_path)    
        read_qry = """ SELECT company_name, company_display_name, project_id FROM company_info WHERE project_id='1'; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()

        check_comp = self.read_project_20_id()

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        comp_cid_dct  = self.read_comp_row_id_map(m_cur, m_conn)
        
        filing_dct = {}
        insert_rows = []
        for row in t_data[1:]:
            cn, company_name, project_id  = row 
            c_name = check_comp.get(cn, '')
            if c_name:continue
            db_path = '/mnt/eMB_db/{0}/{1}/tas_company.db'.format(cn, project_id)
            cid = comp_cid_dct.get(company_name)
            if not cid:continue
            try:
                self.insert_document_master(m_conn, m_cur, db_path, cid)    
            except:
                continue
        m_conn.close()

    def cp_read_comp_row_id_map(self, m_cur, m_conn):
        read_qry = """ SELECT row_id, company_display_name FROM company_mgmt WHERE row_id > 201  """ 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
            
        comp_cid_dct = {}
        for row in t_data:
            cid, company_name = row
            comp_cid_dct[company_name] = int(cid)
        return comp_cid_dct
        
    def company_wise_copy_docs(self, project_id, deal_id, ncid):
        o_path = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/input'%(project_id, deal_id)
        n_path = '/var/www/html/Company_Docs/input/%s'%(ncid)
        print [project_id, deal_id, ncid, o_path, '\t', n_path]
        try:
            os.system('rm -rf %s'%(n_path))
        except:s= 1
        ds = shutil.copytree(o_path, n_path)
        return 

    def copy_docs_20(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db' 
        conn, cur  = self.connect_to_sqlite(db_path)    
        read_qry = """ SELECT company_name, company_display_name, project_id, toc_company_id FROM company_info WHERE project_id='20'; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
         
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        comp_cid_dct  = self.read_comp_row_id_map(m_cur, m_conn)
        
        filing_dct = {}
        insert_rows = []
        for row in t_data[70:144]:
            cn, company_name, project_id, deal_id  = row 
            db_path = '/mnt/eMB_db/{0}/{1}/tas_company.db'.format(cn, project_id)
            cid = comp_cid_dct.get(company_name)
            if not cid:continue
            print cid
            try:
                self.company_wise_copy_docs(project_id, deal_id, cid)
            except:continue
        m_conn.close()

    def copy_docs_1(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db' 
        conn, cur  = self.connect_to_sqlite(db_path)    
        read_qry = """ SELECT company_name, company_display_name, project_id FROM company_info WHERE project_id='1'; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()

        check_comp = self.read_project_20_id()

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        comp_cid_dct  = self.read_comp_row_id_map(m_cur, m_conn)
        
        filing_dct = {}
        insert_rows = []
        for row in t_data[50:]:
            cn, company_name, project_id  = row 
            c_name = check_comp.get(cn, '')
            if c_name:continue
            db_path = '/mnt/eMB_db/{0}/{1}/tas_company.db'.format(cn, project_id)
            cid = comp_cid_dct.get(company_name)
            if not cid:continue
            self.insert_document_master(m_conn, m_cur, db_path, cid)
        m_conn.close()
            
    def get_distinct_docs(self, m_conn, m_cur, ncid, company_id, tm_conn, tm_cur):
        all_docs = cObj.getDistinct_docIds(company_id)
        docs_str = ', '.join([str(r) for r in all_docs])
        read_qry = """ SELECT doc_id, status, batch, doc_name, doc_type, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s) """%(docs_str) 
        tm_cur.execute(read_qry) 
        t_data = tm_cur.fetchall()
        insert_rows = []
        for row in t_data:
            doc_id, status, batch, doc_name, doc_type, meta_data = row
            if meta_data:
                meta_data   = eval(meta_data)
            else:
                meta_data   = {}
            filing_type     = meta_data.get("FilingType", "")
            period_type     = meta_data.get("periodtype", "")
            year            = meta_data.get("Year", "")
            document_name   = meta_data.get("DocumentName", "")
            drd             = meta_data.get("Document Release Date", "") 
            if drd:
                drd_lst = drd.split('/')
                if len(drd_lst) > 1:
                    mm, dd, yy = drd_lst
                    drd = '{0}-{1}-{2}'.format(dd, mm, yy)
                else:
                    drd = "" 
            df              = meta_data.get("Document From", "")
            if df:
                df_lst = df.split('/')
                if len(df_lst) > 1:
                    mm, dd, yy = df_lst
                    df = '{0}-{1}-{2}'.format(dd, mm, yy)
                else:
                    df = "" 
            dt              = meta_data.get("Document To", "")
            if dt:
                dt_lst = dt.split('/')
                if len(dt_lst) > 1:
                    mm, dd, yy = dt_lst
                    dt = '{0}-{1}-{2}'.format(dd, mm, yy)
                else:
                    dt = "" 
            ddd             = meta_data.get("Document Download Date", "")
            if ddd:
                ddd_lst = ddd.split('/')
                if len(ddd_lst) > 1:
                    mm, dd, yy = ddd_lst
                    ddd = '{0}-{1}-{2}'.format(dd, mm, yy)
                else:
                    ddd = "" 
            prd             = meta_data.get("PreviousReleaseDate", "")
            if drd:
                prd_lst = prd.split('/')
                if len(prd_lst) > 1:
                    mm, dd, yy = prd_lst
                    prd = '{0}-{1}-{2}'.format(dd, mm, yy)
                else:
                    prd = "" 
            nrd             = meta_data.get("NextReleaseDate", "") 
            if nrd:
                nrd_lst = nrd.split('/')
                if len(nrd_lst) > 1:
                    mm, dd, yy = nrd_lst
                    nrd = '{0}-{1}-{2}'.format(dd, mm, yy)
                else:
                    nrd = "" 
            language         = meta_data.get("Language", "")
            doc_type         = meta_data.get("DocType", "")
            dt_tup  = (doc_id, ncid, doc_type, filing_type, period_type, year, document_name, drd, df, dt, ddd, prd, nrd, 'TAS-System')
            insert_rows.append(dt_tup)      
        #sys.exit()
        if insert_rows:
            insert_stmt = """ INSERT INTO document_master(doc_id, company_id, document_type, filing_type, period, year, document_name, document_release_date, document_from, document_to, document_download_date, previous_release_date, next_release_date, user_name) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            m_cur.executemany(insert_stmt, insert_rows)
            m_conn.commit()
        return       

    def doc_meta_data_20(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db' 
        conn, cur  = self.connect_to_sqlite(db_path)    
        read_qry = """ SELECT company_name, company_display_name, project_id, toc_company_id FROM company_info WHERE project_id='20'; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
         
        db_data_lst = ['172.16.20.52', 'root', 'tas123', 'AECN_INC']
        tm_conn, tm_cur = self.mysql_connection(db_data_lst)

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        comp_cid_dct  = self.read_comp_row_id_map(m_cur, m_conn)
        
        filing_dct = {}
        insert_rows = []
        for row in t_data[1:144]:
            cn, company_name, project_id, deal_id  = row 
            company_id = '%s_%s'%(project_id, deal_id)
            cid = comp_cid_dct.get(company_name)
            if not cid:continue
            try:
                self.get_distinct_docs(m_conn, m_cur, cid, company_id, tm_conn, tm_cur)
                print cid
            except:
                print "Error : %s"%(cid)
                continue
        m_conn.close()
        
    def read_company_info_comapny_mgmt_lower(self):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test']
        tm_conn, tm_cur = self.mysql_connection(db_data_lst)
        read_qry = """  SELECT company_display_name FROM company_mgmt; """
        tm_cur.execute(read_qry)
        t_data = tm_cur.fetchall()
        res_set = set()
        for row in t_data:
            c_name = ''
            for e in row[0]:
                if e.isalnum():
                    c_name += e
            res_set.add(c_name.lower())
        return res_set, tm_conn, tm_cur 
        
    def company_xl_info(self):
        comp_set, m_conn, m_cur = self.read_company_info_comapny_mgmt_lower()
        #print comp_set
        import common.xlsxReader as xl_py
        x_Obj = xl_py.xlsxReader()
        data = x_Obj.readExcel('/var/www/html/BankRatioRound2.xlsx')
        insert_rows = []
        for row in data['Sheet1'][1:]:
            sec_cik, ticker, comp_name, market_cap , exchange = row
            #if comp_name != 'Citigroup Inc.':continue
            cmp_name_alnum = ''.join([es for es in comp_name if es.isalnum()])
            comp_name_lower = cmp_name_alnum.lower()
            if comp_name_lower in comp_set:
                continue
            #print [comp_name_lower, comp_name]
            ticker_info = ' : '.join((exchange.strip(), ticker.strip())) 
            c_name = ''
            for elm in comp_name:
                if elm.isalnum():
                    c_name += elm
            company_name = c_name[:]
            dt_time = str(datetime.datetime.now())
            data_tup = (company_name, comp_name, "{}", 'TAS-System', dt_time, ticker_info, sec_cik)
            insert_rows.append(data_tup)
        
        if insert_rows: 
            insert_stmt = """ INSERT INTO company_mgmt(company_name, company_display_name, meta_data, user_name, insert_time, ticker, sec_cik) VALUES(%s, %s, %s, %s, %s, %s, %s) """
            m_cur.executemany(insert_stmt, insert_rows)
            m_conn.commit()
        m_conn.close()
        
    def insert_into_ticker_table(self, m_conn, m_cur, update_rows):
        insert_stmt = """ INSERT INTO Ticker(company_id, ticker) VALUES(%s, %s) """
        m_cur.executemany(insert_stmt, update_rows)
        m_conn.commit()
        m_conn.close()
        return 
    
    def update_ticker(self):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT row_id, ticker FROM company_mgmt WHERE row_id in (SELECT company_id FROM client_company_mgmt); """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()   
            
        update_rows = []
        for row_data in t_data: 
            cid, ticker = row_data
            update_rows.append((cid, ticker))
            #ticker_id = self.insert_into_ticker_table(m_conn, m_cur, cid, ticker)
        self.insert_into_ticker_table(m_conn, m_cur, update_rows)
        
    def update_client_id(self):
        import common.xlsxReader as xls
        x_Obj = xls.xlsxReader()
        xl_data = x_Obj.readExcel('/var/www/html/kpcs.xlsx')
        p_1 = xl_data['Phase 2'][1:] 
        p_2 = xl_data['Phase 3'][1:]
        cik_company_map = {}
        for phase_lst in [p_1, p_2]:    
            for dt_arr in phase_lst:
                cik, clt_id, clt_name = dt_arr[0], dt_arr[7], dt_arr[2] 
                if cik == '#N/A':continue
                #if '=' in clt_id:continue
                if not cik:continue
                if not clt_id:continue
                cik_company_map[str(cik)] = (clt_id, clt_name)
        #cik_company_str = ', '.join(['"'+str(e)+'"'for e  in cik_company_map.keys()])
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        #read_qry = """ SELECT row_id, sec_cik FROM company_mgmt WHERE sec_cik in (%s); """%(cik_company_str)
        read_qry = """ SELECT row_id, sec_cik FROM company_mgmt; """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        
        insert_rows = []
        for row_data in t_data:
            row_id, sec_cik = row_data 
            if not sec_cik:continue
            try:
                sec_cik = str(int(sec_cik))
                if sec_cik not in cik_company_map:continue
            except:continue
            d_tup = cik_company_map[sec_cik]
            d_tup = d_tup + (row_id, 1)
            insert_rows.append(d_tup)
        if insert_rows:
            t_stmt = """ TRUNCATE TABLE client_details;  """ 
            m_cur.execute(t_stmt)
            insert_stmt = """ INSERT INTO client_details(client_id, client_name, company_id, project_id) VALUES(%s, %s, %s, %s); """
            m_cur.executemany(insert_stmt, insert_rows)
            m_conn.commit()
        m_conn.close()
        
    def read_all_companies(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db'
        conn, cur  = self.connect_to_sqlite(db_path)    
        read_qry = """ SELECT company_name, company_display_name, project_id, toc_company_id FROM company_info WHERE project_id not in ('1', '60', '80', '34'); """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        doc_cid_map = {}
        for row_data in t_data:
            company_name, company_display_name, project_id, toc_company_id = row_data
            company_id = '{0}_{1}'.format(project_id, toc_company_id)
            doc_path = '/mnt/eMB_db/{0}/{1}/tas_company.db'.format(company_name, project_id)
            dconn, dcur  = self.connect_to_sqlite(doc_path)
            read_doc_name = """ SELECT doc_name FROM company_meta_info; """ 
            try:
                dcur.execute(read_doc_name)
                dt_data = dcur.fetchall()
            except:continue
            dconn.close()
            doc_dct = {es[0]:(company_id, company_name) for es in dt_data}
            doc_cid_map.update(doc_dct)
        return doc_cid_map
        
    def update_data_builder_id_compny_mgmt(self):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT dm.document_name, cm.row_id, cm.company_name FROM document_master as dm INNER JOIN company_mgmt as cm ON dm.company_id=cm.row_id; """ 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        
        doc_cid_map = self.read_all_companies()
        cid_set_map = set(doc_cid_map)            
    
        cm_doc_rid_dct = {}
        for row_data in t_data:
            doc_name, row_id, c_name = row_data
            if not doc_name:continue
            doc_name = doc_name +'.pdf' 
            cm_doc_rid_dct.setdefault((row_id, c_name), set()).add(doc_name)
        

        #print 'RRRRRRRRRR', cid_set_map 
        update_rows = [] 
        for c_cn_tup, dc_set in cm_doc_rid_dct.iteritems():
            rid, cn = c_cn_tup
            #print dc_set, '\n'
            intersect_data = cid_set_map.intersection(dc_set)
            #print 'SSSSSSSSSSSSSSSSSSS', intersect_data, '\n'
            #continue
            if intersect_data:
                el= intersect_data.pop() 
                cid_gt_tup = doc_cid_map.get(el, ()) 
                #print cid_gt_tup
                if cid_gt_tup:
                    cid_gt, old_cn = cid_gt_tup
                    up_dt = (cid_gt, rid)
                    print up_dt, '\n'
                    update_rows.append(up_dt)
        if update_rows:
            update_stmt = """ UPDATE company_mgmt SET DB_Id=%s WHERE row_id=%s; """
            m_cur.executemany(update_stmt, update_rows)    
            m_conn.commit()
        m_conn.close()
        return 
        
    def update_client_id_info(self):
        db_path = '/mnt/eMB_db/user_wise_company_details.db'
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = """ SELECT company_name FROM user_wise_company_details WHERE project_name='Key Banking Capital and Profitability Figures' AND project_id='20'; """
        cur.execute(read_qry)   
        t_data = cur.fetchall()
        conn.close()
        
        check_data = {es[0] for es in t_data}    
        
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        
        read_qry = """ SELECT row_id, company_name FROM company_mgmt; """
        m_cur.execute(read_qry)
        et_data = m_cur.fetchall()
            
        cm_qry = """ SELECT company_id FROM client_company_mgmt;  """
        m_cur.execute(cm_qry)
        mt_data = m_cur.fetchall()
        client_chk_set = {es[0] for es in mt_data}
        
        insert_rows = []
        for row_data in et_data:
            row_id, company_name = row_data
            if company_name in check_data:
                if row_id not in client_chk_set:
                    insert_rows.append((row_id, 1))
        if insert_rows:
            insert_stmt = """ INSERT INTO client_company_mgmt(company_id, client_id) VALUES(%s, %s); """
            m_cur.executemany(insert_stmt, insert_rows)
            m_conn.commit()
        m_conn.close()
                 
if __name__ == '__main__':
    obj = PYAPI()
    ## print obj.update_client_id_info() #UPDATE client_company_mgmt with new companies
    #print obj.update_data_builder_id_compny_mgmt()
    #print obj.update_client_id()
    #print obj.update_ticker()
    #print obj.company_xl_info()
    #print obj.doc_meta_data_20()
    #print obj.insert_doc_info_db_20()
    #print obj.insert_doc_info_db_1()
    #print obj.copy_docs_20()
    ##print obj.read_data_builder_company_list()
    #print obj.read_data_builder_company_list_1()
    #print obj.update_currency()
