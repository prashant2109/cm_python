#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os, sets, hashlib, binascii
import lmdb, copy, json, ast 
import datetime, sqlite3, MySQLdb
import config
import json
import shutil

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
        
    def get_company_name(self, old_cid):
        project_id, deal_id = old_cid.split('_') 
        db_path = '/mnt/eMB_db/company_info/compnay_info.db'
        conn, cur  = self.connect_to_sqlite(db_path)
        read_qry = """ SELECT company_name, company_display_name FROM company_info WHERE project_id='%s' AND toc_company_id='%s'; """%(project_id, deal_id)
        print read_qry
        cur.execute(read_qry)
        t_data =cur.fetchone()
        company_name, cdn = '', ''
        print t_data
        if t_data:
            company_name, cdn = t_data
        return company_name, cdn, conn, cur, project_id
        
    def update_company_meta_info_txt(self, ijson):
        new_cid = ijson['rid']
        old_cid = ijson['DB_id']
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test']
        m_conn, m_cur = self.mysql_connection(db_data_lst)

        read_qry = """ SELECT ticker, industry, account_standard, financial_year_start, financial_year_end, DB_id FROM company_mgmt WHERE row_id='%s' """%(new_cid)
        m_cur.execute(read_qry)
        mt_data = m_cur.fetchone()
        act_ticker, industry_id, account_standard_id, fys_m, fye_m, DB_id = mt_data

        ind_qry = """ SELECT industryName FROM industrytype WHERE ID='%s'; """%(industry_id)
        m_cur.execute(ind_qry)
        i_data =  m_cur.fetchone()
        try:
            industry_name = i_data[0]
        except:industry_name = ''
        
        tic_qry = """ SELECT ticker  FROM Ticker WHERE company_id='%s'; """%(new_cid)
        m_cur.execute(tic_qry)
        tic_data =  m_cur.fetchone()
        ticker = tic_data[0]
        
        
        as_qry = """ SELECT accounting_standard FROM Accounting_Standard WHERE id='%s'; """%(account_standard_id)
        m_cur.execute(as_qry)
        as_data =  m_cur.fetchone()
        accounting_standard = ''
        if as_data:
            accounting_standard = as_data[0]
        
        
        ff_qry = """ SELECT filing_type, from_month, to_month  FROM Filing_Frequency WHERE company_row_id='%s' """%(new_cid)
        m_cur.execute(ff_qry)
        ff_data = m_cur.fetchall()
        m_conn.close()
        
        # ticker, accounting_standards, industry_name, fys_m, fye_m
            
        ff_dct_l = {}
        for row_data in ff_data:
            filing_type, from_month, to_month = row_data    
            ff_dct_l[filing_type] = '-'.join((from_month, to_month))
        
        Q1, Q2, Q3, Q4, H1, H2, M9, FY = '', '', '', '', '', '', '', ''
        ff_dct = {'Q':[], 'H':[], 'Y':[], 'M':[]}
        if  ff_dct_l.get("Q1", ''):
            Q1 = ' - '.join([e.strip()[:3] for e in ff_dct_l["Q1"].split('-')])
            ff_dct['Q'].append('Q1')
        if  ff_dct_l.get("Q2", ''):
            Q2 = ' - '.join([e.strip()[:3] for e in ff_dct_l["Q2"].split('-')])
            ff_dct['Q'].append('Q2')
        if  ff_dct_l.get("H1", ''):
            H1 = ' - '.join([e.strip()[:3] for e in ff_dct_l["H1"].split('-')])
            ff_dct['H'].append('H1')
        if  ff_dct_l.get("Q3", ''):
            Q3 = ' - '.join([e.strip()[:3] for e in ff_dct_l["Q3"].split('-')])
            ff_dct['Q'].append('Q3')
        if  ff_dct_l.get("M9", ''):
            M9 = ' - '.join([e.strip()[:3] for e in ff_dct_l["M9"].split('-')])
            ff_dct['M'].append('M9')
        if  ff_dct_l.get("Q4", ''):
            Q4 = ' - '.join([e.strip()[:3] for e in ff_dct_l["Q4"].split('-')])
            ff_dct['Q'].append('Q4')
        if  ff_dct_l.get("H2", ''):
            H2 = ' - '.join([e.strip()[:3] for e in ff_dct_l["H2"].split('-')])
            ff_dct['H'].append('H2')
        if  ff_dct_l.get("FY", ''):
            FY = ' - '.join([e.strip()[:3] for e in ff_dct_l["FY"].split('-')])
            ff_dct['Y'].append('FY')
        
        res_ff_lst, ff = [], []
        for pt  in ['Q', 'H', 'Y', 'M']:#ff_dct.iteritems():
            pt_lst = ff_dct[pt]
            flg_lst = ['false']
            if pt_lst:
                flg_lst = ['true']
                ff += pt_lst
            dt_str = '#'.join(flg_lst+pt_lst)
            res_ff_lst.append(dt_str)
            
        txt_ff = '_'.join(res_ff_lst)
        ff_str = ', '.join(ff)
        
        sh = ['ER DATE', 'Q1', 'Q2', 'Q3', 'Q4', 'H1', 'H2', 'FY', 'M9', 'Filing Frequency', 'Ticker', 'Accounting Standards','Industry','From Year','To Year']
        hdr_str = '\t'.join(sh)

        dt_pr = ['', Q1, Q2, Q3, Q4, H1, H2, FY, M9, txt_ff, ticker, '', industry_name, fys_m, fye_m]
        dt_pr = map(str, dt_pr)
        
        print dt_pr
        company_name, cdn, conn, cur, model_number = self.get_company_name(old_cid)        
        if not company_name:
            return [{'message':'company does not exists in Data Builder'}]
    
        val_str = '\t'.join(dt_pr)
        txt_path = '/mnt/eMB_db/%s/%s/company_meta_info.txt'%(company_name, model_number) 
        print txt_path
        f1 = open(txt_path, 'w')
        print hdr_str
        print val_str
        f1.write(hdr_str + '\n')
        f1.write(val_str)  
        f1.close() 
        
        update_stmt = """ UPDATE company_info SET industry_type='%s', filing_frequency='%s', reporting_year='%s' WHERE project_id='%s'  and company_name='%s' """%(industry_name, ff_str, FY, model_number, company_name)
        print update_stmt
        cur.execute(update_stmt)
        conn.commit()
        conn.close()
        return [{'message':'done'}] 

if __name__ == '__main__':
    obj = PYAPI()
