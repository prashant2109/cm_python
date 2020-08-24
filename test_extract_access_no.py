import os, sys, MySQLdb, sqlite3, json
import datetime, re

def disableprint():
    return
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    return
    sys.stdout = sys.__stdout__

class Extract(object):
    def __init__(self):
        self.f_type_map = {
                            '10q':'10-Q',
                            '10k':'10-K',
                            '6k':'6-K',
                            '8k':'8-K',
                            '20f':'20-F',
                            }    
    
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



    def date_range_lst(self, start_date , end_date):
        start = datetime.datetime.strptime(start_date, "%d-%m-%Y")
        end = datetime.datetime.strptime(end_date, "%d-%m-%Y")
        date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days+1)]
        range_lst = []
        for date in date_generated:
            range_lst.append(date.strftime("%Y-%m-%d"))
        return set(range_lst)

    def except_date_range_lst(self, start_date , end_date):
        start = datetime.datetime.strptime(start_date, "%m-%d-%Y")
        end = datetime.datetime.strptime(end_date, "%m-%d-%Y")
        date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days+1)]
        range_lst = []
        for date in date_generated:
            range_lst.append(date.strftime("%Y-%m-%d"))
        return set(range_lst)
        
    def read_doc_master(self, m_conn, m_cur, company_id):
        read_qry = """ SELECT dm.doc_id, dm.filing_type, tk.ticker, dm.document_from, dm.document_to FROM document_master AS dm INNER JOIN Ticker AS tk ON dm.company_id=tk.company_id  WHERE dm.company_id={0} """.format(company_id)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        cik_res_dct = {}
        form_type_map = {}
        date_info_dct = {}
        for row in t_data[:1]:
            doc_id, form_type, ticker, doc_from, doc_to = row
            doc_id  = int(doc_id)
            cik = ticker.split(':')
            if len(cik) < 2:
                cik = cik[0].strip()
            else:
                cik = cik[1].strip()
            if (not form_type):continue
            lf = form_type.lower()
            if lf not in self.f_type_map:
                continue
            form_type = self.f_type_map[lf]
            from_d_str = doc_from #'%s-%s-%s'%(fdd, fmm, fyy)
            to_d_str   = doc_to #'%s-%s-%s'%(tdd, tmm, tyy) 
            try:
                
                drange_lst = self.date_range_lst(str(from_d_str), str(to_d_str))
            except:
                try:
                    drange_lst = self.except_date_range_lst(str(from_d_str), str(to_d_str))
                except:
                    continue
            date_info_dct.setdefault(cik, {}).setdefault(form_type, {})[(from_d_str, to_d_str)] = drange_lst
            cik_res_dct.setdefault(cik, {}).setdefault(form_type, {}).setdefault((from_d_str, to_d_str), {})[doc_id] = 1
        
        update_rows = []
        for cik_tkr, fil_dct in cik_res_dct.iteritems():
            for f_type, date_dct in fil_dct.iteritems():
                complete_urls, securl = self.extract_info_fnc(cik_tkr, f_type)
                dw_u_dct = {} 
                date_range_dct = date_info_dct[cik_tkr][f_type]
                url_dct = {}
                other_info_dct = {} 
                for url_tup in complete_urls:
                    u_date, sec_url, other_info = self.extract_period_of_report_data(url_tup, f_type)  
                    if not u_date:continue
                    ac_n = url_tup[1]
                    if isinstance(ac_n, unicode):
                        ac_n = ac_n.encode('utf-8')
                    ac_n = ac_n.replace('\xc2\xa0', ' ').replace('\xa0', ' ')
                    ac_n = ' '.join(ac_n.split())
                    ac_n = ac_n.split('Acc-no:')
                    ac_n = ac_n[1].strip().split()[0]
                    tmp_arr = []
                    for date_tup, ran_lst in date_range_dct.iteritems():
                        if u_date in ran_lst:
                            #dw_u_dct[date_tup] = ac_n
                            tmp_arr.append(date_tup)
                    if len(tmp_arr) > 1:
                        print 'More THAN 1: ', [cik_tkr, f_type, url_tup, tmp_arr]
                    if tmp_arr:
                        dw_u_dct[tmp_arr[0]] = ac_n
                        url_dct[tmp_arr[0]] = sec_url
                        other_info_dct[tmp_arr[0]] = json.dumps(other_info)

                for dt_tup, doc_set in date_dct.iteritems():
                    des_date  =  dw_u_dct.get(dt_tup)
                    if not des_date:continue
                    s_url  = url_dct.get(dt_tup)
                    oi_dct = other_info_dct.get(dt_tup)
                    for did in doc_set: 
                        update_rows.append((des_date, s_url, oi_dct, did))   
        print '################################################################################################'
        for k in update_rows:
            print k, '\n'
        sys.exit('EXIT')
        if update_rows:
            update_stmt = """ UPDATE document_master SET assension_number=?, url_info=?, other_info=? WHERE doc_id=?; """
            m_cur.executemany(update_stmt, update_rows)
            m_conn.commit()
        return 

    def update_extracted_url_doc_master(self, company_id):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test'] 
        m_conn, m_cur = self. mysql_connection(db_data_lst)       
        self.read_doc_master(m_conn, m_cur, company_id)
        m_conn.close()
        return 

    def read_company_info_comapny_mgmt(self):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test']
        tm_conn, tm_cur = self.mysql_connection(db_data_lst)
        read_qry = """  SELECT row_id, ticker, sec_cik, company_display_name FROM company_mgmt WHERE row_id>475;"""
        tm_cur.execute(read_qry)
        t_data = tm_cur.fetchall()
        res_set = {e[0]:(e[1], e[2], e[3]) for e  in t_data}
        return res_set, tm_conn, tm_cur 

    def extract_info_fnc(self, cik, form_type):
        cik         = cik.strip()
        form_type   = form_type.strip()
        complete_urls   = []
        securl = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s&type=%s&dateb=&owner=include&start=%s&count=%s'
        start_cnt  = 0
        address_info = {}
        while True:
            url    = securl%(cik, form_type, start_cnt, start_cnt+100)
            cmd = "node /root/databuilder_train_ui/tenkTraining/Company_Management/core/def14a_url.js '%s' '%s'"%(url.replace("'", ""), form_type)
            start_cnt  += 100
            #print cmd
            res = os.popen(cmd).read().strip()
            urls    = []
            try:
                urls    = json.loads(res)
            except:
                urls    = []
            if not urls[1]:
                break
            complete_urls  += map(lambda x:x+[form_type], urls[1]) 
            if not address_info: 
                address_info = urls[0]
        return complete_urls, securl, address_info 
        
    def update_company_plus_doc_meta_data(self):
        company_dct = self.read_company_info_comapny_mgmt()
            
        company_update_rows = []
        doc_insert_rows  = [] 
        for company_id, info_tup in company_dct.items()[:1]:
            ticker, sec_cik = info_tup
            complete_urls, comp_url, address_info = self.extract_info_fnc(sec_cik, '10-K')
            address_info = json.dumps(address_info)
            company_update_rows.append((comp_url, address_info, company_id))
            continue
            for url_tup in complete_urls[:]:
                u_date, sec_url, other_info = self.extract_period_of_report_data(url_tup, '10-K')  
                if not u_date:continue
                ac_n = url_tup[1]
                if isinstance(ac_n, unicode):
                    ac_n = ac_n.encode('utf-8')
                ac_n = ac_n.replace('\xc2\xa0', ' ').replace('\xa0', ' ')
                ac_n = ' '.join(ac_n.split())
                ac_n = ac_n.split('Acc-no:')
                ac_n = ac_n[1].strip().split()[0]
                other_info = json.dumps(other_info)
                doc_mt_info = {"period_of_report":u_date} 
                doc_mt_info = json.dumps(doc_mt_info)
                # UPDATE document_master SET assension_number=?, url_info=?, other_info=? WHERE doc_id=?;
                doc_data_tup = (company_id, ac_n, sec_url, other_info, doc_mt_info, 'TAS-System')
                doc_insert_rows.append(doc_data_tup)
        
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        
        print company_update_rows
        if company_update_rows:
            update_stmt = """ UPDATE company_mgmt SET extract_url=%s, meta_data=%s WHERE row_id=%s """
            m_cur.executemany(update_stmt, company_update_rows)
            
        if doc_insert_rows:
            insert_stmt = """ INSERT INTO document_master(company_id, assension_number, url_info, other_info, meta_data, user_name) VALUES(%s, %s, %s, %s, %s, %s); """
            #m_cur.executemany(insert_stmt, doc_insert_rows)
        m_conn.commit()
        m_conn.close() 
        return

    def check_industry_type_and_add(self, m_conn, m_cur, industry_type):
        print industry_type
        if not industry_type:
            return 0
        insert_stmt = """ INSERT INTO sec_industry(industryName) VALUES('%s')  """%(industry_type)
        m_cur.execute(insert_stmt)
        m_conn.commit()
        read_qry = """ SELECT ID  FROM sec_industry ORDER BY ID DESC LIMIT 1  """
        m_cur.execute(read_qry)
        it_id = m_cur.fetchone()
        it_id = it_id[0]
        return int(it_id) 

    def check_sector_and_add(self, m_conn, m_cur, industry_type):
        if not industry_type:
            return 0
        insert_stmt = """ INSERT INTO sec_sector(sector) VALUES('%s')  """%(industry_type)
        m_cur.execute(insert_stmt)
        m_conn.commit()
        read_qry = """ SELECT ID  FROM sec_sector ORDER BY ID DESC LIMIT 1  """
        m_cur.execute(read_qry)
        it_id = m_cur.fetchone()
        it_id = it_id[0]
        return int(it_id) 

    def read_industry_type(self, m_conn, m_cur):
        read_qry = """ SELECT ID, industryName FROM sec_industry; """ 
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

    def read_sector(self, m_conn, m_cur):
        read_qry = """ SELECT ID, sector FROM sec_sector """ 
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

    def extract_info_fnc(self, cik, form_type):
        cik         = cik.strip()
        form_type   = form_type.strip()
        complete_urls   = []
        securl = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s&type=%s&dateb=&owner=include&start=%s&count=%s'
        start_cnt  = 0
        address_info = {}
        while True:
            url    = securl%(cik, form_type, start_cnt, start_cnt+100)
            cmd = "node /root/databuilder_train_ui/tenkTraining/Company_Management/core/def14a_url.js '%s' '%s'"%(url.replace("'", ""), form_type)
            start_cnt  += 100
            res = os.popen(cmd).read().strip()
            urls    = []
            try:
                urls    = json.loads(res)
            except:
                urls    = []
            if not urls[1]:
                break
            complete_urls  += map(lambda x:x+[form_type], urls[1]) 
            if not address_info: 
                address_info = urls[0]
        return complete_urls, securl, address_info 
        
    def insert_business_mailing_address(self, m_conn, m_cur, bm_lst):
        insert_stmt = """ INSERT INTO company_address(company_id, type, address) VALUES(%s, %s, %s) """            
        m_cur.executemany(insert_stmt, bm_lst)
        m_conn.commit()
        return  

    def extract_period_of_report_data(self, sec_url, form_type):
        url    = sec_url
        cmd = "node /root/databuilder_train_ui/tenkTraining/Company_Management/core/sec_js/def14a_txt.js '%s' '%s'"%(url.replace("'", ""), form_type)
        res = os.popen(cmd).read().strip()
        try:
            res_info = eval(res)
        except:meta_data = {}
        return res_info, sec_url 
    
    def update_company_plus_doc_meta_data_changed(self):
        company_dct, m_conn, m_cur = self.read_company_info_comapny_mgmt()
        company_update_rows = []
        doc_insert_rows  = [] 
        f = open('/var/www/html/companies_not_matched.txt', 'r')
        f_data = f.readlines()
        f.close()

        check_set = set()
        for rd in f_data:
            if not rd:continue
            ss = rd.strip().split('-')
            try:
                cmd_di, cnm = ss
            except:continue
            check_set.add(int(cmd_di))
        
        in_t_dct = self.read_industry_type(m_conn, m_cur) 
        sec_t_dct = self.read_sector(m_conn, m_cur) 
        address_info_lst = []
        sn = 1
        all_industry = {}
        for company_id, info_tup in company_dct.items()[:]:
            ticker, sec_cik, cdn = info_tup
            print cdn, sn
            sn += 1        
            complete_urls = []            
            for form_type in ['10-K', '10-Q']:
                c_urls, comp_url, address_info = self.extract_info_fnc(sec_cik, form_type)
                complete_urls.extend(c_urls)
            cik, uc_name, address = address_info.get('cik', ''), address_info.get('name', ''), address_info.get('address', '')
            identInfo        = address_info.get('identInfo', []) 
            industry_type, sector = "", ""
            it_id, sec_id = 0, 0 
            if identInfo:
                sector = identInfo[0]
                sector = sector.split('-')[1]
                sector = re.sub(r"[^a-zA-Z0-9]+", ' ', sector)
                industry_type = identInfo[3]
                industry_type = re.sub(r"[^a-zA-Z0-9]+", ' ', industry_type)
                all_industry[industry_type] = 1
                if industry_type:
                    industry_type = industry_type.strip()
                    it_lower = ''.join(industry_type.split()).lower()
                    it_id = in_t_dct.get(it_lower, 0)
                    if not it_id:
                        it_id = self.check_industry_type_and_add(m_conn, m_cur, industry_type)
                        in_t_dct[it_lower] = it_id
                com_tup = (it_id, sec_id, company_id)
                if industry_type:
                    c_update_stmt = """ UPDATE company_mgmt SET sec_industry='%s', sec_sector='%s' WHERE row_id='%s' """%com_tup
                    m_cur.execute(c_update_stmt)
                    m_conn.commit()
        
        print in_t_dct    
        m_conn.close()
        print all_industry
        return 

if __name__ == '__main__':
    obj = Extract() 
    print obj.update_company_plus_doc_meta_data_changed()
    #print obj.update_company_plus_doc_meta_data()
    #print obj.update_extracted_url_doc_master(company_id)
    
