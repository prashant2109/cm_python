import os, sys, MySQLdb, sqlite3, json
import datetime
def disableprint():
    return
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    return
    sys.stdout = sys.__stdout__
#{u'': 1, u'10k': 1, u'6k': 1, u'8K': 1, u'6K': 1, u'KIFRS': 1, u'20F': 1}
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
        
    def read_cik_text_file(self):
        f = open('cid_2.txt')
        data = f.readlines()
        f.close() 
        cik_set = {r.strip() for r in data}
        return cik_set
        
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
            print cmd
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

    def extract_period_of_report_data(self, url_tup, form_type):
        print url_tup
        sec_url  = url_tup[0]
        url    = sec_url
        #cmd = "node /var/www/html/WealthXRender/hostids/def14a_txt.js '%s' '%s'"%(url.replace("'", ""), form_type)
        cmd = "node /root/databuilder_train_ui/tenkTraining/Company_Management/core/sec_js/def14a_txt.js '%s' '%s'"%(url.replace("'", ""), form_type)
        res = os.popen(cmd).read().strip()
        #print ['RRRRRRRRRRRRRRRRRRR', res]
        #sys.exit()
        try:
            res = eval(res)
            u_date = res[0][3]["period of report"]
        except:u_date = ''
        return u_date, sec_url 
            
    def read_batch_cik_doc_info(self):
        cik_set = self.read_cik_text_file()
        db_data_lst = ['172.16.20.52', 'root', 'tas123', 'AECN_INC']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT doc_id, meta_data, batch FROM batch_mgmt_upload; """  
        m_cur.execute(read_qry)
        mt_data  = m_cur.fetchall()
        
        cik_res_dct = {}
        form_type_map = {}
        for row in mt_data:
            doc_id = int(row[0]) 
            try:
                meta_data = json.loads(row[1])
            except:continue
            batch = row[2]
            cik = meta_data.get('Ticker')
            if (not cik):continue
            form_type = meta_data.get('FilingType')
            if not form_type:continue
            lf = form_type.lower()
            if lf not in self.f_type_map:
                print form_type
                continue
            form_type = self.f_type_map[lf]
            cik = cik.split(':')
            if len(cik) < 2:
                cik = cik[0].strip()
            else:
                cik = cik[1].strip()
            print [cik, form_type]
            if (cik not in cik_set) or (not form_type):continue
            filed_date = meta_data.get('Document Release Date')
            dst = filed_date.split('/')
            if len(dst) != 3:continue
            dd, mm, yy = dst
            filed_date = '%s-%s-%s'%(yy, mm, dd) 
            cik_res_dct.setdefault(cik, {}).setdefault(form_type, {}).setdefault(filed_date, {})[doc_id] = (meta_data, batch)

        update_rows = []
        txt_lst = []
        meta_data_lst = []
        for cik_tkr, fil_dct in cik_res_dct.iteritems():
            for f_type, date_dct in fil_dct.iteritems():
                complete_urls, securl, ad_i = self.extract_info_fnc(cik_tkr, f_type)
                dw_u_dct = {} 
                for url_tup in complete_urls:
                    u_date = url_tup[2] 
                    if not u_date:continue
                    ac_n = url_tup[1]
                    if isinstance(ac_n, unicode):
                        ac_n = ac_n.encode('utf-8')
                    ac_n = ac_n.replace('\xc2\xa0', ' ').replace('\xa0', ' ')
                    ac_n = ' '.join(ac_n.split())
                    ac_n = ac_n.split('Acc-no:')
                    ac_n = ac_n[1].strip().split()[0]
                    dw_u_dct[u_date] = ac_n
                for dt, doc_set in date_dct.iteritems():
                    des_date  =  dw_u_dct.get(dt)
                    if not des_date:continue
                    for did, mt_dct_tup in doc_set.iteritems():
                        mt_dct = mt_dct_tup[0]
                        bth = mt_dct_tup[1]
                        d_n = mt_dct.get('DocumentName', "")
                        rt_str = '\t'.join((bth, cik_tkr, str(did), d_n, des_date)) 
                        txt_lst.append(rt_str)
                        mt_dct['TAS_SEC_ACC_No'] = des_date
                        update_rows.append((json.dumps(mt_dct), did))
                        print did, mt_dct, '\n'
                        meta_data_lst.append('\t'.join((str(did), json.dumps(mt_dct))))
            
        if update_rows:
            update_stmt = """ UPDATE batch_mgmt_upload SET meta_data=%s WHERE doc_id=%s  """   
            m_cur.executemany(update_stmt, update_rows)
            m_conn.commit()
        m_conn.close() 
        f = open('/var/www/html/tkr_acc.txt', 'w')
        for rs in txt_lst:    
            f.write(rs+'\n')
        f.close()

        f = open('/var/www/html/meta_acc.txt', 'w')
        for rs in meta_data_lst:
            f.write(rs+'\n')
        f.close()

    def except_date_range_lst(self, start_date , end_date):
        start = datetime.datetime.strptime(start_date, "%m-%d-%Y")
        end = datetime.datetime.strptime(end_date, "%m-%d-%Y")
        date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days+1)]
        range_lst = []
        for date in date_generated:
            range_lst.append(date.strftime("%Y-%m-%d"))
        return set(range_lst)
        
    def date_range_lst(self, start_date , end_date):
        start = datetime.datetime.strptime(start_date, "%d-%m-%Y")
        end = datetime.datetime.strptime(end_date, "%d-%m-%Y")
        date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days+1)]
        range_lst = []
        for date in date_generated:
            range_lst.append(date.strftime("%Y-%m-%d"))
        return set(range_lst)
        
    

    def ft_read_batch_cik_doc_info(self):
        cik_set = self.read_cik_text_file()
        #print cik_set
        #sys.exit()

        db_data_lst = ['172.16.20.52', 'root', 'tas123', 'AECN_INC']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT doc_id, meta_data, batch FROM batch_mgmt_upload; """  
        m_cur.execute(read_qry)
        mt_data  = m_cur.fetchall()
        
        cik_res_dct = {}
        form_type_map = {}
        date_info_dct = {}        

        for row in mt_data:
            doc_id = int(row[0]) 
            #if doc_id != 5177:continue
            if doc_id not in (5313, ):continue
            try:
                meta_data = json.loads(row[1])
            except:continue
            batch = row[2]
            cik = meta_data.get('Ticker')
            if (not cik):continue
            
            form_type = meta_data.get('FilingType')
            if not form_type:continue
            lf = form_type.lower()
            if lf not in self.f_type_map:
                #print form_type
                continue
            form_type = self.f_type_map[lf]
            cik = cik.split(':')
            if len(cik) < 2:
                cik = str(cik[0]).strip()
            else:
                cik = str(cik[1]).strip()
            if (cik not in cik_set) or (not form_type):continue
            #if not form_type:continue
            #if cik not in ('LKFN', 'BMRC', 'ICBK', 'FBSS'):continue
            if cik not in ('PEBO', ):continue
            print doc_id
            from_date = meta_data.get('Document From')
            to_date = meta_data.get('Document To')
            print [from_date, to_date]
            if (not from_date) or (not to_date):continue
            fd_inf = from_date.split('/')
            td_inf = to_date.split('/') 
            if (len(fd_inf) < 3) or (len(td_inf) < 3):continue
            fdd, fmm, fyy = fd_inf
            tdd, tmm, tyy = td_inf 
            from_d_str = '%s-%s-%s'%(fdd, fmm, fyy)
            to_d_str   = '%s-%s-%s'%(tdd, tmm, tyy) 
            
            
            try:
                drange_lst = self.date_range_lst(str(from_d_str), str(to_d_str))
            except:
                try:
                    drange_lst = self.except_date_range_lst(str(from_d_str), str(to_d_str))
                except:
                    continue
            date_info_dct.setdefault(cik, {}).setdefault(form_type, {})[(from_d_str, to_d_str)] = drange_lst
            cik_res_dct.setdefault(cik, {}).setdefault(form_type, {}).setdefault((from_d_str, to_d_str), {})[doc_id] = (meta_data, batch)
        
        '''
        for ks, vs in cik_res_dct.iteritems():
            for ft, pt in vs.iteritems():
                for kst, dd in pt.iteritems():
                    #print kst, dd, '\n'
                    pass
        '''
        update_rows = []
        txt_lst = []
        meta_data_lst = []
        for cik_tkr, fil_dct in cik_res_dct.iteritems():
            for f_type, date_dct in fil_dct.iteritems():
                
                complete_urls, securl, ad_i = self.extract_info_fnc(cik_tkr, f_type)
                #print 'KKKKKKKKK', complete_urls, securl, ad_i
                dw_u_dct = {} 
                date_range_dct = date_info_dct[cik_tkr][f_type]
                url_dct = {}
                for url_tup in complete_urls:
                    u_date, sec_url = self.extract_period_of_report_data(url_tup, f_type)  
                    if not u_date:continue
                    #print url_tup
                    ac_n = url_tup[1]
                    if isinstance(ac_n, unicode):
                        ac_n = ac_n.encode('utf-8')
                    ac_n = ac_n.replace('\xc2\xa0', ' ').replace('\xa0', ' ')
                    ac_n = ' '.join(ac_n.split())
                    ac_n = ac_n.split('Acc-no:')
                    ac_n = ac_n[1].strip().split()[0]
                    tmp_arr = []
                    for date_tup, ran_lst in date_range_dct.iteritems():
                        print '\t', [date_tup, ran_lst]
                        if u_date in ran_lst:
                            #dw_u_dct[date_tup] = ac_n
                            tmp_arr.append(date_tup)
                    if len(tmp_arr) > 1:
                        print 'More THAN 1: ', [cik_tkr, f_type, url_tup, tmp_arr]
                    print tmp_arr 
                    if tmp_arr:
                        dw_u_dct[tmp_arr[0]] = ac_n
                        url_dct[tmp_arr[0]] = sec_url

                print dw_u_dct
                for dt_tup, doc_set in date_dct.iteritems():
                    des_date  =  dw_u_dct.get(dt_tup)
                    print dt_tup, doc_set, [des_date]
                    print 'DDDDDDDDDDDDDDDDDDDDD', [des_date]
                    if not des_date:continue
                    s_url  = url_dct.get(dt_tup)
                    for did, mt_dct_tup in doc_set.iteritems():
                        mt_dct = mt_dct_tup[0]
                        bth = mt_dct_tup[1]
                        d_n = mt_dct.get('DocumentName', "")
                        sec_key = mt_dct.get('SEC Accession No', "")
                        sec_key = sec_key.replace('u00a0', '')   
                        #print 'From meta', sec_key , [sec_key]
                        if sec_key:
                            unm_key = 'UNMATCH'
                            if sec_key == des_date:
                                unm_key = 'MATCH'
                        else:
                            unm_key = 'NEW'
                        rt_str = '\t'.join((bth, cik_tkr, str(did), d_n, des_date, sec_key, unm_key, s_url)) 
                        txt_lst.append(rt_str)
                        mt_dct['TAS_SEC_ACC_No'] = des_date
                        mt_dct['ACC_No_URL'] = securl
                        update_rows.append((json.dumps(mt_dct), did))
                        meta_data_lst.append('\t'.join((str(did), json.dumps(mt_dct))))

        if txt_lst:
            f = open('/var/www/html/tkr_acc_81.txt', 'w')
            for rs in txt_lst:    
                f.write(rs+'\n')
            f.close()

        if meta_data_lst:
            f = open('/var/www/html/meta_acc_81.txt', 'w')
            for rs in meta_data_lst:
                f.write(rs+'\n')
            f.close()
        #sys.exit() 
        print update_rows
        if update_rows:
            update_stmt = """ UPDATE batch_mgmt_upload SET meta_data=%s WHERE doc_id=%s  """   
            m_cur.executemany(update_stmt, update_rows)
            m_conn.commit()
        m_conn.close() 

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur
    
    def update_extracted_url_doc_master(self, company_id):
        pass

if __name__ == '__main__':
    obj = Extract() 
    #print obj.read_batch_cik_doc_info()
    print obj.ft_read_batch_cik_doc_info()
    
