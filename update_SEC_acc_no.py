import os, sys, MySQLdb, sqlite3, json, sets, copy, lmdb, binascii, traceback
import datetime
import update_from_to_data
udate_obj   = update_from_to_data.Company_Info()
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import compute_period_and_date
c_ph_date_obj   = compute_period_and_date.PH()
def print_exception():
        formatted_lines = traceback.format_exc().splitlines()
        for line in formatted_lines:
            print '<br>',line
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
        
    

    def ft_read_batch_cik_doc_info(self, cids):
        #cik_set = self.read_cik_text_file()
        #print cik_set
        #sys.exit()

        db_data_lst     = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test']
        m_conn, m_cur   = self.mysql_connection(db_data_lst)
        sql             = "select row_id, DB_Id from company_mgmt where row_id in (%s)"%(', '.join(cids))
        #print sql
        m_cur.execute(sql)
        res             = m_cur.fetchall()
        tmpar   = []
        for r in res:
            row_id, DB_Id   = r
            sql = "select document_to, assension_number, filing_type from document_master where company_id=%s"%(int(row_id))
            m_cur.execute(sql)
            docres  = m_cur.fetchall()
            date_ar = []
            #print r
            for tr in docres:
                document_release_date, assension_number, filing_type = tr
                #print '\tTR ',tr
                if not document_release_date:continue
                filing_type = ''.join(filing_type.lower().split('-'))
                date_ar.append((document_release_date, assension_number, filing_type))
            tmpar.append((row_id, DB_Id, date_ar))
        m_cur.close()
        for (row_id, DB_Id, date_ar) in tmpar:
            db_data_lst = ['172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(DB_Id)]
            m_conn, m_cur   = self.mysql_connection(db_data_lst)
            sql             = "select document_id, source_type from ir_document_master where source_type is not NULL"
            m_cur.execute(sql)
            tres            = m_cur.fetchall()
            m_conn.close()
            tmpd            = {}
            for tr in tres:
                document_id, source_type    = tr
                tmpd.setdefault(source_type.split('^')[0], {})[str(document_id)]  = tr
            for dbname, docs in tmpd.items():
                udate_obj.update_aecn_inc_from_to_data(dbname, docs)
                db_data_lst = ['172.16.20.52', 'root', 'tas123', dbname]
                m_conn, m_cur = self.mysql_connection(db_data_lst)
                #docs    = ['4504', '15561']
                print dbname, docs
                read_qry = """ SELECT doc_id, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s) and status ='10N'; """  %(', '.join(docs))
                m_cur.execute(read_qry)
                mt_data  = m_cur.fetchall()
                update_rows = []
                #for r in date_ar:
                #    print r
                done_d  = {}
                tmpdoc_ar   = []
                mm_d    = {
                            'Q1':1,
                            'Q2':2,
                            'Q3':3,
                            'Q4':4,
                            'H1':5,
                            'H2':6,
                            'M9':7,
                            'FY':8,
                            }
                for docr in mt_data:
                    doc_id, meta_data   = docr
                    try:
                        meta_data = json.loads(meta_data)
                    except:continue
                    #if meta_data.get('TAS_SEC_ACC_No'):continue
                    from_date = meta_data.get('Document From')
                    to_date = meta_data.get('Document To')
                    if (not from_date) or (not to_date):continue
                    fd_inf = from_date.split('/')
                    td_inf = to_date.split('/') 
                    if (len(fd_inf) < 3) or (len(td_inf) < 3):continue
                    tmpdoc_ar.append((meta_data.get('periodtype'), doc_id, meta_data))
                tmpdoc_ar.sort(key=lambda x:mm_d.get(x[0], 99999))
                for tmptup in tmpdoc_ar:
                    periodtype, doc_id, meta_data   = tmptup
                    #if meta_data.get('TAS_SEC_ACC_No'):continue
                    from_date = meta_data.get('Document From')
                    to_date = meta_data.get('Document To')
                    if (not from_date) or (not to_date):continue
                    fd_inf = from_date.split('/')
                    td_inf = to_date.split('/') 
                    if (len(fd_inf) < 3) or (len(td_inf) < 3):continue
                    fdd, fmm, fyy = fd_inf
                    tdd, tmm, tyy = td_inf 
                    from_d_str = datetime.datetime.strptime('%s-%s-%s'%(fdd, fmm, fyy), '%m-%d-%Y').date()
                    to_d_str   = datetime.datetime.strptime('%s-%s-%s'%(tdd, tmm, tyy), '%m-%d-%Y' ).date()
                    print [doc_id, from_date, to_date]
                    FilingType = ''.join(meta_data.get('FilingType', '').lower().split('-'))
                    tmpsec  =0
                    for tmpr in date_ar:
                        if tmpr[2] != FilingType:continue
                        print '\t', tmpr
                        if tmpr[1] in done_d:
                            print  '\t\tCONTINUE'
                            continue
                        if tmpr[0] >= from_d_str and tmpr[0] <= to_d_str:
                            print 'DONE', tmpr
                            meta_data['TAS_SEC_ACC_No'] = tmpr[1]
                            done_d[tmpr[1]] = 1
                            update_rows.append((json.dumps(meta_data), doc_id))
                            tmpsec  =1
                            break
                    print 'tmpsec ', [tmpsec]
                    if tmpsec == 0:
                        meta_data['TAS_SEC_ACC_No'] = ''
                        update_rows.append((json.dumps(meta_data), doc_id))
                
                #print update_rows
                if update_rows:
                    update_stmt = """ UPDATE batch_mgmt_upload SET meta_data=%s WHERE doc_id=%s  """   
                    m_cur.executemany(update_stmt, update_rows)
                    m_conn.commit()
                m_conn.close() 
        return [{"message":"done"}]

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur
    
    def update_extracted_url_doc_master(self, company_id):
        pass

    def get_company_name(self, DB_id):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db' 
        conn  = sqlite3.connect(db_path)
        cur  =  conn.cursor()
        project_id, deal_id = DB_id.split('_')
        read_qry = """ select company_name from company_info WHERE project_id='%s' AND toc_company_id='%s'; """%(project_id, deal_id)
        cur.execute(read_qry)
        table_data = cur.fetchone()
        conn.close()
        company_name = table_data[0]
        return company_name

    def cp_read_norm_data_mgmt(self, company_id, p_tids):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(company_id)] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT norm_resid, source_table_info FROM norm_data_mgmt"""
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        res_data    = {}
        for row in t_data:
            if row[1]:
                if p_tids and str(row[0]) not in p_tids:continue
                res_data[str(row[0])]    = eval(row[1])
        return res_data

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

    def split_by_camel(self, txt):
        if isinstance(txt, unicode) :
            txt = txt.encode('utf-8')  
        if ' ' in txt:
            return txt
        txt_ar  = []
        for c in txt:
            if c.upper() == c:
                txt_ar.append(' ')
            txt_ar.append(c)
        txt = ' '.join(''.join(txt_ar).split())
        return txt



    def read_kpi_data(self, ijson, tinfo={}):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        disp_name       = ''
        empty_line_display  = 'Y'
        if not ijson.get("template_id"):
            db_file     = '/mnt/eMB_db/page_extraction_global.db'
            conn, cur   = self.connect_to_sqlite(db_file)
            sql = "select industry_type, industry_id from industry_type_storage"
            cur.execute(sql)
            res = cur.fetchall()
            conn.close()
            exist_indus = {}
            for rr in res:
                industry_type, industry_id  = rr
                industry_type   = industry_type.lower()
                exist_indus[industry_type]  = industry_id
            tindustry_type  = ijson['table_type'].lower()
            industry_id = exist_indus[tindustry_type]
            db_file     = "/mnt/eMB_db/industry_kpi_taxonomy.db"
            conn, cur   = self.connect_to_sqlite(db_file)
        else:
            industry_id = ijson['template_id']
            db_file     = "/mnt/eMB_db/template_info.db"
            conn, cur   = self.connect_to_sqlite(db_file)
            sql = "select display_name, empty_line_display from meta_info where row_id=%s"%(industry_id)
            cur.execute(sql)
            tmpres  = cur.fetchone()
            disp_name   = tmpres[0]
            empty_line_display   = tmpres[1]
        empty_line_display  = 'Y'
        #print 'disp_name ', disp_name
        try:    
            sql = "alter table industry_kpi_template add column yoy TEXT"
            cur.execute(sql)
        except:pass
        try:    
            sql = "alter table industry_kpi_template add column editable TEXT"
            cur.execute(sql)
        except:pass
        try:
                sql = "alter table industry_kpi_template add column formula_str TEXT"
                cur.execute(sql)
        except:pass
        try:
                sql = "alter table industry_kpi_template add column taxo_type TEXT"
                cur.execute(sql)
        except:pass
        sql         = "select taxo_id, prev_id, parent_id, taxonomy, taxo_label, scale, value_type, client_taxo, yoy, editable, target_currency, mnemonic_id from industry_kpi_template where industry_id=%s and taxo_type !='INTER'"%(industry_id)
        cur.execute(sql)
        res         = cur.fetchall()
        data_d      = {}
        grp_d       = {}
        all_table_types = {}
            
        for rr in res:
            taxo_id, prev_id, parent_id, taxonomy, taxo_label, scale, value_type, client_taxo,yoy, editable, target_currency, mnemonic_id   = rr
            if client_taxo:
                taxo_label  = self.split_by_camel(client_taxo)
            #if scale in ['1.0', '1']:
            #    scale   = ''
            #if str(deal_id) == '51':
            #    scale   = ''
            grp_d.setdefault(parent_id, {})[prev_id]    = taxo_id
            data_d[taxo_id]  = (taxo_id, taxonomy, taxo_label, scale, value_type, client_taxo, yoy, editable, target_currency, mnemonic_id)
        final_ar    = []
        taxo_exists = {}
        found_open  = {}
        pc_d        = {}
        get_open_d  = {'done':"N"}
        def form_tree_data(dd, level, pid, p_ar):
            prev_id = -1
            iDs = []
            pid = -1
            done_d  = {}
            if (pid not in dd) and dd:
                ks  = dd.keys()
                ks.sort()
                pid = ks[0]

            while 1:
                if pid not in dd:break
                ID  = dd[pid]
                if (ID, pid) in done_d:break #AVOID LOOPING
                done_d[(ID, pid)]  = 1
                pid = ID
                iDs.append(ID)
            tmp_ar  = []
            prev_id = -1
            for iD in iDs:
                if p_ar:
                    pc_d.setdefault(data_d[p_ar[-1]][1], {})[data_d[iD][1]]        = 1
                final_ar.append(data_d[iD]+(level, pid, prev_id))
                if tinfo and (tinfo.get(data_d[iD][1], {}).get('desc', {}).get('f') == 'Y' or tinfo.get(data_d[iD][1], {}).get('desc', {}).get('fm') == 'Y'):
                    for tmpid in p_ar+[iD]:
                        taxo_exists[data_d[tmpid][1]] = 1
                    if get_open_d['done'] == 'N':
                        for tmpid in p_ar+[iD]:
                            found_open[data_d[tmpid][1]]    = 1
                            
                        
                c_ids   = grp_d.get(iD, {})
                if c_ids:
                    form_tree_data(c_ids, level+1, iD, p_ar+[iD])
                prev_id = iD
                if level == 1 and found_open:
                    get_open_d['done']  = "Y"
        root    = grp_d[-1]
        form_tree_data(root, 1, -1, [])
        return final_ar, taxo_exists, found_open, pc_d, disp_name, empty_line_display


    def get_doc_table_ids(self, DB_Id, company_name, project_name, rev_doc_map_d, fye):
        cdn = ''.join([es for es in project_name.split()])
        txt_id          = '414'
        txt_dir_path    = '/var/www/html/DB_Model/{0}/{1}/Norm_Scale/%s-P.txt'.format(company_name, cdn, txt_id)
        cdn = ''.join([es for es in project_name.split()])
        ijson   = {}
        project_id, deal_id = DB_Id.split('_')
        ijson['company_name']   = company_name
        model_number    = project_id
        deal_id         = deal_id
        project_id      = project_id
        ijson['template_id'] = 10
        ijson['model_number'] = model_number
        ijson['deal_id'] = deal_id
        ijson['project_id'] = project_id
        company_id      = "%s_%s"%(project_id, deal_id)


        db_file = "/mnt/eMB_db/company_info/compnay_info.db"
        conn, cur   = conn_obj.sqlite_connection(db_file)
        sql = "select table_type, description from model_id_map"
        cur.execute(sql)
        res = cur.fetchall()
        conn.close()
        ttype_map_d = {}
        for r in res:
            tt, description = r
            ttype_map_d[tt] = description
        db_file         = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        read_qry = 'SELECT table_type, group_id, display_name FROM tt_group_display_name_info;'
        cur.execute(read_qry)
        table_data = cur.fetchall()
        for row in table_data:
            ttable_type, tgroup_id, display_name = row[:]
            if display_name:
                ttype_map_d[ttable_type]    = display_name
        
        final_ar, taxo_exists, found_open, pc_d, disp_name, empty_line_display  = self.read_kpi_data(ijson)

        scale_dct = {'mn':'Million', 'th':'Thousands', 'bn':'Billion'}
        vt_dct    = {'MNUM':'Monetary Number', 'BNUM':'Cardinal Number'}
        txt_dir_path = '/var/www/html/DB_Model/{0}/{1}/Norm_Scale/'.format(company_name, cdn)
        list_txt_files = os.listdir(txt_dir_path) 


        done_docs   = {}
        all_docids  = {}
        all_table_ids  = {}
        not_matching_docids = {}
        more_than_one_doc   = []
        d_point_d           = {}
        for txt_file in list_txt_files:
            txt_file = txt_file.strip()
            if '-P' not in txt_file:continue
            if '414-P.txt' != txt_file:continue
            gr_id =  txt_file.split('-')    
            gen_id = gr_id[0]
            if len(gr_id) == 3:
                gr_id = gr_id[1:-1]
                gr_id = ''.join(gr_id)
            else:gr_id = ''
            txt_path = os.path.join(txt_dir_path, txt_file)
            f = open(txt_path)
            txt_data = f.read()
            f.close()
            txt_data = eval(txt_data)
            for table_type, tt_data in  txt_data.iteritems():
                #ijson['table_type'] = table_type
                #ijson['grpid'] = gr_id
                data     = tt_data['data']
                key_map  = tt_data['key_map']
                rc_keys = data.keys()  
                rc_keys.sort()
                mneumonic_txt_d = {}
                map_d   = {}
                ph_cols = {}
                for rc_tup in rc_keys:
                    dt_dct = data[rc_tup]
                    
                    if rc_tup[1]  == 0:
                        mneumonic_txt_d[dt_dct[21]] = dt_dct[1]
                        map_d[dt_dct[21]]   = rc_tup[0]
                        #map_d[('REV', rc_tup[0])]   = dt_dct[21]
                    else:
                        ph_cols[rc_tup[1]]  = dt_dct[10]
                        #map_d.setdefault(map_d[('REV', rc_tup[0])], {}).append(rc_tup)
            
                
                phs = ph_cols.keys()
                phs.sort()
                for row in final_ar:
                    mneumonic_txt   =  row[2]
                    mneumonic_id   =  row[9]
                    rowid   = map_d.get(row[1], -1)
                    for colid in phs:
                        rc_tup  = (rowid, colid)
                        g_dt_dct = data.get(rc_tup, {})
                        formula     = g_dt_dct.get(15, [])
                        op_ttype    = {}
                        taxo_d      = []
                        for f_r in formula[:1]: 
                            for r in f_r:
                                if r['op'] == '=' or r['type'] != 't':continue
                                op_ttype[r['ty']] = 1
                                taxo_d.append(r['ty'])
                        if len(taxo_d) > 1:
                            re_stated_all   = []
                        else:
                            re_stated_all = g_dt_dct.get(31, [])
                        year_wise_d = {}
                        idx_d   = {}
                        for r in re_stated_all:
                            #print '\t', r
                            if(r.get(2)):
                                if r[2] not in idx_d:
                                    idx_d[r[2]]   = len(idx_d.keys())+1
                                year_wise_d.setdefault(r.get(2), []).append(r)
                        
                        if not year_wise_d:
                            if re_stated_all:
                                continue
                                print 'Error ', (rc_tup, re_stated_all)
                                sys.exit()
                            year_wise_d[1]  = [g_dt_dct]
                            idx_d[1]    = 1
                        values  = year_wise_d.keys()
                        values.sort(key=lambda x:idx_d[x])
                        for vidx, v1 in enumerate(values):
                            dt_dct  = year_wise_d[v1][0]
                            formula     = g_dt_dct.get(15, [])
                            op_ttype    = {}
                            taxo_d      = []
                            docids      = {}
                            scale_d     = {}
                            ttype_d     = {}
                            table_ids   = {}
                            for f_r in formula[:1]: 
                                #print
                                for r in f_r:
                                    #print r
                                    if r['op'] == '=' or r['type'] != 't':continue
                                    if r['label'] == 'Not Exists':continue
                                    if r.get('table_id', ''):
                                        table_ids[r['table_id']]   = 1
                                        pass
                                    if r['doc_id']:
                                        all_docids[str(r['doc_id'])]  = 1
                                        if str(r['doc_id']) not in rev_doc_map_d:
                                            not_matching_docids[str(r['doc_id'])]  = 1
                                        docids[rev_doc_map_d[str(r['doc_id'])]]      = 1
                                    if r.get('v'):
                                        scale_d[str(r['phcsv']['s'])]     = 1
                                    ttype_d[ttype_map_d[r['tt']]]    = 1
                                    op_ttype[ttype_map_d[r['tt']]] = 1
                                    taxo_d.append(r['ty'])
                            if len(taxo_d) > 1:
                                gv_txt      = dt_dct.get(2, '')
                            else:
                                gv_txt      = dt_dct.get(38, '')
                            tmpgv_txt  = gv_txt #numbercleanup_obj.get_value_cleanup(gv_txt)
                            if (gv_txt == '-' and  not tmpgv_txt) or (gv_txt == '--' and  not tmpgv_txt) or (gv_txt == 'n/a'  and  not tmpgv_txt):
                                tmpgv_txt = '-'
                            gv_txt  = tmpgv_txt 
                            
                                
                            #print
                            #print (row, colid)
                            #{1: '8,792.03', 2: '8792.03', 3: '13681', 4: '170', 5: [[383, 215, 43, 7]], 6: '2013FY', 7: u'Mn', 8: 'MNUM', 9: 'USD', 10: 'FY2013', 39: {'p': '2013', 's': 'TH', 'vt': 'MNUM', 'c': 'USD', 'pt': 'FY'}, 34: '', 14: {'d': '13681', 'bbox': [[46, 215, 27, 7]], 'v': 'Amount', 'x': 'x28_170@0_6', 'txt': u'Tier1capital - Amount', 't': '219'}, 40: '', 24: '219', 25: 'x29_170@0_11', 26: 'PeriodicFinancialStatement-FY2013', 38: '$ 8,792,035'}
                            #if len()
                            clean_value     = dt_dct.get(2, '')
                            cln_val         = copy.deepcopy(clean_value)
                            currency        = dt_dct.get(9, '')
                            if len(taxo_d) > 1:
                                scale           = dt_dct.get(7, '')
                            else:
                                scale           = dt_dct.get(39, {}).get('s', '')
                                if not scale:
                                    scale           = dt_dct.get(7, '')
                            scale1          = dt_dct.get(7, '')
                            tmp_ttype       = table_type
                            calc_value = dt_dct.get(41, '')
                            if op_ttype:#len(op_ttype.keys()) == 1:
                                tmp_ttype   = op_ttype.keys()[0]

                            value_type = dt_dct.get(8, '')
                            restated_lst = dt_dct.get(40, []) 
                            rep_rst_flg  =  'Original'
                            if restated_lst == 'Y':
                                rep_rst_flg = 'Restated' 
                            if len(values) > 1 and idx_d[v1] > 1:
                                rep_rst_flg = 'Restated' 
                            ph_info = ph_cols[colid]
                            pdate, period_type, period = '', '', ''
                            if ph_info:
                                #print [fye, ph_info,  dt_dct.get(3, '')]
                                pdate = self.read_period_ending(fye, ph_info)
                                #print pdate
                                period_type = ph_info[:-4]
                                period  = ph_info[-4:]
                            #print [ph_info, pdate]
                            doc_id = dt_dct.get(3, '')
                            doc_data = dt_dct.get(27, [])
                            if doc_id:
                                all_docids[str(doc_id)] = 1
                                if str(doc_id) not in rev_doc_map_d:
                                    not_matching_docids[str(doc_id)] = 1
                            if len(taxo_d) > 1:# or rc_tup not in data:
                                calc_value  = 'Y'
                                if len(ttype_d.keys()) > 1:
                                    tmp_ttype   = ''
                                if len(scale_d.keys()) > 1:
                                    scale       = ''
                                    scale1       = ''
                                gv_txt      = ''
                                if len(docids.keys()) > 1:
                                    doc_id      = ''
                            if rc_tup not in data:
                                tmp_ttype   = ''
                                scale       = ''
                                scale1       = ''
                                gv_txt      = ''
                                doc_id      = ''
                            if not clean_value:
                                rep_rst_flg = ''
                            if len(taxo_d) > 1  and len(scale_d.keys())  > 1:# or rc_tup not in data:
                                    scale       = ''
                                    scale1       = ''
                            table_id  = dt_dct.get(24, '')
                            if table_id:
                                table_ids[str(table_id)]    = 1
                            if table_ids:
                                d_point_d.setdefault((mneumonic_txt, mneumonic_id, ph_info, vidx), {}).update(table_ids)
                            
                            info_ref = '' 
                            if table_id and doc_id:
                                all_table_ids[str(table_id)]    = 1
                            #mneumonic_txt = mneumonic_txt.decode('utf-8')
                            #print [mneumonic_txt], mneumonic_txt
                            try:
                                mneumonic_txt = mneumonic_txt.encode('utf-8')
                            except:
                                mneumonic_txt = str(mneumonic_txt)
                            #if value_type != 'Percentage':value_type = 'Absolute'
                            if info_ref:
                                if info_ref != 'From Sentence':
                                    info_ref = 'Table'
                                if info_ref == 'From Sentence':
                                    info_ref = 'Text'
                            #if len(taxo_d) > 1:# or rc_tup not in data:
                            #    scale   = ''
                            vaur = scale_dct.get(scale.lower(), scale)
                            vt_c = vt_dct.get(value_type, value_type)
                            #print ['SSSSSSSSSS', vt_c, value_type, mneumonic_txt], '\n'
                            tmpcalc_value   = 'false'
                            if calc_value == 'Y':
                                tmpcalc_value   = 'true'
                                vaur    = ''
                                gv_txt  = ''
                            if len(docids.keys()) > 1:
                                more_than_one_doc.append((mneumonic_txt, mneumonic_id, pdate, str(period_type), docids))
        return (more_than_one_doc, all_docids, all_table_ids, not_matching_docids, d_point_d)

    def read_period_ending(self, year_ending, ph):
        y_end_num   = year_ending
        return c_ph_date_obj.get_date_from_ph(ph, y_end_num)

    def validate_docs(self, cids, project_id=None):
        cids            = map(lambda x:str(x), cids)
        db_data_lst     = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test']
        m_conn, m_cur   = self.mysql_connection(db_data_lst)
        sql             = "select row_id, DB_Id, financial_year_end from company_mgmt where row_id in (%s)"%(', '.join(cids))
        print sql
        m_cur.execute(sql)
        res             = m_cur.fetchall()
        project_name    = ''
        if project_id:
            sql = "select client_name from client_mgmt where row_id   = %s"%(project_id)
            m_cur.execute(sql)
            tmpres  = m_cur.fetchone()
            project_name    = tmpres[0]
            
        tmpar   = []
        e_ar    = []
        rev_doc_map_d   = {}
        for r in res:
            row_id, DB_Id, financial_year_end   = r
            c_id    = row_id
            db_data_lst     = ['172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(DB_Id)]
            m_conn, m_cur   = self.mysql_connection(db_data_lst)
            p_tids          = {}
            p_dids          = {}
            company_name    = self.get_company_name(DB_Id)
            more_than_one_doc   = {}
            not_matching_docids = {}
            sql             = "select document_id, source_type from ir_document_master where source_type is not NULL"
            m_cur.execute(sql)
            tres            = m_cur.fetchall()
            rev_doc_map_d   = {}
            for tr in tres:
                document_id, source_type    = tr
                rev_doc_map_d[str(document_id)] = str(document_id)
            if project_name:
                (more_than_one_doc, p_dids, p_tids, not_matching_docids, xxxxx)  = self.get_doc_table_ids(DB_Id, company_name, project_name, rev_doc_map_d, int(datetime.datetime.strptime(financial_year_end, '%B').strftime('%m')))
            if 0:#more_than_one_doc:
                e_ar.append({'id':4, 'type':'More than one docids in a cell', 'data':more_than_one_doc})
            if not_matching_docids:
                e_ar.append({'id':3, 'type':'Data Builder docid not matching with company management', 'data': not_matching_docids})
                    
            t_d             = self.cp_read_norm_data_mgmt(DB_Id, p_tids)
            if p_dids:
                sql             = "select document_id, source_type from ir_document_master where document_id in (%s) and source_type is not NULL"%(', '.join(p_dids.keys()))
            else:
                sql             = "select document_id, source_type from ir_document_master where source_type is not NULL"
            m_cur.execute(sql)
            tres            = m_cur.fetchall()
            m_conn.close()
            tmpd            = {}
            for tr in tres:
                document_id, source_type    = tr
                tmpd.setdefault(source_type.split('^')[0], {})[str(document_id)]  = tr
            m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, DB_Id.split('_')[0], [])
            done_tabel_grp  = {}
            for dbname, docs in tmpd.items():
                #udate_obj.update_aecn_inc_from_to_data(dbname, docs)
                db_data_lst = ['172.16.20.52', 'root', 'tas123', dbname]
                m_conn, m_cur = self.mysql_connection(db_data_lst)
                #docs    = ['4504', '15561']
                read_qry = """ SELECT docid, pageno, groupid FROM db_data_mgmt_grid_slt WHERE docid in (%s)"""%(', '.join(docs))
                m_cur.execute(read_qry)
                mt_data  = m_cur.fetchall()
                for tr in mt_data:
                    docid, pageno, groupid  = tr
                    done_tabel_grp[(str(docid), str(pageno), str(groupid))] = 1
                read_qry = """ SELECT doc_id, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s) and status ='10N'; """  %(', '.join(docs))
                m_cur.execute(read_qry)
                mt_data  = m_cur.fetchall()
                update_rows = []
                #for r in date_ar:
                #    print r
                done_d  = {}
                tmpdoc_ar   = []
                mm_d    = {
                            'Q1':1,
                            'Q2':2,
                            'Q3':3,
                            'Q4':4,
                            'H1':5,
                            'H2':6,
                            'M9':7,
                            'FY':8,
                            }
                dd  = {}
                for docr in mt_data:
                    doc_id, meta_data   = docr
                    try:
                        meta_data = json.loads(meta_data)
                    except:continue
                    #if meta_data.get('TAS_SEC_ACC_No'):continue
                    from_date = meta_data.get('Document From')
                    to_date = meta_data.get('Document To')
                    if (not from_date) or (not to_date):continue
                    fd_inf = from_date.split('/')
                    td_inf = to_date.split('/') 
                    if (len(fd_inf) < 3) or (len(td_inf) < 3):continue
                    FilingType = ''.join(meta_data.get('FilingType', '').lower().split('-'))
                    tup = ('/'.join(fd_inf), '/'.join(td_inf), FilingType, meta_data['periodtype'], meta_data['Year'])
                    #print tup, [doc_id]
                    dd.setdefault(tup, {})[doc_id]  = 1
                ks  = dd.keys()
                morthan_one = filter(lambda x:len(dd[x].keys()) > 1, ks)
                if morthan_one:
                    e_ar.append({'id':2, 'type':'More than one docid has same PH from & To Date', 'data':map(lambda x:dd[x].keys(), morthan_one)})
            t_ar    = []
            for tid, sinfo in t_d.items():
                if sinfo not in done_tabel_grp:
                    t_ar.append(sinfo)
            if t_ar and str(c_id) not in ['1401']:
                    e_ar.append({'id':1, 'type':'INC Table is missing', 'data':map(lambda x:'_'.join(x), t_ar)})
        if e_ar:
            res = [{"message":"Error ", "data":e_ar, 'ef_flg':1}]
        else:
            res = [{"message":"done", "data":e_ar}]
        return res

    def get_main_table_info(self, company_name, model_number, f_tables=[]):
        return self.get_main_table_info_new(company_name, model_number, f_tables)

    def get_all_docids(self, company_name, model_number):
        db_file         = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql             = "select doc_id from company_meta_info"
        cur.execute(sql)
        res             = cur.fetchall()
        doc_d   = {}
        for rr in res:
            doc_d[str(rr[0])]   = 1
        return doc_d


    def sheet_id_map(self):
        from collections import defaultdict as dd
        db_file     = '/mnt/eMB_db/node_mapping.db'
        conn, cur   = conn_obj.sqlite_connection(db_file)
        sql   = "select main_header, sub_header, sheet_id, node_name, description from node_mapping where review_flg = 0"
        cur.execute(sql)
        tres    = cur.fetchall()
        #print rr, len(tres)
        ddict   = dd(set)
        for tr in tres:
            #print tr
            main_header, sub_header, sheet_id, node_name, description = map(str, tr)
            ddict[sheet_id] = [main_header, sub_header, node_name, description]
        return ddict

    def get_main_table_info_new(self, company_name, model_number, f_tables=[]):
        tdict = {}
        #elif str(model_number) == '60':
        #    tdict = self.get_all_tablessm(company_name, model_number)
        doc_d           = self.get_all_docids(company_name, model_number)
        sheet_id_map = self.sheet_id_map()
        #print 'SSSS', sheet_id_map
        m_tables        = {}
        doc_m_d         = {}
        rev_m_tables    = {}
        db_file         = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn, cur       = conn_obj.sqlite_connection(db_file)
        sql             = "select sheet_id, doc_id, doc_name, table_id from table_group_mapping"
        try:
            cur.execute(sql)
            res             = cur.fetchall()
        except:
            res = []
        tmpres          = []
        table_type_m    = {}
        '''
        try:
            t1_dct, i_lst = self.custom_info(conn, cur)
        except:t1_dct, i_lst = {}, []
        '''
        
        #print sheet_id_map
        #print res, doc_d
        for rr in res:
            sheet_id, doc_id, doc_name, table_id    = rr
            #print (sheet_id, doc_id, doc_name, table_id)
            if str(doc_id) not in doc_d:continue
            main_header, sub_header, node_name, description = sheet_id_map.get(str(sheet_id), ['','','',''])
            #print main_header, sub_header, node_name, description, 
            #print rr, (main_header, sub_header, node_name, description)
            if f_tables and node_name not in f_tables:continue
            #print rr, main_header
            #if main_header not in ['Main Table']:continue
            desc_sp = description.split('-')
            desc_sp = map(lambda x:x.strip(), desc_sp[:])
            desc_sp = list(sets.Set(desc_sp))
            if len(desc_sp) == 1:
               description = desc_sp[0]
            table_type_m[node_name]    = description
            #print (node_name, doc_id, table_id, main_header), '\n\n'
            tmpres.append((node_name, doc_id, table_id, main_header))
        cur.close()
        conn.close()
        m_tables        = {}
        doc_m_d         = {}
        rev_m_tables    = {}
        main_headers    = {}
        for rr in tmpres:
            table_type, doc_id, table_id_str, main_header    = rr
            doc_id      = str(doc_id)
            table_type  = str(table_type)
            for table_id in table_id_str.split('^!!^'):
                if not table_id:continue
                if tdict and table_id not in tdict:continue
                main_headers[main_header]    = 1
                table_id            = str(table_id)
                m_tables[table_id]  = table_type
                rev_m_tables.setdefault(table_type, {})[table_id]   = 1
                doc_m_d[table_id]   = doc_id
        table_type_m['main_header'] = main_headers.keys()
        return m_tables, rev_m_tables, doc_m_d, table_type_m

    def get_table_info(self, cids, project_id=None):
        cids            = map(lambda x:str(x), cids)
        db_data_lst     = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test']
        m_conn, m_cur   = self.mysql_connection(db_data_lst)
        deal_ids        = ["20_177", "20_152", "20_181", "20_16", "20_122", "20_100", "20_165", "20_162", "20_182", "20_133", "20_137", "20_184", "20_153", "20_163", "1410_1", "20_120", "20_167", "20_130", "20_114", "20_99", "20_151", "20_185", "20_161", "20_169", "20_174", "20_175", "20_180", "20_186", "20_176", "20_101", "20_160", "20_166", "20_146", "20_171", "20_159", "20_179", "20_172", "20_178", "20_187", "1_116", "1406_1", "20_199", "20_170", "20_158", "20_149", "20_183", "20_198", "20_118", "20_154", "20_80", "20_136", "20_157", "20_144", "20_200", "20_168", "20_164", "20_134", "20_173", "20_124", "20_150", "20_155", "20_197", "20_145", "20_89", "20_13"]
        sql             = "select company_id from client_details where project_id=1"
        m_cur.execute(sql)
        res             = m_cur.fetchall()
        if cids == ['ALL']:
            cids            = []
            for r in res:
                cids.append(str(r[0]))
        sql             = "select row_id, DB_Id, financial_year_end from company_mgmt where row_id in (%s)"%(', '.join(cids))
        print sql
        m_cur.execute(sql)
        res             = m_cur.fetchall()
        project_name    = ''
        if project_id:
            sql = "select client_name from client_mgmt where row_id   = %s"%(project_id)
            m_cur.execute(sql)
            tmpres  = m_cur.fetchone()
            project_name    = tmpres[0]
            
        tmpar   = []
        e_ar    = []
        rev_doc_map_d   = {}
        table_info_d    = {}
        rev_table_info_d    = {}
        cnt = 1
        error_deals = {}
        gd_point_d           = {}
        for r in res:
            row_id, DB_Id, financial_year_end   = r
            if DB_Id not in deal_ids:continue
            print 'Running ',cnt, '/', 50, r
            cnt += 1
            try:
                c_id    = row_id
                db_data_lst     = ['172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(DB_Id)]
                m_conn, m_cur   = self.mysql_connection(db_data_lst)
                p_tids          = {}
                p_dids          = {}
                company_name    = self.get_company_name(DB_Id)
                more_than_one_doc   = {}
                not_matching_docids = {}
                d_point_d           = {}
                sql             = "select document_id, source_type from ir_document_master where source_type is not NULL"
                m_cur.execute(sql)
                tres            = m_cur.fetchall()
                rev_doc_map_d   = {}
                for tr in tres:
                    document_id, source_type    = tr
                    rev_doc_map_d[str(document_id)] = str(document_id)
                if project_name:
                    (more_than_one_doc, p_dids, p_tids, not_matching_docids, d_point_d)  = self.get_doc_table_ids(DB_Id, company_name, project_name, rev_doc_map_d, int(datetime.datetime.strptime(financial_year_end, '%B').strftime('%m')))
                    t_d             = self.cp_read_norm_data_mgmt(DB_Id, p_tids)
                if p_dids:
                    sql             = "select document_id, source_type from ir_document_master where document_id in (%s) and source_type is not NULL"%(', '.join(p_dids.keys()))
                else:
                    sql             = "select document_id, source_type from ir_document_master where source_type is not NULL"
                m_cur.execute(sql)
                tres            = m_cur.fetchall()
                m_conn.close()
                tmpd            = {}
                for tr in tres:
                    document_id, source_type    = tr
                    tmpd.setdefault(source_type.split('^')[0], {})[str(document_id)]  = tr
                m_tables, rev_m_tables, doc_m_d,table_type_m = self.get_main_table_info(company_name, DB_Id.split('_')[0], [])
                done_tabel_grp  = {}
                pid = ''
                for dbname, docs in tmpd.items():
                    if pid == '':
                        pid = {'AECN_INC':'34'}.get(dbname, str(row_id))
                    #udate_obj.update_aecn_inc_from_to_data(dbname, docs)
                    db_data_lst = ['172.16.20.52', 'root', 'tas123', dbname]
                    m_conn, m_cur = self.mysql_connection(db_data_lst)
                    #docs    = ['4504', '15561']
                    read_qry = """ SELECT docid, pageno, groupid, userid FROM db_data_mgmt_grid_slt WHERE docid in (%s)"""%(', '.join(docs))
                    m_cur.execute(read_qry)
                    mt_data  = m_cur.fetchall()
                    for tr in mt_data:
                        docid, pageno, groupid, userid  = tr
                        done_tabel_grp[(str(docid), str(pageno), str(groupid))] = userid
                
                lmdb_path1  =  "/var/www/html/fill_table/%s/table_info"%(DB_Id)
                env         = lmdb.open(lmdb_path1, readonly=True)
                txn_m       = env.begin()
                for tid in p_tids:
                    sinfo   = t_d[tid]
                    ref     = done_tabel_grp.get(sinfo, ())
                    ttype   = ''
                    if ref == 'From Sentence':
                        ttype   = 'S'
                    else:
                        xml_ids, nrows, pnos     = self.read_table_xmls_ids(tid, txn_m)
                        if len(pnos) > 1:
                            ttype   = 'T'
                        else:
                            ttype       = self.compare_data(tid, sinfo, xml_ids, pid, nrows, pnos)
                    table_info_d[tid]    = ttype
                    rev_table_info_d.setdefault(ttype, {})[tid]    = 1
                for k, v in d_point_d.items():
                    ttype    = {}
                    for tid in v.keys():
                        if tid in table_info_d:
                            ttype[table_info_d[tid]]    = 1
                    gd_point_d.setdefault(k[:2], {'tinfo':{'S':{}, 'T':{}, 'Both':{}}, 'dpoints':0}) #['tinfo'].setdefault(table_info_d[tid], {})[(row_id, tid)] = 1
                    gd_point_d[k[:2]]['dpoints'] += 1
                    if len(ttype.keys()) > 1:
                        ttype   = {'Both':1}
                    gd_point_d[k[:2]]['tinfo'][ttype.keys()[0]][len(gd_point_d[k[:2]]['tinfo'][ttype.keys()[0]].keys())+1] = 1
                    
                    
            except:
                error_deals[(row_id, DB_Id)]    = 1
                print_exception()
        print rev_table_info_d.get('S', {}).keys()
        print 'Total ', len(table_info_d.keys())
        print 'S ', len(rev_table_info_d.get('S', {}).keys())
        print 'T ', len(rev_table_info_d.get('T', {}).keys())
        print 'Error ', len(error_deals.keys()), error_deals
        fout    = open('/var/www/html/muthu/KBRA_stats.txt', 'w')
        ar      = ['Taxonomy', 'KBRA ID', 'Total DP', 'Sentence', 'Tabe', 'Both']
        fout.write('\t'.join(ar)+'\n')
        for k, v in gd_point_d.items():
            ar  = [k[0], k[1], v['dpoints'],len(v['tinfo'].get('S', {}).keys()), len(v['tinfo'].get('T', {}).keys()), len(v['tinfo'].get('Both', {}).keys())]
            ar  = map(lambda x:str(x), ar)
            fout.write('\t'.join(ar)+'\n')
        fout.close()
            
            


    def compare_data(self, table_id, sinfo, xml_ids, pid, nrows, pnos):
        #{(12, 1): {'pos_split_flag': 0, 'cell_dict': {'cord': (141, 203, 28, 10), 'rowspan': 1, 'align_v': u'', 'colspan': 1, 'wrap': 0, 'chunks': [{'docsubtype': 'chunk', 'words_lst': [(144.0, 203.0, 161.0, 212.0)], 'text': '11.5', 'NODEID': '62', 'doctype': 'content', 'groupname': 'chunk', 'grid_bbox': {'h': '9', 'w': '18', 'y1': 212, 'y0': 203, 'x0': 143, 'x1': 161}, 'WordLength': '18', 'wbbox': {'h': 9.0, 'w': 17.0, 'y1': 212.0, 'y0': 203.0, 'x0': 144.0, 'x1': 161.0}, 'SEQNO': '485#486#487#488', 'raw_chunk_ref': '710', 'text_align': 'right', 'fontid': 4, 'WordSpaces': '', 'xmlID': u'x62_180', 'grid_glyph_bbox': {'h': '9', 'w': '17', 'y1': 212, 'y0': 203, 'x0': 144, 'x1': 161}, 'grid_text_align': 'right', 'childids': [], 'line_id': 710, 'bbox': {'h': 9.0, 'w': 18.0, 'y1': 212.0, 'y0': 203.0, 'x0': 143.0, 'x1': 161.0}, 'pno': 180, 'ACTUAL_REN_CORD': '144.903436989_203.92414665_17.0278232406_9.01137800253', 'font_id_marker': '4', 'WordsCord': '', 'PDF_CORD': '144.903436989_194.912768647_212.935524653_203.92414665', 'about': [], 'org_bbox': {'h': '9', 'w': '17', 'y1': 212, 'y0': 203, 'x0': 144, 'x1': 161}, 'words_id_list': [(711, '11.5', (144.0, 203.0, 161.0, 212.0), 6, '485#486#487#488', '4', '0')], 'WRDNOS': '', '_id': ''}], 'align_h': u''}, 
        tmpsinfo    = (sinfo[0], pnos[0], sinfo[2])
        
        cell_dict   = self.read_cell_dict(pid, tmpsinfo)
        f_rc    = {}
        rows    = {}
        cols    = {}
        for k, v in cell_dict.items():
            if isinstance(k, tuple) and isinstance(v, dict):
                for c in v['cell_dict']['chunks']:
                    xml = c['xmlID']
                    if xml in xml_ids:
                        f_rc[k] = 1
                        rows[k[0]]  = 1
                        cols[k[1]]  = 1
        if not f_rc:
            print 'XLM IDS are not matching ', table_id, sinfo, xml_ids
            #xxxxxxxxxxxxxxxxxxxxxxxxxxxxx
            if nrows > 5:
                return 'T'
            else:
                return 'S'
        if len(cols.keys()) == 1 and len(rows) <= 2:#nrows:
                return 'S'
        else:
                return 'T'
        
            

    def read_cell_dict(self, pid, sinfo):
        doc_id, page_no, grid_id    = sinfo
        path  = '/var/www/html/WorkSpaceBuilder_DB/{0}/1/pdata/docs/'.format(pid)
        import common_func as common_func
        import common.baseobj as bobj
        import common.datastore as datastore
        import common.GlobalData as GlobalData
        import common.filePathAdj as fileabspath
        import common.getconfig as getconfig
        idbkey = 'QngxaW1pcGZGeWwxNUFBNmshqVLICs12'
        idbkeysize = len(idbkey)
        common_func.ipath = path
        common_func.opath = path
        common_func.isdb = 1
        common_func.isenc = 1
        # Global data object
        gobj = GlobalData.GlobalData()
        if idbkey is not None: gobj.add('dbkey', idbkey)
        if idbkeysize is not None: gobj.add('dbkeysize', idbkeysize)
        cell_dict = common_func.get_cell_info_dict(doc_id, page_no)
        return cell_dict
        
       

    def read_table_xmls_ids(self, table_id, txn_m):
        ids = txn_m.get('GV_'+str(table_id))
        xml_d   = {}
        rows    = {}
        pnos    = {}
        for c_id in ids.split('#'):
            t       = binascii.a2b_hex(txn_m.get('TEXT_'+c_id))
            if not t.strip():continue
            x       = txn_m.get('XMLID_'+c_id)
            for x1 in x.split(':@:'):
                for x2 in x1.split('#'):
                    xml = x2.split('@')[0]
                    if ('x-' not in xml) and 'x' in xml and  ('_' in xml):
                        pnos[int(xml.split('_')[1])] = 1
                        xml_d[xml]   = 1
            rows[c_id.split('_')[1]]    = 1
        pnos    = pnos.keys()
        pnos.sort()
        return xml_d, len(rows.keys()), pnos
                        
            
        

                        

if __name__ == '__main__':
    obj = Extract() 
    #print obj.read_batch_cik_doc_info()
    cids    = sys.argv[1].split('#')
    print obj.get_table_info(cids, sys.argv[2])
    
