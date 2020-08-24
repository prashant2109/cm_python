#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os, sets, hashlib, binascii, lmdb, copy, json, ast, datetime,sqlite3, MySQLdb, redis
import subprocess
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import config
import json
import urllib
import mysql.connector as mysql
import MySQLdb
from urllib import unquote
from urlparse import urlparse
from url_execution import Request
import msgpack
import httplib
import xlrd
import xlsxwriter
from xlwt import *
import xlwt
import run_html as rn_html
html_conn = rn_html.run_html()

import run_html_229 as run_htmls
create_db = run_htmls.run_html() 
import traceback
def PrintException():
   formatted_lines = traceback.format_exc().splitlines()
   for line in formatted_lines:
     print line



def disableprint():
    return
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    return
    sys.stdout = sys.__stdout__

class PYAPI():
    def __init__(self):
        pypath    = os.path.dirname(os.path.abspath(__file__))
        self.dateformat = {
                            'www.businesswire.com'      : ['%Y-%m-%d %H:%M:%S', '%B %d, %Y %H:%M:%S', '%Y-%m-%d %H:%M:%S'], #[April 24, 2019 12:44:29]
                            'press.aboutamazon.com'     : ['%Y-%m-%d %H:%M:%S', '%b/%d/%Y %H:%M:%S'], #['2019-04-09 12:04:21', 'Apr/29/2019 17:20:4']
                            'www.sec.gov'               : ['%Y-%m-%d %H:%M:%S', '%b/%d/%Y %H:%M:%S'], #['2019-04-09 12:04:21', 'Apr/29/2019 17:20:4']
                            'pressroom.aboutschwab.com' : ['%m/%d/%y %H:%M %p %Z'],
                            'www.gapinc.com'            : ['%m/%d/%y', '%m/%d/%y %H:%M:%S'],
                            }
        self.month_map  = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        pass

    def calll_ftp_data(self, project_id, ws_id):
        url = "http://172.16.20.10/cgi-bin/ftp_scanner/cgi_ftp_scanner.py?input_str={'project_id':%s, 'url_id':%s}" %(39, 1)
        content = urllib.urlopen(url).read()
        datadict = json.loads(content)
        return datadict

    def update_insert_company_mgmt_user_log(self, ijson):
        save_meta    = ijson['data']
        row_id       = ijson['rid']
        user_name = ijson.get('user', "")
        company_display_name = ijson['company']
        company_display_name = company_display_name.encode('utf-8')
        ticker = ijson['ticker']
        country = ijson['country']
        company_name = ''.join(company_display_name.split())
        logo = ijson['logo']
        meta_dct = json.dumps(save_meta)
        table_name = 'company_mgmt'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        user_log_ins_rows = [] 
        if row_id == 'new':
            insert_stmt = """ INSERT INTO company_mgmt(company_name, company_display_name, meta_data, user_name, ticker, country, logo, updated_user) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')   """%(company_name, company_display_name, meta_dct, user_name, ticker, country, logo, user_name)
            m_cur.execute(insert_stmt)
               
        elif row_id != 'new':
            update_stmt = """ UPDATE company_mgmt SET company_name='%s', company_display_name='%s', meta_data='%s', user_name='%s', ticker='%s', country='%s', logo='%s', updated_user='%s' WHERE row_id='%s'  """%(company_name, company_display_name, meta_dct, user_name, ticker, country, logo, user_name, row_id)
            m_cur.execute(update_stmt)
        m_conn.commit()
        if row_id != 'new':
            lid = row_id
        lid = self.last_inserted_row_id(table_name, m_conn, m_cur)
        user_log_ins_rows.append(('company_mgmt', lid, 'company_name', user_name, 'insert', company_name)) 
        user_log_ins_rows.append(('company_mgmt', lid, 'company_display_name', user_name, 'insert', company_display_name)) 
        user_log_ins_rows.append(('company_mgmt', lid, 'meta_data', user_name, 'insert', meta_dct))
        user_log_ins_rows.append(('company_mgmt', lid, 'user_name', user_name, 'insert', user_name))
        user_log_ins_rows.append(('company_mgmt', lid, 'ticker', user_name, 'insert', ticker))
        user_log_ins_rows.append(('company_mgmt', lid, 'country', user_name, 'insert', country))
        user_log_ins_rows.append(('company_mgmt', lid, 'logo', user_name, 'insert', logo))
        user_log_ins_rows.append(('company_mgmt', lid, 'updated_user', user_name, 'insert', user_name))
        user_log_stmt = """ INSERT INTO user_log(table_name, table_row_id, table_column, user_name, action, value) VALUES(%s, %s, %s, %s, %s, %s) """
        m_cur.executemany(user_log_stmt, user_log_ins_rows)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]

    def setup_new_url(self, ijson):
        project_id  = ijson.get("project_id")
        user_id     = ijson.get("user")
        urlname     = ijson.get("link")
        conn, cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)

        sql2 = "insert into url_name_info (company_row_id, url_name, user_id,status) values (%s,%s,%s,%s)"
        value = (project_id, urlname,user_id,"Y")
        cur.execute(sql2, value) 
        conn.commit()
        cur.close()
        conn.close()
        res = [{"message":"done"}]
        return res

    def get_redis_conn(self, redis_ip, redis_port):
        r = redis.Redis( host= redis_ip, port= redis_port, password='')
        return r

    def get_training_status_mgmt(self, ProjectID, cur, conn):
        sql = "select url_id, training_status from training_status_mgmt where project_id = '%s'"
        cur.execute(sql % (ProjectID))
        res = cur.fetchall()
        ret = {}
        for r in res:
            url_id = int(r[0])
            training_status = str(r[1])
            ret[url_id] = training_status
        return ret

    def get_max_url_id(self, ijson):
        project_id  = ijson.get("project_id","Amazon")
        user_id     = ijson.get("user_id","tas")
        r = self.get_redis_conn(config.Config.dbinfo['host'], config.Config.dbinfo['redis_port'])
        pkey = 'webextract_%s_urlids' %(project_id)
        ddict  = r.hgetall(pkey)
        res = []
        maxid   = 0
        for d in sorted(ddict.keys()):

            val         = ddict.get(d)
            cdict       = msgpack.unpackb(val.decode("hex"), raw=False)
            urlname     = str(cdict.get('urlname', ''))
            url_status  =  str(cdict.get('url_status', ''))
            maxid   = max(maxid, int(d))
        return [{"message":"done","data":maxid+1}]
        
    def get_url_stats(self, ijson):
        project_id  = ijson.get("project_id","Amazon")
        user_id     = ijson.get("user_id","tas")
        conn, cur   = conn_obj.MySQLdb_connection(config.Config.dbinfo)
        #sql         = "select project_id, url_id, urlname, status from project_url_mgmt where project_id = '%s'"
        #cur.execute(sql % (project_id))
        #res         = cur.fetchall()
        render_status_dict = self.get_training_status_mgmt(project_id, cur, conn)
        conn, cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql         = "select company_row_id, url_id, url_name, status from url_name_info where company_row_id = '%s'"
        cur.execute(sql % (project_id))
        res1         = cur.fetchall()
        res = []
        for r in res1:
            company_row_id, url_id, urlname, status = r
            status = render_status_dict.get(int(url_id), 'N')
            res.append((company_row_id, url_id, urlname, status))
        #r = self.get_redis_conn(config.Config.dbinfo['host'], config.Config.dbinfo['redis_port'])
        #pkey = 'webextract_%s_urlids' %(project_id)
        #ddict  = r.hgetall(pkey)
        if 0:
            #for d in sorted(ddict.keys()):

                val         = ddict.get(d)
                cdict       = msgpack.unpackb(val.decode("hex"), raw=False)
                urlname     = str(cdict.get('urlname', ''))
                url_status  =  str(cdict.get('url_status', ''))
                status = render_status_dict.get(int(d))
                #print urlname, "====", url_status, "====", status
                if not status:continue
                #tdict = {'urlid': d, 'urlname':urlname, 'url_status':url_status, 'status':status}
                res.append((project_id, d, urlname, status))

        stats       = {}
        #{"src_date_time":"7/16/19 5:45 am PDT","Headline":"Schwab Reports Net Income of $937 Million, Up 8%, Posting the Strongest Second Quarter in Company History","File Description":"The Charles Schwab Corporation announced today that its net income for the second quarter of 2019 was $937 million, down 3% from $964 million for the prior quarter, and up 8% from $866 million for the second quarter of 2018. Net income for the six months ended June 30, 2019 was a record $1.9 billion, up 15% from the year-earlier period.\n\n\n\n\n\n \n\n\n\n\n \n\n\n\n\nThree Months Ended\nJune 30,\n\n\n\n\n \n\n\n\n\n%\n\n\n\n\n \n\n\n\n\nSix Months Ended\nJune 30,\n\n\n\n\n \n\n\n\n\n%\n\n\n\n\n\n\nFinancial Highlights\n\n\n\n\n \n\n\n\n\n2019\n\n\n\n\n  more...","linkid":"219","link":"https://pressroom.aboutschwab.com/press-release/corporate-and-financial-news/schwab-reports-net-income-937-million-8-posting-strongest"} 
        done_d  = {}
        year_d  = {}
        url_d   = {}
        overall = {}
        sn  = 1
        recent  = []
        done_url    = {}
        recent_data = {}
        #res+[()]
        doc_d   = 1
        #print sql%(project_id)
        done_urls_d = {}
        for r in res:
            #print '\t', r
            project_id, url_id, rurlname, status = r
            sql = "select record_id, urlname, meta_data, process_time from scheduler_download_common_new_%s_%s.link_mgmt_meta_data where active_status='Y'"%(project_id, url_id)
            try:
                cur.execute(sql)
                tmpres  = cur.fetchall()
            except:
                tmpres  = []
            #stats.setdefault(url_id, {'total':0, 'processed':0, 'pending':0, 'rejected':0})
            tmplst  = []
            urlname = rurlname
            udata = urlparse(urlname)
            domain  = udata.netloc.split('/')[-1]
            if (domain, udata.path) in done_url:
                url_id  = done_url[(domain, udata.path)]
            else:
                done_url[(domain, udata.path)]   = url_id
            for tmpr in tmpres:
                document_id, urlname, meta_data, process_time   = tmpr
                #print (url_id, domain, tmpr[0])
                document_id = str(doc_d)
                doc_d   += 1
                meta_data       = json.loads(unquote(meta_data).decode('utf8'))
                #print (url_id, urlname, meta_data)
                #print (meta_data["src_date_time"], self.dateformat[domain])
                src_date_time   = ''
                for frmt in self.dateformat.get(domain, []):
                    try:
                        src_date_time   = datetime.datetime.strptime(str(meta_data["src_date_time"]), frmt)
                    except:
                        try:
                            src_date_time   = datetime.datetime.strptime(str(meta_data["src_date_time"].split()[0]), frmt.split()[0])    
                        except:pass
                    if src_date_time:break
                if not src_date_time:
                    print 'ERROR ', (url_id, meta_data["src_date_time"], domain, self.dateformat.get(domain, []))
                    continue
                tmplst.append((document_id, urlname, process_time, meta_data, src_date_time))
            urlname = rurlname
            urlname = urlname.decode("iso-8859-1")
            if isinstance(urlname, unicode):
                urlname = urlname.encode('utf-8')
            url_d.setdefault(url_id, {'n':domain, 'link':urlname, 's':status, 'sn':sn, 'uinfo':{}, 'info':udata.path, 'tsts':status})
            sn  += 1
            tmplst.sort(key=lambda x:x[4], reverse=True)
            stats   = {'total':{}, 'processed':{}, 'pending':{}, 'rejected':{}}
            for ii, tmpr in enumerate(tmplst):
                document_id, urlname, process_time, meta_data, src_date_time    = tmpr
                year            = int(src_date_time.strftime('%Y'))
                #print (url_id, domain, document_id, src_date_time)
                #print year
                if year < 2000:continue
                urlname = urlname.decode("iso-8859-1")
                if isinstance(urlname, unicode):
                    urlname = urlname.encode('utf-8')
                if ii == 0:
                    if domain not in done_urls_d:
                        recent_data[url_id] = src_date_time
                        recent.append({'n':domain, 'sd':src_date_time.strftime('%d-%b-%Y %H:%M'), 'count':1, 'd':str(document_id), 'link':urlname})
                        done_urls_d[domain] = 1
                month           = src_date_time.strftime('%b')
                Headline        = meta_data['Headline']
                #desc            = meta_data['File Description']
                stats['total'][document_id]   = 1
                year_d.setdefault(year, {}).setdefault(month, {}).setdefault(url_id, {'total':0, 'processed':0, 'pending':0, 'rejected':0})
                year_d[year][month][url_id]['total']    += 1
                year_d.setdefault('Overall', {}).setdefault(year, {}).setdefault(url_id, {'total':0, 'processed':0, 'pending':0, 'rejected':0})
                year_d['Overall'][year][url_id]['total']    += 1
                done_sts    = 'N'
                if done_d.get(str(document_id)) == 'Y':
                    done_sts    = 'Y'
                    stats['processed'][document_id]   = 1
                    year_d[year][month][url_id]['processed']        += 1
                    year_d['Overall'][year][url_id]['processed']    += 1
                elif done_d.get(str(document_id)) == 'R':
                    stats['rejected'][document_id]   = 1
                    year_d[year][month][url_id]['rejected']        += 1
                    year_d['Overall'][year][url_id]['rejected']    += 1
                    done_sts    = 'R'
                else:
                    stats['pending'][document_id]   = 1
                    year_d[year][month][url_id]['pending']        += 1
                    year_d['Overall'][year][url_id]['pending']    += 1
                    done_sts    = 'P'
                #Headline    = Headline.decode("iso-8859-1")
                if isinstance(Headline, unicode):
                    Headline = Headline.encode('utf-8')
                dtype   = 'Link'
                if urlname.split('?')[0].split('.')[-1].lower() == 'pdf':
                    dtype   = 'PDF'
                #print 'YES'
                url_d[url_id]['uinfo'][str(document_id)]    = {'n':Headline, 'link':urlname, 'sn':ii+1, 'sd':src_date_time.strftime('%d-%b-%Y %H:%M'), 's':done_sts, 'dtype':dtype}
                url_d[url_id]['uinfo'][str(document_id)]['sn'] = len(url_d[url_id]['uinfo'].keys())
            if not url_d[url_id]['uinfo']:
                url_d[url_id]['s']  = 'N'
            if url_d[url_id]['s']  == 'Y':
                url_d[url_id]['s'] = 'success'
            elif url_d[url_id]['s']  == 'N':
                url_d[url_id]['s'] = 'danger'
            total   = len(stats['total'].keys())
            pending = len(stats['pending'].keys())
            processed = len(stats['processed'].keys())
            rejected = len(stats['rejected'].keys())
            pending_p       = 0
            processed_p     = 0
            rejected_p      = 0
            if total:
                pending_p   = int((float(pending)/total)*100)
                processed_p   = int((float(processed)/total)*100)
                rejected_p   = int((float(rejected)/total)*100)
            tmpsts  = url_d[url_id].get('stats', {})
            tmpsts['pending_c']  = tmpsts.get('pending_c', 0)+pending
            tmpsts['pending_p']  = tmpsts.get('pending_p', 0)+pending_p
            tmpsts['processed_c']  = tmpsts.get('processed_c', 0)+processed
            tmpsts['processed_p']  = tmpsts.get('processed_p', 0)+processed_p
            tmpsts['rejected_c']  = tmpsts.get('rejected_c', 0)+rejected
            tmpsts['rejected_p']  = tmpsts.get('rejected_p', 0)+rejected_p
            
                
            
            url_d[url_id]['stats']  = tmpsts
        gdata   = []
        years   = filter(lambda x:x != 'Overall', year_d.keys())
        years.sort(reverse=True)
        years   = ['Overall']+years
        for y in years[:]:
            months  = self.month_map
            if y == 'Overall' and y in year_d:
                months  = year_d[y].keys()
                months.sort(reverse=True)
            dd  = {'categories':months, 'series':[], 'n':y}
            for urlid in url_d.keys():
                tdd  = {'name':url_d[urlid]['n'], 'data':[]}
                for m in months:
                    tdd['data'].append(year_d.get(y, {}).get(m, {}).get(urlid, {}).get('total', 0))
                dd['series'].append(tdd)
            gdata.append(dd)
        u_ar    = []
        urls    = url_d.keys()
        urls.sort(key=lambda x:recent_data.get(x, datetime.datetime.strptime('1900', '%Y')), reverse=True)
        for urlid in urls:
            u_ar.append(url_d[urlid])
            u_ar[-1]['uid'] = urlid
                
        return [{'message':'done','data':u_ar, 'ysts':gdata, 'recent':recent}]

    def dinsert_demo_doc(self, ijson):
        dbname =  ''
        projectid = ''
        dbinfo  = copy.deepcopy(config.Config.dbinfo)
        dbinfo['host']  = '172.16.20.10'
        dbinfo['db']    = ijson['dbname']
        conn, cur   = conn_obj.MySQLdb_connection(config.Config.dbinfo)
        pass

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def insert_demo_doc(self, ijson):
        db_name            = ijson["db_name"]
        project_id   = ijson["project_id"]
        batch_dct    = ijson["batch_dct"]
        
        db_data_lst = ['172.16.20.10', 'root', 'tas123', db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        batch_str = ', '.join(["'"+str(e)+"'" for e in batch_dct.keys()])
        read_qry = """  SELECT doc_id, batch, doc_name, doc_type, meta_data FROM batch_mgmt_upload WHERE batch in (%s)  """%(batch_str)                
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        data_rows = []
        for row in t_data:
            doc_id, batch, doc_name, doc_type, meta_data = row
            meta_data = eval(meta_data)
            meta_data.update({'doc_name':str(doc_name), 'doc_type':str(doc_type)}) 
            try:
                get_cid = batch_dct[str(batch)]
            except:get_cid = "2"
            data_rows.append((str(project_id), str(get_cid), str(meta_data), str(doc_id), 'Y'))  

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        ins_stmt = """ INSERT INTO document_mgmt(project_id, company_row_id, meta_data, doc_id, status) VALUES(%s, %s, %s, %s, %s)  """ 
        m_cur.executemany(ins_stmt, data_rows)
        m_conn.commit()
        m_conn.close() 
        return  
    def insert_demo_doc_no_batch(self):
        sys.exit()
        db_name  = 'FactSheet_Tree'        #ijson["db_name"]
        project_id =  '40'            #ijson["project_id"]
        doc_ids  =  ['104', '109', '110'] #   ['88', '87', '102', '103', '104', '107', '108', '109', '110']             #ijson["doc_lst"]
        company_row_id = '16' #"9" 

        db_data_lst = ['172.16.20.10', 'root', 'tas123', db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        doc_str = ', '.join(["'"+str(e)+"'" for e in doc_ids])
        read_qry = """  SELECT doc_id, batch, doc_name, doc_type, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s)  """%(doc_str)                
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        data_rows = []
        for row in t_data:
            doc_id, batch, doc_name, doc_type, meta_data = row
            meta_data = eval(meta_data)
            meta_data.update({'doc_name':str(doc_name), 'doc_type':str(doc_type)}) 
            data_rows.append((str(project_id), str(company_row_id), str(meta_data), str(doc_id), 'Y'))  

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        ins_stmt = """ INSERT INTO document_mgmt(project_id, company_row_id, meta_data, doc_id, status) VALUES(%s, %s, %s, %s, %s)  """ 
        m_cur.executemany(ins_stmt, data_rows)
        m_conn.commit()
        m_conn.close() 
        return  
    
    def add_companies(self):
        company_display_name = 'BristolMyersSquibbCompany'
        company_name= ''.join(company_display_name.split())
        meta_data = '{"company_name":"BristolMyersSquibbCompany", "deal_id":"88", "industry_type":"Pharmaceutical", "model_number":"1", "project_id":"1", "project_name":["Credit Model", "Schroders"]}'
        user_name = 'demo'
        insert_time = ''
        project_id = 'FE'        

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        ins_stmt = """ INSERT INTO company_mgmt(company_name, company_display_name, meta_data, user_name, insert_time) VALUES('%s', '%s', '%s', '%s', '%s'); """%(company_name, company_display_name, meta_data, user_name, insert_time)
        m_cur.execute(ins_stmt)
        m_conn.commit()
 
        read_qry = """ SELECT max(row_id) FROM company_mgmt;  """
        m_cur.execute(read_qry)
        crid   =  int(m_cur.fetchone()[0])
        
        insp_stmt = """ INSERT INTO project_company_mgmt(project_id, company_row_id, user_name, insert_time) VALUES('%s', '%s', 'demo', '');  """%(project_id, str(crid)) 
        m_cur.execute(insp_stmt)
        m_conn.commit()
        m_conn.close() 

    def insert_demo_doc_1(self):
        #sys.exit()
        project_id =  'FE'            #ijson["project_id"]
        doc_ids  =  ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44']
        company_row_id = '6' #"9" 
        
        db_path = '/mnt/eMB_db/%s/%s/tas_company.db'%('Westjetairlinesltd', '1')
        m_conn = sqlite3.connect(db_path)
        m_cur  = m_conn.cursor()
        doc_str = ', '.join(["'"+str(e)+"'" for e in doc_ids]) 
        read_qry = """  SELECT doc_id, document_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to FROM company_meta_info WHERE doc_id in (%s)  """%(doc_str)                
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        data_rows = []
        for row in t_data:
            doc_id, document_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to = row
            meta_data = {'doc_type':document_type, 'period':period, 'reporting_year':reporting_year, 'doc_name':doc_name, 'doc_release_date':doc_release_date, 'doc_from':doc_from, 'doc_to':doc_to}
            data_rows.append((str(project_id), str(company_row_id), str(meta_data), str(doc_id), 'Y'))  
        
        #for k in data_rows: 
        #    print k
        #    print 
        #sys.exit()
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        ins_stmt = """ INSERT INTO document_mgmt(project_id, company_row_id, meta_data, doc_id, status) VALUES(%s, %s, %s, %s, %s)  """ 
        m_cur.executemany(ins_stmt, data_rows)
        m_conn.commit()
        m_conn.close() 
        return  
        
    def update_meta_10(self, ijson):
        user_name = str(ijson.get('user', 'demo'))
        project_id       =  str(ijson['project_id'])
        demo_project_id       =  str(ijson.get('demo_project_id', project_id))
        if not user_name:
            user_name = 'demo'
        callback_str = 'http://172.16.20.229:7777/status_update?project_id=%s&company_id=%s&doc_id=%s&user=%s'%(str(demo_project_id), str(ijson['i_company_id']), str(ijson['doc_lst']), user_name) 
        db_name = ijson['db_name']
        db_data_lst = ['172.16.20.10', 'root', 'tas123', '%s'%(db_name)] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        dc_id = str(ijson['doc_lst'])
        read_qry = """  SELECT meta_data FROM batch_mgmt_upload WHERE doc_id='%s' """%(dc_id)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchone()
        if t_data:
            try:
                meta_data = json.loads(t_data[0])
            except:
                meta_data = eval(t_data[0])
            meta_data['call_back'] = callback_str
            #print meta_data
            update_stmt = """ UPDATE batch_mgmt_upload SET meta_data="%s" WHERE doc_id='%s' """%(str(meta_data), dc_id)
            m_cur.execute(update_stmt)
            m_conn.commit()
        m_conn.close()
        self.execute_url(ijson)
        res = [{"message":"done"}]
        return res
        
    def data_path_url_execution(self, ijson):
        import url_execution as ue
        u_Obj = ue.Request()
        path = ijson['path']
        http_method = ijson.get('method', 'GET')
        data = json.dumps(ijson['data'])
        url_info = ''.join([path, data]) 
        #print 'DDDDDDDDDDDDDD', url_info
        txt, txt1   = u_Obj.load_url(url_info, 120)
        #print txt1, txt
        return txt1
    
    def data_path_method_url_execution(self, ijson):
        '''
        conn  = httplib.HTTPConnection(self.config.get('pr_link','value'),timeout=10)
        headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
        conn.request("POST","/tree_data", json.dumps(fs_in), headers)
        response = conn.getresponse()
        data = response.read()
        conn.close() 
        '''
        import url_execution as ue
        u_Obj = ue.Request()
        path = ijson['path']
        http_method = ijson.get('method', 'GET')
        data = ijson['data']
        if http_method  == 'GET':
            data = json.dumps(data)
            url_info = ''.join([path, data])
            #print 'DDDDDDDDDDDDDD', url_info
            txt, txt1   = u_Obj.load_url(url_info, 120)
            data = json.loads(txt1)
            #print txt1, txt
            return data
        elif http_method == 'POST':
            splt_ar = path.split("://")
            path_host_lst = splt_ar[1].split("?")[0].split("/")
            path_host  = path_host_lst[0]
            extention = '/' + '/'.join(path_host_lst[1:])
            get_input_key = ijson.get('input', '')
            if not get_input_key:
                params  =  json.dumps(data[0])
            elif get_input_key:
                params = json.dumps({get_input_key:data[0]})
            headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
            conn  = httplib.HTTPConnection(path_host, timeout=120)
            conn.request("POST",  extention, params, headers)
            response = conn.getresponse()
            d_info = response.read()
            conn.close()
            return json.loads(d_info)
        elif http_method == 'WEB':
            data = data[0]
            url_info = path + json.dumps(data)
            webUrl = urllib.urlopen(url_info)
            data = webUrl.read()
            result = [{'message': 'done', 'data': json.loads(data)}]
            return result
    
    def execute_url(self, ijson):
        j_ijson = json.dumps(ijson)
        import url_execution as ue
        u_Obj = ue.Request()
        url_info = 'http://172.16.20.10:5008/tree_data?input=[%s]'%(j_ijson)
        print url_info
        txt, txt1   = u_Obj.load_url(url_info, 120)
        print 'SSSSSS', (txt, txt1)
        data = json.loads(txt1)
        return data#[{'message':'done', 'data':data}]
    
    def insert_into_demo(self, ijson):
        docid_lst        = ijson['doclist']
        project_id       = str(ijson['project_id'])
        company_row_id             = str(ijson['i_company_id'])
        doc_type         = str(ijson['type'])
        
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
            
        ''' 
        read_qry = """ SELECT project_id, company_row_id FROM company_project_mgmt WHERE project_id='%s'   """%(project_id) 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        all_row_str = ', '.join([str(r[0]) for r in t_data])
        
        rd_qy = """ SELECT row_id FROM company_mgmt WHERE row_id in (%s) AND company_name=%s """%(all_row_str, company_name)
        m_cur.execute(rd_qy)
        company_row_id = m_cur.fetchone()
        '''
        
        chk_read = """ SELECT project_id, company_row_id, doc_id FROM document_mgmt WHERE project_id='%s' AND company_row_id='%s' """%(project_id, company_row_id)
        m_cur.execute(chk_read)
        t_info = m_cur.fetchall()       
            
        info_chk_dct = {}
        for rw in t_info:
            pid, crid, dcid = map(str, rw)
            info_chk_dct[(pid, crid, dcid)] = 1

        insert_rows = []
        update_rows = []
        
        for dc_info in docid_lst:
            dc, meta = str(dc_info['doc_id']),  str(dc_info['meta_data'])                   
            if (project_id, company_row_id, dc) in info_chk_dct:
                update_rows.append((meta, project_id, company_row_id, dc))                
            elif (project_id, company_row_id, dc) not in info_chk_dct:
                if doc_type == 'PDF':
                    ipath = os.path.join(self.out_dir, dc, 'sieve_input')
                    n2 = os.path.join(ipath, dc+'.pdf')
                    cmd = 'qpdf  --show-npages %s'%(n2)
                    process = subprocess.Popen(cmd , stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)    
                    op = process.communicate()
                    print '>>>', op
                    nop = op[0].strip()
                    #nop = str(self.read_txt_from_server(dc_page_path)[0])
                elif doc_type == 'HTML':
                    nop = '1' 
                insert_rows.append((project_id, company_row_id, meta, dc, 'N', nop))
        
        print 'ss', insert_rows
        #print 'tt', update_rows
        #sys.exit()
        if insert_rows:
            ins_stmt = """ INSERT INTO document_mgmt( project_id, company_row_id, meta_data, doc_id, status, no_of_pages) VALUES(%s, %s, %s, %s, %s, %s)  """
            
            m_cur.executemany(ins_stmt, insert_rows)
        if update_rows:
            update_stmt = """ UPDATE document_mgmt SET meta_data=%s WHERE project_id=%s AND company_row_id=%s AND doc_id=%s   """
            m_cur.executemany(update_stmt, update_rows)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]

    def doc_wise_meta_info(self, doc_list, db_name):
        db_data_lst = ['172.16.20.10', 'root', 'tas123', db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        doc_str = ', '.join(["'"+str(e)+"'" for e in doc_list])
        read_qry = """ SELECT doc_id, doc_name, doc_type, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s)  """%(doc_str)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        doc_meta_map_lst = []
        for row in t_data:
            doc_id, doc_name, doc_type = row[:-1]
            try:
                meta_data = eval(row[-1])
            except:meta_data = {}
            meta_data.update({'doc_name':doc_name, 'doc_type':doc_type})
            dt_dct = {'doc_id':str(doc_id), 'meta_data':meta_data, 'doc_type':doc_type}
            doc_meta_map_lst.append(dt_dct)
        return doc_meta_map_lst        
 
    def upload_document_info(self, ijson):
        doc_lst        =  ijson['doclist']
        project_id       =  str(ijson['project_id'])
        demo_project_id       =  str(ijson.get('demo_project_id', project_id))
        company_row_id   =  str(ijson['i_company_id'])
        db_name          =  ijson['db_name'] 
        user_name        = str(ijson.get('user', 'demo'))
        #print 'ijson ', ijson
        if not user_name:
            user_name = 'demo'
       
        doc_meta_map_lst = self.doc_wise_meta_info(doc_lst, db_name)
        #print doc_lst, project_id, company_row_id, db_name, doc_meta_map_lst;sys.exit()
         
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
            
        chk_read = """ SELECT project_id, company_row_id, doc_id FROM document_mgmt WHERE project_id='%s' AND company_row_id='%s' """%(demo_project_id, company_row_id)
        m_cur.execute(chk_read)
        t_info = m_cur.fetchall()       
            
        info_chk_dct = {}
        for rw in t_info:
            pid, crid, dcid = map(str, rw)
            info_chk_dct[(pid, crid, dcid)] = 1

        insert_rows = []
        update_rows = []
        
        def reaf_page_info(path):
            no_pages=0;
            if os.path.exists(path):  
                f = open(path)
                no_pages = f.read()
                f.close()
            return no_pages
        
        for dc_info in doc_meta_map_lst:
            dc, meta, doc_type = str(dc_info['doc_id']),  str(dc_info['meta_data']), dc_info['doc_type']                  
            if meta == 'None':
                meta = '{}'

            if (demo_project_id, company_row_id, dc) in info_chk_dct:
                update_rows.append((meta, demo_project_id, company_row_id, dc, user_name))                

            elif (demo_project_id, company_row_id, dc) not in info_chk_dct:
                if doc_type == 'PDF':
                    dc_page_path = '/var/www/html/WorkSpaceBuilder_DB_demo/%s/1/pdata/docs/%s/pdf_total_pages'%(project_id, dc)
                    if not os.path.exists(dc_page_path):
                        nop = '0'
                    elif os.path.exists(dc_page_path): 
                        nop = reaf_page_info(dc_page_path)
                elif doc_type == 'HTML':
                    nop = '1' 
                insert_rows.append((demo_project_id, company_row_id, meta, dc, 'N', nop, user_name))
        
        #print 'ss', insert_rows
        #print 'tt', update_rows
        #sys.exit()
        if insert_rows:
            ins_stmt = """ INSERT INTO document_mgmt( project_id, company_row_id, meta_data, doc_id, status, no_of_pages, user_name) VALUES(%s, %s, %s, %s, %s, %s, %s)  """
            m_cur.executemany(ins_stmt, insert_rows)
        if update_rows:
            update_stmt = """ UPDATE document_mgmt SET meta_data=%s WHERE project_id=%s AND company_row_id=%s AND doc_id=%s AND user_name='%s'  """
            m_cur.executemany(update_stmt, update_rows)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]

    def read_all_company_info(self, ijson):
        user_name  = str(ijson.get('user', 'demo'))
        if not user_name:
            user_name = 'demo'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        read_qry = """ SELECT row_id, company_name, company_display_name, user_name FROM company_mgmt WHERE user_name='%s' ; """%(user_name)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        res_lst = []
        for row in t_data:
            company_row_id, company_name, company_display_name, usr_nm  = row
            if company_row_id == 7:continue
            dt_dct = {'company_id':company_name, 'company_name':company_display_name, 'crid':company_row_id, 'flg':1, 'user':usr_nm}
            res_lst.append(dt_dct)
        res = [{'message':'done', 'data':res_lst}]
        return res

    def get_projectid_info(self, project_name):
        ru_Obj = Request()
        s_json = {"user_id":"sunil","ProjectName":"%s"%(project_name), "oper_flag":3}
        as_json = json.dumps(s_json)
        url_info = """ http://172.16.20.10:5007/tree_data?input=[%s] """%(as_json)
        print url_info
        #sys.exit('MT')
        txt, txt1   = ru_Obj.load_url(url_info, 120)
        #print (txt, txt1, type(txt1))
        data = json.loads(txt1)
        #print data, type(data)
        info_dct = data[0]['data']
        project_id = int(info_dct.get('ProjectID', 0))
        if not project_id:
            return '', ''
        user_id    = str(info_dct['UserID'])
        p_name     = str(info_dct['ProjectName'])
        db_name = '_'.join(p_name.split())

        ###################  DB creation ##################
        # {"user_id":"sunil","ProjectID":44,"WSName":"1","db_name":"muthu_test_proj","oper_flag":90014} 
        d_json = {"user_id":"%s"%(user_id),"ProjectID":"%d"%(project_id),"WSName":"1","db_name":"%s"%(db_name),"oper_flag":90014}
        ad_json = json.dumps(d_json) 
        d_url_info = """ http://172.16.20.10:5007/tree_data?input=[%s] """%(ad_json)
        #print d_url_info
        dtxt, dtxt1   = ru_Obj.load_url(d_url_info, 120)
        #print (dtxt, dtxt1)
        d_data = json.loads(dtxt1)
        d_info_dct = d_data[0]['data'] 
        d_db_name = d_info_dct['DBName']
        return project_id, db_name

    def call_module_mgmt_user_save(self, ijson, project_id):
        dc_ijson =  {}
        dc_ijson['data']          =   ijson['data']
        dc_ijson['project_id']    =   project_id
        user_name   = str(ijson.get('user', 'demo'))
        if not user_name:
            user_name = 'demo'
        dc_ijson['user'] = user_name 
                 
        import module_storage_info_project_company_mgmt as mpyf
        m_Obj = mpyf.PC_mgmg()
        m_Obj.user_save(dc_ijson)
        return 

    def project_configuration(self, ijson):
        if ijson.get('PRINT', 'N') != 'Y':
            disableprint() 
        pc_data = ijson['pc_data']
        project_name = pc_data['project_name'] 
        description  = pc_data['desc'] 
        user_name    = str(ijson['user'])
        project_id, db_name =  self.get_projectid_info(project_name)
        project_id, db_name = map(str, [project_id, db_name])
        print project_id, db_name
        if not project_id:
            return [{'message':'Project Already Exists'}]            
        dt_time = str(datetime.datetime.now())
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        insert_stmt = """ INSERT INTO project_mgmt(project_id, project_name, description, user_name, insert_time, db_name) VALUES('%s', '%s', '%s', '%s', '%s', '%s')  """%(project_id, project_name, description, user_name, dt_time, db_name) 
        print insert_stmt
        m_cur.execute(insert_stmt)

        '''
        insert company info for respective company
        '''
        
        project_company_info = pc_data['info']
        #print project_company_info;sys.exit()
        insert_rows = []
        for row_dct in project_company_info:
            crid = str(row_dct['crid'])
            dt_tup = (project_id, crid, user_name, dt_time)
            insert_rows.append(dt_tup)
        if insert_rows:
            ins_stmt = """ INSERT INTO project_company_mgmt(project_id, company_row_id, user_name, insert_time) VALUES(%s, %s, %s, %s)   """
            m_cur.executemany(ins_stmt, insert_rows)
            m_conn.commit()
        m_conn.close()
        self.call_module_mgmt_user_save(ijson, project_id) 
        enableprint()
        res = [{"message":"done", 'project_id':project_id}]
        return res        
        
    def scheduler_process_mgmt_insert(self, ijson):
        '''
        s =         {
        "cmd_id": 3,
        "user_id": 21,
        "project_id" : this.gbl_project_id,
        "1": this.gbl_schedule_id,
        "2": start_db_dtime,
        "3": end_db_dtime,
        "4": repeate_status,
        "5": rec_val,
        "8": scheduler_status,
        "9": main_str
                    }     
        '''
        #sys.exit() 
        user_id           = ijson['user_id']
        p_id              = str(ijson['project_id'])
        schedule_id        = str(ijson['1'])
        start_db_dtime    = str(ijson['2'])
        end_db_dtime      = str(ijson['3'])
        repeate_status    = str(ijson['4'])
        rec_val           = str(ijson['5']) 
        scheduler_status  = str(ijson['8']) 
        main_str          = str(ijson['9'])     
        title             = str(ijson['title'])     
        from config import Config 
        c_Obj     = Config()
        host_info = c_Obj.s_dbinfo       
        db_data_lst = [host_info["host"], host_info["user"], host_info["password"], host_info["db"]] 
        print db_data_lst
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        del_stmt = """  DELETE FROM scheduler_process_mgmt WHERE project_id='%s' AND schedule_id='%s'   """%(p_id, schedule_id)
        m_cur.execute(del_stmt)
        # format(json["1"], json["project_id"], json["1"], json["user_id"], json["8"], json["4"], json["5"], json["2"], json["3"], json["9"])
        insert_stmt = """  INSERT INTO scheduler_process_mgmt(schedule_id, project_id, url_id, user_id, status,schedule_repeat_flag,schedule_repeat_interval,schedule_start_time,schedule_end_time,schedule_pattern, title) VALUES ('%s', '%s', '%s', '%s', '%s', '%s',  '%s', '%s', '%s', '%s', '%s')  """%(schedule_id, p_id, schedule_id, user_id, scheduler_status, repeate_status, rec_val, start_db_dtime, end_db_dtime, main_str, title)
        print insert_stmt
        m_cur.execute(insert_stmt)
        
        d_stmt = """ DELETE FROM changedetection_process_mgmt WHERE project_id='%s' AND url_id='%s'  """%(p_id, schedule_id)
        m_cur.execute(d_stmt) 
            
        # project_id | url_id | user_id | agent_id | mgmt_id | priority | running_status | active_status | changedetection_mode | ref_url_id | download_mode | create_datetime     
        i_stmt = """ INSERT INTO changedetection_process_mgmt(project_id, url_id, user_id, agent_id, mgmt_id, priority, running_status, active_status, changedetection_mode, ref_url_id, download_mode) VALUES ('%s', '%s', '%s', '%s', '%s', '%s',  '%s', '%s', '%s', '%s', '%s') """%(p_id, schedule_id, user_id, '1', '1', '0', 'Y', 'Y', 'U', '0', 'Y')
        print i_stmt
        m_cur.execute(i_stmt)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]

    def get_schedule_info(self, ijson):
        project_id  = ijson.get("project_id","Amazon")
        #user_id     = ijson.get("user_id","tas")
        conn, cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql         = "select project_id, schedule_id, schedule_start_time, schedule_end_time, schedule_pattern, title from scheduler_process_mgmt where project_id='%s'"%(project_id)
        cur.execute(sql)
        res = cur.fetchall()
        far = []
        for r in res:
            project_id, schedule_id, schedule_start_time, schedule_end_time, schedule_pattern, title   = r
            dd  = {'title':title, 'start':schedule_start_time.strftime('%Y-%m-%d %H:%M:%S'), 'end':schedule_end_time.strftime('%Y-%m-%d %H:%M:%S')}
            far.append(dd)
        res = [{'message':'done', 'data':far}]
        return res
    
    
    def insert_url_name_data(self, ijson):
        user_name = str(ijson.get('user', 'demo'))
        if not user_name:
            user_name = 'demo'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
            
        company_row_id = str(ijson['crid'])
        client_url_id  = ijson['monitor_id']
        user_id     = ijson.get("user_id","tas")
        r = self.get_redis_conn(config.Config.dbinfo['host'], config.Config.dbinfo['redis_port'])
        pkey = 'webextract_%s_urlids' %(client_url_id)
        ddict  = r.hgetall(pkey)
        res_lst = []
        for d in sorted(ddict.keys()):
            val         = ddict.get(d)
            cdict       = msgpack.unpackb(val.decode("hex"), raw=False)
            urlname     = str(cdict.get('urlname', ''))
            if (company_row_id, urlname, project_id, 'Y', user_id) not in res_lst:
                res_lst.append((company_row_id, urlname, client_url_id, 'Y', user_id, user_name)) 
        
        ins_stmt = """ INSERT INTO url_name_info(company_row_id, url_name, client_url_id, status, user_id, user_name) VALUES(%s, %s, %s, %s, %s, %s)     """
        m_cur.executemany(ins_stmt, res_lst)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]

    def delete_url_data(self, ijson):
        project_id  = ijson.get("project_id")
        user_id     = ijson.get("user_id")
        url_id      = ijson.get("url_id")
        user_name = str(ijson.get('user', 'demo'))
        if not user_name:
            user_name = 'demo'

        conn, cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql = "delete from url_name_info where company_row_id= '%s' and url_id = '%s' and user_name='%s' "
        cur.execute(sql % (project_id, url_id, user_name))
        conn.commit()
        cur.close()
        conn.close()
        return [{"message":"done"}]
    
    def validate_login(self, ijson):
        import login.user_info as login
        obj = login.Login(config.Config.s_dbinfo)
        return obj.validate_login(ijson)
    
    def project_wise_doc_info_10(self, ijson):
        db_name = ijson['db_name']
        db_data_lst = ['172.16.20.10', 'root', 'tas123', db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        # doc_id | batch      | doc_name          | doc_type | processed_date_time        | meta_data
        read_qry = """ SELECT doc_id, batch, doc_name, doc_type, meta_data, status FROM batch_mgmt_upload ORDER BY doc_id; """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()  
        res_lst = []
        for row in t_data:      
            doc_id, batch, doc_name, doc_type, meta_data, status = row
            meta_data = eval(meta_data) 
            company_name = meta_data.get('Company', '')
            if (company_name == 'test') or (not company_name):continue
            print meta_data
            try:
                pt = meta_data['periodtype']
            except:pt = meta_data.get('Period Type', '')
            year = meta_data.get('Year', '')
            fye = meta_data.get('FYE', '')
            dt_dct = {'d':doc_id, 'batch':batch, 'doc_name':doc_name, 'doc_type':doc_type, 'Company':company_name, 'periodtype':pt, 'Year':year, 'FYE':fye, 'status':status}
            res_lst.append(dt_dct)
        return [{'message':'done', 'data':res_lst}]

    def add_new_company_docs(self, ijson):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        sql = "select project_id, project_name from project_mgmt"
        m_cur.execute(sql)
        res = m_cur.fetchall()
        sql = "select row_id, company_display_name from company_mgmt"
        m_cur.execute(sql)
        res1 = m_cur.fetchall()
        sql = "select project_id, company_row_id from project_company_mgmt"
        m_cur.execute(sql)
        res2 = m_cur.fetchall()
        sql = "select project_id, company_row_id, doc_id from document_mgmt"
        m_cur.execute(sql)
        res3 = m_cur.fetchall()
        m_cur.close()
        m_conn.close()
        e_p_d   = {}
        
        for r in res:
            project_id, project_name    = r
            project_name    = ' '.join(project_name.lower().split())
            e_p_d[project_name]   = project_id
            e_p_d[(project_id, 'P')]   =  project_name
        c_p_d   = {}
        for r in res1:
            c_id, c_name    = r
            c_name          = ' '.join(c_name.lower().split())
            c_p_d[c_name]   = c_id
        proj_comp_d = {}
        for r in res2:
            p_id, c_id    = r
            proj_comp_d[(p_id, str(c_id))] = 1
        proj_comp_doc_d = {}
        for r in res3:
            p_id, c_id, doc_id    = r
            proj_comp_doc_d.setdefault((p_id, str(c_id)), {})[str(doc_id)] = 1

        db_data_lst = ['172.16.20.10', 'root', 'tas123', 'WorkSpaceDb_DB'] 
        p_conn, p_cur = self.mysql_connection(db_data_lst)
        sql = "select ProjectID, ProjectCode from ProjectMaster"
        p_cur.execute(sql)
        res = p_cur.fetchall()
        dbname_d    = {}
        for r in res:
            ProjectID, ProjectCode  = r 
            dbname_d[str(ProjectID)]    = ProjectCode
        

        u_ar        = []
        ci_ar       = []
        p_cp_ar     = []
        doc_i_ar    = []
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        pid = ijson['project_id']
        del_ar  = []
        if 1:
            
            docids  = [] #doc_d[pid].keys()
            for c_d in ijson['data']:
                docids  += map(lambda x:str(x), c_d['docs'])
            
            db_data_lst = ['172.16.20.10', 'root', 'tas123', dbname_d[pid]] 
            p_conn, p_cur = self.mysql_connection(db_data_lst)
            read_qry = """  SELECT doc_id, batch, doc_name, doc_type, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s)  """%(', '.join(docids))                
            
            p_cur.execute(read_qry)
            t_data = p_cur.fetchall()
            p_conn.close()
            data_rows = {}
            print read_qry
            for row in t_data:
                doc_id, batch, doc_name, doc_type, meta_data = row
                print [doc_id, batch, doc_name]
                data_rows[str(doc_id)] = (meta_data, doc_type, doc_name)
            user_name   = ijson.get('user', 'demo')
            for c_d in ijson['data']:
                cname   = c_d['comp_name']
                crid    = c_d['crid']
                tmpcname    = ' '.join(cname.lower().split())
                tmpcname1    = ''.join(cname.split())
                if str(crid) == 'new':
                    ins_stmt = """ INSERT INTO company_mgmt(company_name, company_display_name, meta_data, user_name) VALUES('%s', '%s', '%s', '%s'); """%(tmpcname1, cname, '{}', 'demo')
                    m_cur.execute(ins_stmt)
                    m_conn.commit()
             
                    read_qry = """ SELECT LAST_INSERT_ID(); """
                    m_cur.execute(read_qry)
                    crid   =  str(m_cur.fetchone()[0])
                    c_p_d[tmpcname] = crid
                else:
                    crid    = str(crid)
                if(pid, crid) not in proj_comp_d:
                    p_cp_ar.append((pid, crid, 'demo'))
                del_ar.append((pid, crid))
                for doc_id in c_d['docs']:
                    if 1:#doc_id not in proj_comp_doc_d.get((pid, crid), {}):
                        dc_page_path = '/var/www/html/WorkSpaceBuilder_DB/%s/1/pdata/docs/%s/pdf_total_pages'%(pid, doc_id) 
                        txt_pg_info = '1'
                        try:
                            txt_pg_info = self.read_txt_from_server(dc_page_path)
                            txt_pg_info = str(txt_pg_info[0])
                        except:pass
                        print [doc_id]
                        meta_data, doc_type, doc_name   = data_rows[doc_id]
                        meta_data   = eval(meta_data)
                        meta_data.update({'doc_name':str(doc_name), 'doc_type':str(doc_type)}) 
                        doc_i_ar.append((pid, crid, doc_id, str(meta_data), txt_pg_info, 'Y', user_name))
        m_cur.executemany('delete from  document_mgmt where project_id=%s and company_row_id =%s', del_ar)
        m_cur.executemany('insert into document_mgmt(project_id, company_row_id, doc_id, meta_data, no_of_pages, status, user_name)values(%s,%s,%s, %s,%s,%s, %s)', doc_i_ar)
        m_cur.executemany('insert into project_company_mgmt(project_id, company_row_id, user_name)values(%s,%s,%s)', p_cp_ar)
        m_conn.commit()
        m_conn.close() 
        res = [{"message":"done"}]
        return res

    def remove_company_docs(self, ijson):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        sql = "delete from project_company_mgmt where project_id='%s' and company_row_id='%s'"%(ijson['project_id'], ijson['crid'])
        m_cur.execute(sql)
        sql = "delete from document_mgmt where project_id='%s' and company_row_id='%s'"%(ijson['project_id'], ijson['crid'])
        m_cur.execute(sql)
        m_conn.commit()
        res = [{"message":"done"}]
        return res

    def configured_users(self):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        rd_qry = """ SELECT distinct(assigned_user_id) FROM user_configuration_info """
        m_cur.execute(rd_qry)
        t_data = {str(e[0]) for e in m_cur.fetchall()}
        return t_data

    def send_user_lst(self):
        c_lst = self.configured_users()
        log_info = config.Config.s_dbinfo 
        db_name = log_info.get("db")
        db_data_lst = ['172.16.20.229', 'root', 'tas123', db_name] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """  SELECT user_id, user_name, user_role FROM login_master  """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        res_lst = []
        for row in t_data:
            user_id, user_name, user_role = row
            d_flg = 'ND'
            if user_id in c_lst:
                d_flg = 'D' 
            dt_dct = {'uid':user_id, 'un':user_name, 'ur':user_role, 'd_flg':d_flg}
            res_lst.append(dt_dct)
        lidx_lst = ['D', 'ND']
        res_lst.sort(key=lambda x:(lidx_lst.index(x['d_flg']), x['un'].lower()))
        return [{"message":"done", "data":res_lst}]

    def save_user_configurations(self, ijson):
        configured_user = ijson['user']
        save_data = ijson['data']
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        for row_dct in save_data:
            pid, comp_lst, assigned_user, assigned_user_id  = row_dct['project_id'], row_dct['info'], row_dct['un'], row_dct['uid']
            del_stmt = """ DELETE FROM user_configuration_info WHERE assigned_user_id='%s' AND project_id='%s' """%(assigned_user_id, pid)
            m_cur.execute(del_stmt)
            for comp in comp_lst:
                insert_stmt = """ INSERT INTO user_configuration_info(project_id, company_row_id, assigned_user, configured_user, assigned_user_id) VALUES('%s', '%s', '%s', '%s', '%s') """%(pid, comp, assigned_user, configured_user, assigned_user_id)
                try:
                    m_cur.execute(insert_stmt)
                except:continue
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]

    def configured_users_info(self, m_conn, m_cur, ijson):
        uid = ijson['uid']
        read_qry = """ SELECT project_id, company_row_id FROM user_configuration_info WHERE assigned_user_id='%s' ; """%(uid)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        project_comp_dct = {}
        for row in t_data:
            project_id, company_row_id = row
            project_comp_dct.setdefault(project_id, {})[company_row_id] = 1
        return project_comp_dct
        
    def user_wise_cofigured_project_data(self, ijson):
        user_name = ijson.get('user', 'demo')
        if not user_name:
            user_name = 'demo'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        
        project_comp_dct = self.configured_users_info(m_conn, m_cur, ijson)
        if not project_comp_dct:
            return [{'message':'done', 'data':[]}]  
        
        pc_info_dct = {}         
        for prj, cm_dct in project_comp_dct.iteritems():
            cmpid_str = ', '.join({"'"+str(e)+"'" for e in cm_dct})
            read_pc = """ SELECT project_id, company_row_id, user_name, meta_data, url_id FROM project_company_mgmt WHERE project_id='%s' AND company_row_id in (%s); """%(prj, cmpid_str)
            m_cur.execute(read_pc)
            pc_data = m_cur.fetchall()
        
            for row in pc_data:
                project_id, company_id, user_name = map(str, row[:-2])
                try:
                    meta_data = eval(row[-2])
                except:meta_data = {}
                url_id = row[-1]
                if not url_id:  
                    url_id = ''
                pc_info_dct.setdefault(project_id, {})[company_id] = (user_name, meta_data, str(url_id))
           
        pid_str = ', '.join(['"'+str(e)+'"' for e in pc_info_dct.keys()])
        read_qry = """  SELECT project_id, project_name, description, db_name FROM project_mgmt WHERE project_id in (%s) """%(pid_str)
        print read_qry
        m_cur.execute(read_qry)
        dt_p  = m_cur.fetchall()
        p_map_dct = {str(r[0]):(str(r[1]), str(r[2]), str(r[3])) for r in dt_p} 
        res_lst = []
        for pid, cid_dct in p_map_dct.iteritems():
            cid_dct = pc_info_dct.get(pid, {})
            dt  = p_map_dct[pid]
            p_name, description, db_name = dt
            if pid == 'HFS':
                print 'HERE', pid
                continue 
            cid_str = ', '.join(["'"+str(e)+"'" for e in cid_dct.keys()])
            if cid_dct:
                cid_dct_qry = """ SELECT row_id, company_name, company_display_name, meta_data, user_name FROM company_mgmt WHERE row_id in (%s) """%(cid_str)
                m_cur.execute(cid_dct_qry)
                c_data = m_cur.fetchall()
            else:
                c_data=[]
            p_dct = {'project_id': pid, 'project_name': p_name, 'desc': description, 'info':[], 'db_name':db_name}
            c_dct = {}
            for rid_w in c_data:
                rid , c_n, cdn, mtd, u_n = map(str, rid_w)
                c_dct[rid] = (c_n, cdn, mtd, u_n)
        
            dc_qry = """ SELECT company_row_id, doc_id, meta_data, status, no_of_pages FROM document_mgmt WHERE project_id='%s' AND disable_flag='N' AND company_row_id in (%s) """%(pid, cid_str)
            m_cur.execute(dc_qry)
            dc_data = m_cur.fetchall()
            unikeys = {}
            d_res_dct = {}
            dc_data = list(dc_data)
            dc_data.sort(key=lambda x:int(x[1]))
            for dr in dc_data:
                crd, did, dmeta_data, sts, no_of_pages = dr 
                dmeta_data = eval(dmeta_data)
                if not no_of_pages:
                    no_of_pages = ''
                if not dmeta_data:
                    dmeta_data = {}
                tmpdd   = {'d':did, 'status':sts, 'nop':no_of_pages}
                for dk, dv in dmeta_data.iteritems():
                    if dv:
                        unikeys[dk] = dv
                    tmpdd[dk]   = dv
                d_res_dct.setdefault(crd, []).append(tmpdd)
            
            for cid, us_nm in cid_dct.iteritems():
                c_name, cd_name, mt_data, uer_nme = c_dct[cid]
                get_dc_info = d_res_dct.get(cid, []) 
                m_id_tup = pc_info_dct[pid][cid]
                m_id  = m_id_tup[1].get("model_id", pid)
                rc_id = m_id_tup[1].get("rc_id", pid)
                ul_id = cid #m_id_tup[2]
                p_dct['info'].append({'company_name':cd_name, 'company_id':c_name,'user':uer_nme, 'info':get_dc_info, 'crid':cid, 'model_id':m_id, 'rc_id':rc_id, 'rc_user':'tas', 'monitor_id':ul_id})   
                meta_data = mt_data
                if meta_data:
                    for k, v in eval(meta_data).items():
                        if k in ['company_name', 'project_id']:continue
                        p_dct['info'][-1][k]    =v
            get_p_dct_info = p_dct['info'] 
            get_p_dct_info.sort(key=lambda x:x['company_name'])
            p_dct['info'] = get_p_dct_info 
            res_lst.append(p_dct)
        #p_inf = self.read_hfs_info(m_cur, m_conn)
        #res_lst.append(p_inf)
        m_conn.close()
        return [{'message':'done', 'data':res_lst}]

    def get_uml_data(self, ijson):
        path            = '/var/www/html/DataModel/excel/%s.xlsx'%(ijson['project_id']) 
        #print path
        workbook        = xlrd.open_workbook(path, on_demand = True)
        count           = workbook.sheet_names()
        category_dic    = {}
        level_category_dic    = {}
        linking         = []
        main_link       = []
        level_d         = {}
        for index,sheet in enumerate(count):
            if index == 0:
                worksheet       = workbook.sheet_by_index(index)
                sheet_name      = sheet.strip()
                for row in range(0, worksheet.nrows):
                    find = 0
                    temp = {}
                    name = worksheet.cell_value(row,0)
                    sub_cat = worksheet.cell_value(row,1)
                    typet   = worksheet.cell_value(row,2)
                    tt      = worksheet.cell_value(row,3)
                    try:
                        tindex   = worksheet.cell_value(row,4)
                    except:
                        tindex   = None
                    
                    if not name:continue
                    if name not in category_dic:
                        category_dic[name] = []
                    if tindex:#typet.strip() == 'HEADER':
                        level   = tindex.lower().strip('l')
                        level_category_dic[name]    = level
                        level_d[int(level)]         = 1
                    category_dic[name].append({'text': '%s : %s'%(sub_cat, (typet+tt)), 'level_id':level})
            if index == 1:
                worksheet       = workbook.sheet_by_index(index)
                sheet_name      = sheet.strip()
                for row in range(0, worksheet.nrows):
                    if row == 0:continue
                    name = worksheet.cell_value(row,0)
                    sub_cat = worksheet.cell_value(row,1)
                    typet   = worksheet.cell_value(row,2)
                    linking.append({'from': name ,'to': sub_cat ,'relationship': typet})
                 
        fs  = []
        for k,v in category_dic.items():
            fs.append({'question': k ,'action': v,'key': k, 'level':level_category_dic[k]})
        levels  = level_d.keys()
        levels.sort()
        return [{'message':'done', 'data':[fs,linking], 'levels':levels}]

    def get_month_id(self):
        sql_sel = """select id, month from Months"""
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        m_cur.execute(sql_sel)
        ress_all = m_cur.fetchall()
        print ress_all
        sdicts ={}
        for data in ress_all:
            sdicts[data[1]] = data[0]
        m_conn.close()
        return sdicts
        
    def get_filng_freq_details(self, rid):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        month_id_dict = self.get_month_id()
        sql_sel_data = """select filing_type, from_month, to_month from Filing_Frequency where company_row_id = %s"""%(rid)         
        m_cur.execute(sql_sel_data)
        res_all = m_cur.fetchall()
        data_dicts ={}
        for data in res_all:
            if data[0] in data_dicts.keys():
              data_lst = data_dicts[data[0]]
              lists =[]
              lists.append(data[0])
              lists.append({'k':str(month_id_dict[data[1]]), 'n': data[1]})
              lists.append({'k':str(month_id_dict[data[2]]), 'n': data[2]})
              data_lst.append(lists)
              data_dicts[data[0]] =data_lst
            else:
               lists =[]
               lists.append(data[0])
               lists.append({'k':str(month_id_dict[data[1]]), 'n': data[1]})
               lists.append({'k':str(month_id_dict[data[2]]), 'n': data[2]})
               new_lst  =[]
               new_lst.append(lists)
               data_dicts[data[0]] = new_lst
        return data_dicts 
  
    def get_holding_and_subsidary_info(self, rid):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)         
        sql_sel = "select company_id , other_company , types from holding_and_subsidary_company where company_id = %s"%(rid)
        m_cur.execute(sql_sel)
        res_data = m_cur.fetchall()
        holding = {}
        subsidary_data =[]
        if len(res_data) > 0:
           for data in res_data:
               sql_get_comp_data = "select company_name from company_mgmt where row_id = %s"%(data[1])
               m_cur.execute(sql_get_comp_data)
               res_d = m_cur.fetchone()
               if not res_d:continue
               comp_name = res_d[0]
                
               if data[2] == 'holding':
                  holding = {'n': comp_name, 'k':data[1]}
                 
               else:
                  sdicts ={'n': comp_name, 'k': data[1]}
                  subsidary_data.append(sdicts)
        m_conn.close()
        return holding, subsidary_data

    def ticker_dispaly(self, row_id):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        sql_Sels = "select row_id, ticker from Ticker where company_id = %s"%(row_id)
        m_cur.execute(sql_Sels)
        res_data = m_cur.fetchall()
        lists =[]
        if res_data:
           for res in res_data:
               sdicts = {}
               sdicts['n'] = res[1]
               sdicts['k'] = res[0]
               lists.append(sdicts)
        m_conn.close()
        return lists
      
    def get_currency_types(self, m_conn, m_cur):
        industry_currencys = {}
        currency_lst =[]
        sql_sel_curr = "select id, currency, code from Currency"
        m_cur.execute(sql_sel_curr)
        res_curr = m_cur.fetchall()
        for cur in res_curr:
           sdicts = {}
           sdicts['k'] = str(cur[0])
           if (str(cur[1]) == ''):
               sdicts['n'] = ''
           else:
               sdicts['n'] = str(cur[1])+ '-'+str(cur[2])
           industry_currencys[int(cur[0])] =str(cur[1])+ '-'+str(cur[2])
           currency_lst.append(sdicts)
        return industry_currencys, currency_lst

    def get_accountancy_standard(self, m_conn, m_cur):
        lists_acc_std = []
        sicts_acc_std = {}
        sql_sel_acc = "select id, accounting_standard from Accounting_Standard"
        m_cur.execute(sql_sel_acc)
        res_std =  m_cur.fetchall()
        for std in res_std:
            sdicts = {}
            sdicts['k'] = str(std[0])
            sdicts['n'] = str(std[1])
            sicts_acc_std[int(std[0])] = str(std[1])
            lists_acc_std.append(sdicts)
        return sicts_acc_std , lists_acc_std

    def accountancy_display(self, row_id):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sicts_acc_std , lists_acc_std = self.get_accountancy_standard( m_conn, m_cur)
        sql_sel = "select row_id, account_st_id from company_accounting_standard where company_id = %s"%(row_id)
        m_cur.execute(sql_sel)
        res_data = m_cur.fetchall()
        cur_sel_lst =[]
        if res_data:
           for res in res_data:
              
               res_val = sicts_acc_std.get(int(res[1]))
               sdicts = {'k': res[1], 'n': res_val}
               cur_sel_lst.append(sdicts)
        return cur_sel_lst
    
    def currency_dispaly(self, row_id):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        industry_currencys, currency_lst = self.get_currency_types(m_conn, m_cur)
        print industry_currencys, '\n'
        print currency_lst
        sql_sel = "select row_id, currency_id from company_currency where company_id = %s"%(row_id)
        m_cur.execute(sql_sel)
        res_data = m_cur.fetchall()
        cur_sel_lst =[]
        if res_data:
           for res in res_data:
               res_val = industry_currencys.get(int(res[1]))
               print res_val
               sdicts = {'k': res[1], 'n': res_val}
               cur_sel_lst.append(sdicts)
        return cur_sel_lst

    def read_company_meta_data(self, ijson):
        rid = int(ijson['rid'])
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)         
        month_dicts , acc_std, filing_type, country_list, industry_types, sicts_acc_std ,sicts_countrys,sicts_industry, sicts_months, sicts_ffdict, currency_lst, industry_currencys, languages,  entity_types, entity_list = self.get_accounting_dtails(m_conn, m_cur)
        sel_stmt = """ SELECT row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time , industry , account_standard , financial_year_end, reporting_start_period, reporting_start_year, meta_data, financial_year_start, currency, sec_cik, logo, entity_type, user_relation_url, company_url FROM company_mgmt WHERE row_id ='%s' """%(rid)
        #sel_stmt = """ SELECT meta_data FROM company_mgmt WHERE row_id='%s'  """%(rid)
        m_cur.execute(sel_stmt)
        t_data = m_cur.fetchone()
        inf_dct = {}
        data_dicts ={}
        
        if t_data:
               row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time , industry , account_standard , financial_year_end, reporting_start_period, reporting_start_year, meta_data, financial_yearstart, currency, sec_cik, logo , ent_ty, user_relation_url, company_url= t_data
               ticker_data = self.ticker_dispaly(row_id)
               currency_data = self.currency_dispaly(row_id)
               account_st_data = self.accountancy_display(row_id)
               print ticker_data
               if meta_data != None:
                  meta_data = eval(meta_data)
               else:
                  meta_data ={}
               data_dicts = {'rid': str(row_id), 'company_id': company_name, 'company_name':company_display_name, 'user':user_name, 'ticker':ticker, 'country':country,  'updated_user':updated_user, 'update_time':update_time, 'industry_info':{'k':industry, 'n':sicts_industry.get(str(industry))}, 'account_standard': {'k':account_standard, 'n':sicts_acc_std.get(str(account_standard))}, 'sel_country': {'k':str(country), 'n': sicts_countrys.get(str(country))}, 'fye': {'n':financial_year_end, 'k':sicts_months.get(financial_year_end)}, 'rsp': {'n':reporting_start_period, 'k':sicts_ffdict.get(reporting_start_period)}, 'rsy': {'n':reporting_start_year, 'k':reporting_start_year}, "meta_info": meta_data, 'fys': {'n':financial_yearstart, 'k':sicts_months.get(financial_yearstart)}, 'curr': {'n':industry_currencys.get(currency), 'k': currency}, 'sec_cik': sec_cik, 'logo': logo, 'ticker_data': ticker_data, 'cur_sel_lst':  currency_data, 'accountency_std': account_st_data, 'entity_type': {'n':entity_types.get(ent_ty), 'k': ent_ty},'user_relation_url': user_relation_url,'company_url': company_url}
               inf_dct = meta_data
        m_conn.commit()
        m_conn.close()
        recent_updates = self.read_user_log(rid)
        filing_frequency = self.get_filng_freq_details(rid)
        holding_info , subsidary_info = self.get_holding_and_subsidary_info(rid)
        return [{'message':'done', 'data':inf_dct, 'recent_updates':recent_updates, 'filing_frequency': filing_frequency, "company_mgmt": data_dicts, 'honding': holding_info , 'subsidary': subsidary_info}]

    def get_accounting_dtails(self, m_conn, m_cur):
        sicts_acc_std = {}
        sicts_countrys = {}
        sicts_industry = {}
        sicts_months = {}
        sicts_ffdict ={}
        lists_month =[]
         
        sql_sel_months = "select id, month from Months"
        m_cur.execute(sql_sel_months)
        res_months = m_cur.fetchall()
        for res in res_months:
           sdicts = {}
           sdicts['k'] = str(res[0])
           sdicts['n'] = str(res[1])
           sicts_months[str(res[1])] =  str(res[0])
           lists_month.append(sdicts)

        lists_acc_std = []
        sql_sel_acc = "select id, accounting_standard from Accounting_Standard"
        m_cur.execute(sql_sel_acc)
        res_std =  m_cur.fetchall()
        for std in res_std:
            sdicts = {}
            sdicts['k'] = str(std[0])
            sdicts['n'] = str(std[1])
            sicts_acc_std[str(std[0])] = str(std[1])
            lists_acc_std.append(sdicts)

        filing_types  = []
        sql_Sel_ftyp = "select id, filing_type from Filing_Types"
        m_cur.execute(sql_Sel_ftyp)
        res_ftyp = m_cur.fetchall()
        for ftyp in res_ftyp:
            sdicts = {}
            sdicts['k'] = str(ftyp[0])
            sdicts['n'] = str(ftyp[1])
            sicts_ffdict[str(ftyp[1])] = str(ftyp[0])
            filing_types.append(sdicts)

        country_list = []
        sql_sel_country = "select id, country from country"
        m_cur.execute(sql_sel_country)
        res_cntry = m_cur.fetchall()
        for cntry in res_cntry:
            sdicts = {}
            sdicts['k'] = str(cntry[0])
            sdicts['n'] = str(cntry[1])
            sicts_countrys[str(cntry[0])] = cntry[1]
            country_list.append(sdicts)
      
        industry_types = []
        sql_sel_ind = "select ID, industryName from industrytype"
        m_cur.execute(sql_sel_ind)
        res_indu =  m_cur.fetchall()
        for indu in res_indu:
            sdicts = {}
            sdicts['k'] = str(indu[0])
            sdicts['n'] = str(indu[1])
            sicts_industry[str(indu[0])] = str(indu[1])
            industry_types.append(sdicts)
        print lists_acc_std
        
        industry_currencys = {}
        currency_lst =[]
        sql_sel_curr = "select id, currency, code from Currency"
        m_cur.execute(sql_sel_curr)
        res_curr = m_cur.fetchall()
        for cur in res_curr:
           sdicts = {}
           sdicts['k'] = str(cur[0])
           if (str(cur[1]) == ''):
               sdicts['n'] = ''
           else:
               sdicts['n'] = str(cur[1])+ '-'+str(cur[2])
           industry_currencys[cur[0]] =str(cur[1])+ '-'+str(cur[2])
           currency_lst.append(sdicts)

        languages =[]
        sql_sel_lang = "select id, language from Languages"
        m_cur.execute(sql_sel_lang)
        res_lang = m_cur.fetchall()
        for lan in res_lang:
           sdicts = {}
           sdicts['k'] = str(lan[0])
           sdicts['n'] = str(lan[1])
           languages.append(sdicts)
      
        entity_types = {}
        entity_list  = []
        sql_sel_entity = "select row_id ,ent_type from entity_type"
        m_cur.execute(sql_sel_entity)
        res_entity = m_cur.fetchall()
        for ent in res_entity:
           sdicts = {}
           sdicts['k'] = str(ent[0])
           sdicts['n'] = str(ent[1])
           entity_types[ent[0]] = ent[1]
           entity_list.append(sdicts)



        return lists_month, lists_acc_std, filing_types, country_list, industry_types, sicts_acc_std ,sicts_countrys,sicts_industry, sicts_months, sicts_ffdict, currency_lst, industry_currencys, languages , entity_types, entity_list
        #sys.exit()

    def read_company_mgmt(self, ijson):
        last_row_id = ijson.get('lrid', 0)
        limit       = ijson.get('limit', 100)
        t_cnt       = ijson.get('t_cnt', 0) + 1
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        month_dicts = []
        acc_std     = [] 
        filing_type = []
        country_list= []
        industry_types =[]
        company_lists_data = []
        #if last_row_id == 0:
        month_dicts , acc_std, filing_type, country_list, industry_types, sicts_acc_std ,sicts_countrys,sicts_industry, sicts_months, sicts_ffdict, currency_lst, industry_currencys , languages,  entity_types, entity_list= self.get_accounting_dtails(m_conn, m_cur)
        read_qry = """ SELECT row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time FROM company_mgmt WHERE row_id >'%s' LIMIT %s """%(last_row_id, limit)
        #read_qry = """ SELECT row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time FROM company_mgmt  WHERE row_id >'%s'"""%(last_row_id)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        res_lst = []
        mx_lrid = last_row_id
        mx_tcnt = t_cnt
        total_comps = 0
        for row in t_data:
            row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time = row
            if not updated_user:
                updated_user = ''
            if not update_time:
                update_time = ''
            if update_time:
                update_time = str(update_time)
            mx_lrid = max(mx_lrid, row_id)
            mx_tcnt = max(mx_tcnt, t_cnt)
            doct_comp_drp ={'n':company_name,'k':row_id}
            company_lists_data.append(doct_comp_drp)     
            data_dct = {'rid':str(row_id), 'company_id':company_name, 'company_name':company_display_name, 'user':user_name, 'ticker':ticker, 'country':country, 'sn':t_cnt, 'updated_user':updated_user, 'update_time':update_time, 'sel_country': {'k':str(country), 'n': sicts_countrys.get(str(country))}}
            res_lst.append(data_dct)
            t_cnt += 1
            total_comps += 1
        return [{'message':'done', 'data':res_lst, 'lrid':mx_lrid, 'row_cnt':total_comps,  't_cnt':mx_tcnt, 'month':month_dicts ,'acc': acc_std,'filing': filing_type,'country': country_list , 'indust': industry_types, 'currency_list': currency_lst, 'languages': languages, 'company_lst': company_lists_data, 'entity_list': entity_list}]

    def cp_update_insert_company_mgmt(self, ijson):
        save_meta    = ijson['data']
        row_id       = ijson['rid']
        user_name = ijson.get('user', "")
        company_display_name = ijson['company']
        company_display_name = company_display_name.encode('utf-8')
        ticker = ijson['ticker']
        country = ijson['country']
        company_name = ''.join(company_display_name.split())
        logo = ijson['logo']
        meta_dct = json.dumps(save_meta)
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        #meta_dct = '"' +str(meta_dct)+ '"'
        if row_id == 'new':
            insert_stmt = """ INSERT INTO company_mgmt(company_name, company_display_name, meta_data, user_name, ticker, country, logo, updated_user) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')   """%(company_name, company_display_name, meta_dct, user_name, ticker, country, logo, user_name)
            m_cur.execute(insert_stmt)
        elif row_id != 'new':
            update_stmt = """ UPDATE company_mgmt SET company_name='%s', company_display_name='%s', meta_data='%s', user_name='%s', ticker='%s', country='%s', logo='%s', updated_user='%s' WHERE row_id='%s'  """%(company_name, company_display_name, meta_dct, user_name, ticker, country, logo, user_name, row_id)
            m_cur.execute(update_stmt)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]
    
    def delete_company_from_company_mgmt(self, ijson):
        rid = int(ijson['rid'])
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        #del_stmt = """ DELETE FROM company_mgmt WHERE row_id='%s'  """%(rid)
        #m_cur.execute(del_stmt)
 
        del_acc_std = """delete from company_accounting_standard where company_id ='%s'"""%(rid)
        print del_acc_std
        m_cur.execute(del_acc_std)
        m_conn.commit()

        del_ticker ="""delete from Ticker where company_id ='%s'"""%(rid)
        print del_ticker
        m_cur.execute(del_ticker)
        m_conn.commit()

        del_curr = """delete from company_currency where company_id ='%s'"""%(rid)
        print del_curr
        m_cur.execute(del_curr) 
        m_conn.commit()

        del_hold_sub ="""delete from holding_and_subsidary_company  where company_id ='%s'"""%(rid)
        print del_hold_sub
        m_cur.execute(del_hold_sub)
        m_conn.commit()

        del_comp_web ="""delete from company_website where company_id ='%s'"""%(rid)
        print del_comp_web
        m_cur.execute(del_comp_web)
        m_conn.commit()

        del_client_det ="""delete from client_details  where company_id ='%s'"""%(rid)
        print del_client_det
        m_cur.execute(del_client_det)
        m_conn.commit()
        
        del_fil_frq = """delete from Filing_Frequency where company_row_id ='%s'"""%(rid)
        print del_fil_frq
        m_cur.execute(del_fil_frq)  
        m_conn.commit()
         
        del_stmt = """ DELETE FROM company_mgmt WHERE row_id='%s'  """%(rid)
        m_cur.execute(del_stmt)
        del_stmt = """ DELETE FROM document_master WHERE company_id='%s'  """%(rid)
        m_cur.execute(del_stmt)

  
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]
        
    def last_inserted_row_id(self, table_name, m_conn, m_cur):
        rd_qry = """ SELECT max(row_id) FROM %s  """%(table_name)
        m_cur.execute(rd_qry)
        t_data = m_cur.fetchone()
        mx = 0
        if t_data:
            mx = int(t_data[0])
        return mx
      
    def read_all_doc_ids(self , json_d):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        print json_d
        row_id = json_d["rid"]
        print "row id:::::::::::::", row_id
        sql_sel_docs =  "select doc_id, document_type, period, year, language, meta_data, user_name, date, time, link from document_master where company_id =%s"%(row_id)
        m_cur.execute(sql_sel_docs)
        ress_docs = m_cur.fetchall()
        column_defs = [{'n':'Doc_Id', 'k':'doc_id'},{'n':'Document Type', 'k':'doc_type'},{'n':'Period', 'k':'period'},{'n':'Year', 'k':'year'},{'n':'Language', 'k':'language'},{'n':'Meta Data', 'k':'meta_data'},{'n':'User Name', 'k':'user_name'},{'n':'Date', 'k':'date'},{'n':'Time', 'k':'time'}]
        all_data = []
        for doc_data in ress_docs:
            doc_id, doc_typ, period, year, lan, met_d, un, date, times, link = doc_data
            sdicts = {'doc_id': {'v':doc_id}, 'doc_typ': {'v':doc_typ}, 'period':{'v': period}, 'year': {'v':year}, 'language':{'v':lan}, 'meta_data':{'v':met_d}, 'user_name': {'v':un}, 'date':{'v':date}, 'time':{'v':times}, 'cid':1, 'link': link}
            all_data.append(sdicts)
           
        return [{'message':'done', "data":all_data, "col_defs":column_defs}]
        sys.exit()
 
    def read_user_log(self, crid):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        rd_qry = """ SELECT table_name, table_row_id, table_column, value, action, user_name, updated_time FROM user_log WHERE table_row_id='%s' ORDER BY row_id  DESC LIMIT 5 ; """%(crid)
        print "k:::::::::", rd_qry
        m_cur.execute(rd_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        res_lst = []
        for row in t_data:
            table_name, table_row_id, table_column, value, action, user_name, update_time = row 
            if update_time:
                update_time =str(update_time)
            try:
                value = json.loads(value)   
            except:s = ''
            dt_dct = {'tn':table_name, 'trid':table_row_id, 'tc':table_column, 'v':value, 'a':action, 'user':user_name, 'ut':update_time}
            res_lst.append(dt_dct)
        return res_lst

    def insert_filing_freq_data(self, filing_frequency, row_id, company_name, m_conn, m_cur):
        lists_col  = ['company_row_id','filing_type','from_month','to_month']
        table_name = "Filing_Frequency"
        if 1:
         sql_sels = "delete from Filing_Frequency where company_row_id = %s"%(row_id)
         m_cur.execute(sql_sels)
         for fil_frq_data in filing_frequency:
            st = fil_frq_data[0]
            fm = fil_frq_data[1]['n']
            to = fil_frq_data[2]['n']
            sql_insert = """insert into Filing_Frequency (company_id,company_row_id,filing_type,from_month,to_month)values('%s', '%s', '%s', '%s', '%s')"""%(company_name, row_id, st, fm, to)
            m_cur.execute(sql_insert)
         self.update_insert_user_log(m_conn, m_cur, lists_col, table_name, 1, 'update', 'demo', row_id)
        
        #m_conn.close()

    def icp_update_insert_company_mgmt(self, ijson):
        #m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        save_meta    = ijson['data']
        row_id       = ijson['rid']
        user_name = ijson.get('user', "demo")
        company_display_name = ijson['company']
        company_display_name = company_display_name.encode('utf-8')
        country = ijson['country']
        company_name = ''.join(company_display_name.split())
        meta_dct = json.dumps(save_meta)
        accounting_standard = ijson['accessors_standard']
        industry = ijson['industry']
        filing_frequency = ijson['filing_frequency']
        filing_frequency_status = ijson['filing_frequency_status']
        meta_updated_staus = ijson['meta_updated_staus']
         
        holding_company_details   = ijson['holding_details']
        subsidary_company_details = ijson['subsidary_details']
 
       #ticker_st":1,"curr_st":1,"acc_st":1,"ticker_infos":[{"k":"new","n":"11","$$hashKey":"object:2188"},{"k":"new","n":"1111","$$hashKey":"object:2190"},{"k":"new","n":"111111","$$hashKey":"object:2192"}],"currency_lists":[{"k":"164","n":"Botswana Pula-BWP","$$hashKey":"object:595"},{"k":"165","n":"Bulgarian Lev-BGN","$$hashKey":"object:596"},{"k":"166","n":"Korean Won-KRW","$$hashKey":"object:597"}],"account_standard":[{"k":"2","n":"IFRS","$$hashKey":"object:510"},{"k":"3","n":"US-GAAP","$$hashKey":"object:511"},{"k":"22","n":"US GAAP","$$hashKey":"object:512"}

        ticker_status  = ijson['ticker_st']
        account_status = ijson['acc_st']
        curr_st        = ijson['curr_st']

        ticker_vals = ijson['ticker_infos']
        currency_lists = ijson['currency_lists']
        account_standard = ijson['account_standard']

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        table_name = 'company_mgmt'
        all_data = ijson['dict_data'] 
        if len(save_meta) > 0 and meta_updated_staus == 1:
            all_data['meta_data'] = meta_dct
        i_cols = all_data.keys()
        i_cols.insert(0, 'row_id') 
        u_cols = i_cols
        if row_id == 'new':
           all_data['company_display_name'] = company_display_name
           for kk, vv in all_data.items():
               if kk == 'logo':
                  data = vv.encode('utf-8')
                  all_data['logo'] = data
           print all_data
           column_hd = ', '.join(all_data.keys())
           column_hd1 = ', '.join(['%s'] * len(all_data.keys()))
                              
           column_dt = ', '.join(all_data.values())
           column_dt1 = all_data.values()
           #column_dt = ', '.join(all_data.values())
           insert_stmt = """insert into %s (%s) values (%s)"""%(table_name, column_hd, column_hd1)
           #try:
           m_cur.execute(insert_stmt, column_dt1)
           #except:s = ''
           m_conn.commit()
           sql_row_id = "SELECT LAST_INSERT_ID()";
           m_cur.execute(sql_row_id)
           res = m_cur.fetchone()
           row_id = res[0]
 
           self.update_insert_user_log(m_conn, m_cur, i_cols, table_name, 1, 'insert', user_name, row_id)
           if len(filing_frequency)> 0 and filing_frequency_status == 1:
              self.insert_filing_freq_data(filing_frequency, row_id, company_name, m_conn, m_cur)
        else:
           for key , val in all_data.items():
               try:
                 sql_update = "update company_mgmt set "+key+"=%s where row_id = '%s'"%(val, row_id)
                 m_cur.execute(sql_update)
               except:
                 sql_update = "update company_mgmt set "+key+"='%s' where row_id = '%s'"%(val, row_id)
                 m_cur.execute(sql_update)
           m_conn.commit()
           if len(filing_frequency)> 0 and filing_frequency_status == 1:
              self.insert_filing_freq_data(filing_frequency, row_id, company_name, m_conn, m_cur)
           self.update_insert_user_log(m_conn, m_cur, u_cols, table_name, 1, 'update', user_name, row_id)
        m_conn.close()    
        self.insert_holding_subsidary_company_details(holding_company_details, subsidary_company_details, row_id, company_name)
        #if ticker_status == 1:
        self.insert_ticker_info(ticker_vals, ticker_status, row_id)
        #if account_status == 1:
        self.insert_accounting_standard(account_standard, account_status, row_id)
        #if curr_st == 1:
        self.insert_currency_info(currency_lists, curr_st, row_id)
        return [{'message':'done'}]


    def insert_ticker_info(self, ticker_vals, ticker_status, row_id):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_del = "delete from Ticker where company_id = %s"%(row_id)
        m_cur.execute(sql_del)
        m_conn.commit()
        if len(ticker_vals)>0:
          for data in  ticker_vals:
              
               sql_insert = "insert into Ticker(company_id, ticker)values(%s, '%s')"%(row_id, data['n'])
               print "sel:::::::",sql_insert
               m_cur.execute(sql_insert)
        m_conn.commit()  
        m_conn.close()

    def insert_accounting_standard(self, account_standard, account_status, row_id):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_del ="delete from company_accounting_standard where company_id = %s"%(row_id)
        m_cur.execute(sql_del)
        for data in account_standard:
            sql_insert = "insert into company_accounting_standard(company_id, account_st_id) values(%s, %s)"%(row_id, data['k'])
            m_cur.execute(sql_insert)
        m_conn.commit()
        m_conn.close()
        pass
    def insert_currency_info(self, currency_lists, curr_st, row_id):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_del ="delete from company_currency where company_id = %s"%(row_id)
        m_cur.execute(sql_del)
        for data in currency_lists:
            sql_insert = "insert into  company_currency(company_id, currency_id) values(%s, %s)"%(row_id, data['k'])
            m_cur.execute(sql_insert)
        m_conn.commit()
        m_conn.close()
        pass

           

    def insert_holding_subsidary_company_details(self, holding_company_details, subsidary_company_details, company_id, cmp_name):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        if holding_company_details != '':
           other_company_id   = holding_company_details
           sql_sel = "delete from holding_and_subsidary_company where company_id = %s and types = 'holding'"%(company_id)
           m_cur.execute(sql_sel)
           sql_insert = "insert into holding_and_subsidary_company(company_id,other_company,types) values(%s, %s, '%s')"%(company_id,other_company_id,'holding')
           print "holding:::::::", sql_insert
           m_cur.execute(sql_insert)
        if len(subsidary_company_details) > 0:
           sql_sel = "delete from holding_and_subsidary_company where company_id = %s and types = 'subsidary'"%(company_id)
           m_cur.execute(sql_sel)
           
           for data in subsidary_company_details:
               other_company_id   = data['k']
               company_name = data['n']
               sql_insert = "insert into holding_and_subsidary_company(company_id,other_company,types) values(%s, %s, '%s')"%(company_id,other_company_id,'subsidary')
               m_cur.execute(sql_insert)
           
        m_conn.commit()
        m_conn.close()

    def icp_update_insert_company_mgmt_old(self, ijson):
        print "yes::::::::::"
        save_meta    = ijson['data']
        row_id       = ijson['rid']
        user_name = ijson.get('user', "demo")
        company_display_name = ijson['company']
        company_display_name = company_display_name.encode('utf-8')
        ticker = ijson['ticker']
        country = ijson['country']
        company_name = ''.join(company_display_name.split())
        logo = ijson['logo']
        meta_updated_staus = ijson['meta_updated_staus']
        meta_dct = json.dumps(save_meta)
        accounting_standard = ijson['accessors_standard']
        industry = ijson['industry']
        financial_year_end = ijson['financial_year_end']
        reporting_start_period = ijson['reporting_start_period']
        reporting_start_year = ijson['reporting_start_year']
        filing_frequency = ijson['filing_frequency']
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        table_name = 'company_mgmt'
        #meta_dct = '"' +str(meta_dct)+ '"'
        i_cols = ['row_id', 'company_name', 'company_display_name', 'meta_data', 'ticker', 'country', 'logo', 'industry,country' ,'account_standard','financial_year_end','reporting_start_period' ,'reporting_start_year']
        u_cols = ['row_id', 'company_name', 'company_display_name', 'meta_data', 'ticker', 'country', 'logo', 'industry,country' ,'account_standard','financial_year_end','reporting_start_period' ,'reporting_start_year']
        #row_id,company_name,company_display_name,meta_data,user_name,updated_user,insert_time ,update_time ,ticker,industry,country ,account_standard,financial_year_end,reporting_start_period ,reporting_start_year
        if row_id == 'new':
            insert_stmt = """ INSERT INTO company_mgmt(company_name, company_display_name, meta_data, user_name, ticker, logo, updated_user, financial_year_end,reporting_start_period ,reporting_start_year) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')   """%(company_name, company_display_name, meta_dct, user_name, ticker,  logo, user_name, financial_year_end, reporting_start_period, reporting_start_year)
            print insert_stmt
            try:
                m_cur.execute(insert_stmt)
            except:s = ''
            m_conn.commit()
            if country != '':
                   update_stmt_count = """UPDATE company_mgmt SET country =%s WHERE row_id='%s'  """%(int(country), row_id)
                   m_cur.execute(update_stmt_count)
                   m_conn.commit()
            if industry != '':
                   update_stmt_indus = """UPDATE company_mgmt SET industry =%s WHERE row_id='%s'  """%(int(industry), row_id)
                   m_cur.execute(update_stmt_indus)
                   m_conn.commit()

            if accounting_standard != '':
                   update_stmt_acc = """UPDATE company_mgmt SET account_standard =%s WHERE row_id='%s'  """%(int(accounting_standard), row_id)
                   m_cur.execute(update_stmt_acc)
                   m_conn.commit()
            self.update_insert_user_log(m_conn, m_cur, i_cols, table_name, 1, 'insert', user_name, row_id)
        elif row_id != 'new':
               update_stmt = """ UPDATE company_mgmt SET company_name='%s', company_display_name='%s', meta_data='%s', ticker='%s', logo='%s', updated_user='%s', financial_year_end='%s',reporting_start_period ='%s',reporting_start_year='%s' WHERE row_id='%s'  """%(company_name, company_display_name, meta_dct, ticker, logo, user_name, financial_year_end, reporting_start_period, reporting_start_year, row_id)
               
               print "update info:::::::::::::", update_stmt
               m_cur.execute(update_stmt)
               m_conn.commit()
               print country, industry, accounting_standard
               if country != '':
                   update_stmt_count = """UPDATE company_mgmt SET country =%s WHERE row_id='%s'  """%(int(country), row_id)
                   print update_stmt_count
                   m_cur.execute(update_stmt_count)
                   m_conn.commit()
               if industry != '':
                   update_stmt_indus = """UPDATE company_mgmt SET industry =%s WHERE row_id='%s'  """%(int(industry), row_id)
                   m_cur.execute(update_stmt_indus)
                   m_conn.commit()

               if accounting_standard != '':
                   update_stmt_acc = """UPDATE company_mgmt SET account_standard =%s WHERE row_id='%s'  """%(int(accounting_standard), row_id)
                   m_cur.execute(update_stmt_acc)
                   m_conn.commit()

               self.update_insert_user_log(m_conn, m_cur, u_cols, table_name, 1, 'update', user_name, row_id)
        if len(filing_frequency)> 0:
           self.insert_filing_freq_data(filing_frequency, row_id, company_name, m_conn, m_cur)
        m_conn.close()
        return [{'message':'done'}]
        
    def update_insert_user_log(self, m_conn, m_cur, column_lst, table_name, limit, action, user_name, rid):
        # company_name, company_display_name, meta_data, user_name, ticker, country, logo, updated_user
        print ":UPDATE:::::::::::::::::::::::::::::"
        print m_conn, m_cur, column_lst, table_name, limit, action, user_name, rid
        column_str = ', '.join(column_lst)  
        print "column lists:::::::", column_str
        if table_name == 'company_mgmt':
          rd_qry = """ SELECT %s FROM %s ORDER BY row_id DESC LIMIT %s """%(column_str, table_name, limit)
          print rd_qry
          if rid != 'new':
            rd_qry = """ SELECT %s FROM %s WHERE row_id='%s' """%(column_str, table_name, rid) 
            print "q::::::::::", rd_qry
          m_cur.execute(rd_qry)
          t_data  = m_cur.fetchall()
        elif table_name == 'Filing_Frequency':
          rd_qry = """ SELECT %s FROM %s ORDER BY company_row_id DESC LIMIT %s """%(column_str, table_name, limit)
          print rd_qry
          if rid != 'new':
            rd_qry = """ SELECT %s FROM %s WHERE company_row_id='%s' """%(column_str, table_name, rid) 
            print "q::::::::::", rd_qry
          m_cur.execute(rd_qry)
          t_data  = m_cur.fetchall()
        else:
          rd_qry = """ SELECT %s FROM %s ORDER BY company_id DESC LIMIT %s """%(column_str, table_name, limit)
          print rd_qry
          if rid != 'new':
            rd_qry = """ SELECT %s FROM %s WHERE company_id='%s' """%(column_str, table_name, rid)
            print "q::::::::::", rd_qry
          m_cur.execute(rd_qry)
          t_data  = m_cur.fetchall()
        insert_rows = []
        dt_time = str(datetime.datetime.now())
        print dt_time
        print "sel data:::::::", t_data
        for row in t_data:
            lid = int(row[0])
            print "kkkkkkkkkkk",lid
            for ix, el in enumerate(column_lst[1:],1):
                
                tp = ((table_name, lid, el, user_name, action, row[ix], dt_time))
                insert_rows.append(tp) 
        print "K::::::::::::::",insert_rows         
        if len(insert_rows)>0:
           ins_stmt = """  INSERT INTO user_log(table_name, table_row_id, table_column, user_name, action, value, updated_time) VALUES(%s, %s, %s, %s, %s, %s, %s) """ 
           print ins_stmt, insert_rows
           m_cur.executemany(ins_stmt, insert_rows)
           m_conn.commit()
        return  

    def cp_execute_url(self, ijson):
        j_ijson = json.dumps(ijson)
        import url_execution as ue
        u_Obj = ue.Request()
        url_info = 'http://172.16.20.10:5008/tree_data?input=[%s]'%(j_ijson)
        print url_info
        txt, txt1   = u_Obj.load_url(url_info, 120)
        print 'SSSSSS', (txt, txt1)
        data = json.loads(txt1)
        return data#[{'message':'done', 'data':data}]

    def get_filing_frequency_details(self, row_id, m_conn, m_cur):
        sql_sels = "select filing_type , from_month , to_month from Filing_Frequency where company_row_id = %s"%(row_id)
        m_cur.execute(sql_sels)
        res_data = m_cur.fetchall()
        sdicts ={}
        if res_data:
           for data_sel in res_data:
               lists = []
               lists.append(str(data_sel[0]))
               lists.append(str(data_sel[1]))
               lists.append(str(data_sel[2]))
               sdicts[data_sel[0]] = lists
        return sdicts

    def generate_excel_sheet(self, ijson):
        row_id = ijson['rid']
        ##########row_id                 ,company_name           ,company_display_name   ,meta_data              ,user_name              ,updated_user           ,insert_time            ,update_time            ,ticker                 ,industry               ,country                ,account_standard       ,currency               ,financial_year_start   ,financial_year_end     ,reporting_start_period ,reporting_start_year   ,logo                   
        workbook = xlwt.Workbook()
        worksheet = workbook.add_sheet("Document_Meta_Data")
        #xlwt.add_palette_colour("custom_colour", 0x21)

        worksheet_meta = workbook.add_sheet("Company_Meta_Data")
        style = xlwt.easyxf('pattern: pattern solid, fore_colour  dark_blue; align: wrap on, vert centre, horiz center; font: colour white,bold on;')
        cell_style = xlwt.easyxf('border: top thin,bottom thin, left thin,right thin;align: vert centre, horiz right;')
        sunday_cell_style = xlwt.easyxf('border: top thin,bottom thin, left thin,right thin;')
        font_style = xlwt.easyxf('pattern: pattern solid,fore_colour blue ;font: bold on; border: top thin,bottom thin, left thin,right thin; align: vert centre,horiz right;')
        xlwt.add_palette_colour("gray25", 0x17)
        workbook.set_colour_RGB(0x17,217, 217, 237)
        row_count=0

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        header_lists = ['CompanyName', 'DealId', 'Ticker', 'Industry', 'Currency', 'Accounting Standards', 'Reporting Date From', 'Reportng Date To', 'Old Deal Id','Display Name', 'Filing Frequency']
        header_lists_docs = ['CompanyName', 'DocType', 'FiligType', 'Period', 'Financial Year', 'Document Name', 'Document Release Date','Document From (mm/dd/yyyy)', 'Document To (mm/dd/yyyy)', 'Document Download Date (mm/dd/yyyy)','Previous Release Date', 'FYE', 'Language']
        print "headers::::::::::::", header_lists
        sql_sel_data = "select company_name , company_display_name, ticker, industry, currency, account_standard, financial_year_start   ,financial_year_end from company_mgmt where row_id = '%s'"%(row_id)
        m_cur.execute(sql_sel_data)
        res_data = m_cur.fetchall()
        sdicts = self.get_filing_frequency_details(row_id, m_conn, m_cur)
        lst = sdicts.keys()
        header_lists.extend(lst)
        month_dicts , acc_std, filing_type, country_list, industry_types, sicts_acc_std ,sicts_countrys,sicts_industry, sicts_months, sicts_ffdict, currency_lst, industry_currencys, languages, entity_types, entity_list = self.get_accounting_dtails(m_conn, m_cur)        
        for r in range(0, len(header_lists_docs)):
            worksheet.write(row_count, r, header_lists_docs[r],font_style)
        for r in range(0, len(header_lists)):
            worksheet_meta.write(row_count, r, header_lists[r],font_style)
        row_count+=1
        lists_data = []
        if res_data :
           for data_comp in res_data:
               industry = sicts_industry.get(str(data_comp[3]), '')
               currency_val = industry_currencys.get(str(data_comp[4]), '') 
               acc_std = sicts_acc_std.get(str(data_comp[5]), '')
               lists_data.append(data_comp[0])
               lists_data.append('')
               lists_data.append(data_comp[2])
               lists_data.append(industry)
               lists_data.append(currency_val)
               lists_data.append(acc_std)
               lists_data.append(data_comp[6])
               lists_data.append(data_comp[7])
               lists_data.append('')
               lists_data.append(data_comp[1])
        lists_data.append(','.join(sdicts.keys()))
        for k, v in sdicts.items():
            year = v[1]+'-'+v[2]
            lists_data.append(year)
        data_col = lists_data
        for col in range(0, len(data_col)) :
           worksheet_meta.write(row_count, col, data_col[col],cell_style)
        workbook.save('ToC_DailyReport_1.xls')
        return [{'message':'done', 'file_name': 'ToC_DailyReport_1.xls'}]

    def chage_date_format(self, date):
        if date == '':
           return '0000-00-00'
        else:
           return date
           lst = date[11:].split(':')
           sec= lst[0][0:2]+':'+lst[1][0:2]+':'+lst[2][0:2]
           new_date = date[0:10]
           print 'new date::::::::::::', new_date
           return new_date

    def save_doc_details(self, ijson):
            row_id = ijson['rid']
            comp_data =  ijson['data']
            m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
             #[{u'doc_type': u'PeriodicFinancialStatement', u'finantial_year': u'2001', u'$$hashKey': u'uiGrid-00JZ', u'filing_type': u'1-E', u'language': u'Arabic', u'document_form': u'', u'browse': u'', u'fye': u'', u'document_download_date': u'', u'period': u'Q1', u'previous_release_date': u'', u'company_name': u'Amazon', u'document_to': u'', u'doc_id': u'new', u'document_name': u'Amazon_PeriodicFinancialStatement_1-E_Q1_2001.pdf', u'document_release_date': u''}]
            for data in comp_data:
                
                #comp_id = comp_data
                
                doc_type = data['doc_type']
                finantial_year = data['finantial_year']
                filing_type = data['filing_type']
                language = data['language']
                document_form = data['document_form']
                #document_form  = self.chage_date_format(document_form)
                fye = data['fye']
                #fye = self.chage_date_format(fye)
                document_download_date = data['document_download_date']
                #document_download_date = self.chage_date_format(document_download_date)
                period = data['period']
                previous_release_date = data['previous_release_date']
                #previous_release_date = self.chage_date_format(previous_release_date)
                company_name = data['company_name']
                document_to = data['document_to']
                #document_to = self.chage_date_format(document_to)
                doc_id = data['doc_id']
                document_name = data['document_name']
                document_release_date = data.get('document_release_date')
                assension_number = data.get('assension_number')
                source = data.get('source')
                sec_filing_number = data.get('sec_filing_number')
                source_type = data.get('source_type')
                link = data.get('link')

                #document_release_date = self.chage_date_format(document_release_date)
                 
                if doc_id == 'new':
                   sql_ins ="""insert into document_master(company_id, document_type, filing_type, year, language, document_name, document_release_date, document_from, document_to, document_download_date, fye, period,previous_release_date, assension_number, source, sec_filing_number, source_type, url_info)VALUES('%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s')"""%(row_id, doc_type, filing_type, finantial_year, language, document_name,document_release_date,  document_form, document_to, document_download_date, fye, period,previous_release_date, assension_number, source, sec_filing_number, source_type, link) 
                   print sql_ins
                   m_cur.execute(sql_ins)
                else:
                   sql_up = """update document_master set document_type ='%s', filing_type='%s', year=%s, language='%s', document_name='%s', document_release_date='%s', document_from='%s', document_to='%s', document_download_date='%s', fye='%s', period ='%s', previous_release_date='%s', assension_number = '%s', source='%s' , sec_filing_number ='%s', source_type='%s', url_info='%s'  where company_id = '%s' and doc_id = %s"""%(doc_type, filing_type, finantial_year, language, document_name, document_release_date, document_form, document_to, document_download_date, fye, period, previous_release_date,  assension_number, source, sec_filing_number, source_type,link, row_id, doc_id)
                   print sql_up
                   m_cur.execute(sql_up)
            res_data = self.read_doc_id_details(ijson)
            contents =[]
            max_sn = 0 
            if(res_data[0]["message"] == 'done'):
               contents = res_data[0]["data"]
               max_sn =  res_data[0]['max_sn']
            return [{'message':'done', 'data': contents, 'max_sn': max_sn}]

    def change_date_formate(self, date_val):
        if date_val == "0000-00-00" or  date_val == None or date_val=='0000-00-00 00:00:00':
           return ''
        else:
           return str(date_val)
             

    
    def read_doc_id_details_master(self, row_id):
        sdicts = {}  
        db_name = "DataBuilder_"+str(row_id) 
        try:
            conn = MySQLdb.connect("172.16.20.52", "root", "tas123", db_name)
            cursor = conn.cursor()
        except:
            return {}
        sql_sel = "select doc_id, meta_data from batch_mgmt_upload"
        cursor.execute(sql_sel)
        res_all = cursor.fetchall()
        if res_all:
         for results in res_all:
             data_val = eval(results[1])
             source_infos = data_val["SourceInfo"]
             comp_ids, doc_id = source_infos.split('_')
             sdicts[int(doc_id)] = results[0]
        return sdicts


    def read_doc_id_details(self, ijson):
        row_id = ijson['rid']
        sel_data = self.read_doc_id_details_master(row_id) 
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_sels = "select doc_id, company_id, document_type, filing_type, period, year, document_name ,document_release_date,document_from ,document_to, document_download_date, previous_release_date  ,language, fye, assension_number, source, url_info from document_master where company_id = %s"%(row_id)
        m_cur.execute(sql_sels)
        res_data = m_cur.fetchall()
        lists_data =[]
        i = 0
        input_file_path = "/var/www/html/Company_Docs/input/"+str(row_id)+"/"
        if res_data:
           for data_val in res_data:
               doc_id, company_name, doc_type, filing_type, period, finantial_year, document_name ,document_release_date, document_form ,document_to, document_download_date, previous_release_date  ,language, fye , assension_number, source , url_info= data_val
               if (document_name == None):
                 document_name = '' 
               doc_file_path = input_file_path+document_name
               sho_flg = 1
               if os.path.exists(doc_file_path):
                  sho_flg = 0

               doc_status = sel_data.get(doc_id, -1)  
               document_release_date = self.change_date_formate(document_release_date)
               document_form = self.change_date_formate(document_form)
               document_to = self.change_date_formate(document_to)
               document_download_date = self.change_date_formate(document_download_date)
               previous_release_date = self.change_date_formate(previous_release_date)
               sdicts = {'doc_id':doc_id,'company_name':company_name, 'doc_type':doc_type, 'filing_type':filing_type, 'period':period, 'finantial_year':finantial_year, 'document_name':document_name, 'document_release_date':document_release_date, 'document_form':document_form, 'document_to':document_to, 'document_download_date':document_download_date, 'previous_release_date':previous_release_date, 'fye': fye, 'language':language, 'browse':'', 'sn':i, 'sf':0,"show_flg": sho_flg, 'assension_number': assension_number,'source': source, 'link': url_info, 'upload_status': 'N' if doc_status == -1 else 'Y', 'inc_doc_id':  doc_status}
               i = i+1
               lists_data.append(sdicts)
        else:
          lists_data.append({'doc_id':'new','company_name':'', 'doc_type':'', 'filing_type':'', 'period':'', 'finantial_year':'', 'document_name':'', 'document_release_date':'', 'document_form':'', 'document_to':'', 'document_download_date':'', 'previous_release_date':'', 'fye':'', 'language':'', 'browse':'','assension_number': '','source':'', 'sn':i, 'sf':0, "show_flg": 1, 'link':'', 'inc_doc_id':'', 'upload_status': 'N'})
           
        return [{'message':'done', 'data':  lists_data, 'max_sn':i }]

    def delete_selected_doc_id(self, ijson):
       row_id = ijson['rid']
       m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
       doc_data = ijson["data"]
       input_file_path = "/var/www/html/Company_Docs/input/"+str(row_id)+"/"
       for key, vals  in doc_data.items():
           doc_id    = vals["doc_id"]
           doc_name  = vals["document_name"]
           if (doc_id == 'new'):
              continue
           sql_delete ="delete from document_master where doc_id = %s"%(doc_id)
           m_cur.execute(sql_delete)
           doc_file_path = input_file_path+doc_name
           if os.path.exists(doc_file_path):
              cmd = "rm -rf "+doc_file_path
              os.system(cmd)    
       m_conn.commit()
       m_conn.close()
       res_data = self.read_doc_id_details(ijson)
       contents =[]
       max_sn = 0
       if(res_data[0]["message"] == 'done'):
         contents = res_data[0]["data"]
         max_sn =  res_data[0]['max_sn']
       return [{'message':'done', 'data': contents, 'max_sn': max_sn}]
      
    def delete_sel_sub_data(self, ijson): 
        row_id = ijson['rid']
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sub_data = ijson["data"]
        other_doc_id = sub_data['k']
        sql_del = "delete from holding_and_subsidary_company where other_company = %s"%(other_doc_id)
        m_cur.execute(sql_del)
        m_conn.commit()
        sql_sel = "select company_id , other_company , types from holding_and_subsidary_company where company_id = %s and types='subsidary'"%(row_id)
        m_cur.execute(sql_sel)
        res_data = m_cur.fetchall()
        subsidary_data =[]
        if len(res_data) > 0:
           for data in res_data:
               sql_get_comp_data = "select company_name from company_mgmt where row_id = %s"%(data[1])
               m_cur.execute(sql_get_comp_data)
               res_d = m_cur.fetchone()
               comp_name = res_d[0]
               sdicts ={'n': comp_name, 'k': data[1]}
               subsidary_data.append(sdicts)
        m_conn.close()
        return [{'message':'done', 'data':subsidary_data}]

    """
    def delete_sel_sub_data(self, ijson): 
        row_id = ijson['rid']
        curr_id = ijson['rem_data']['k']
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_del = "delete from company_currency where company_id =%s and currency_id = %s"%(row_id, curr_id)
        cursor.execute(sql_del)
        currency_data = self.currency_dispaly(row_id)
        m_conn.close()
        return [{'message':'done', 'data':currency_data}]
    """
         
    def get_state_info(self):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sdicts = {}
        sql_sel_state = "select id, country, state from state"
        m_cur.execute(sql_sel_state)
        res_data = m_cur.fetchall()
        sdicts_state_in = {} 
        for res in res_data :
            sdicts_state_in[int(res[0])] = res[2]
            if res[1] in sdicts.keys() :
               sdicts[res[1]].append({'k': int(res[0]), 'n': res[2]})
            else:
               sdicts[res[1]] = [{'k': int(res[0]), 'n': res[2]}]
        m_conn.close()
        return sdicts, sdicts_state_in

    def get_ciuntry_info(self):
       m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
       sql_sel_country = "select id, country from country"
       m_cur.execute(sql_sel_country)
       res_cntry = m_cur.fetchall()
       sdicts_country = {}
       for cntry in res_cntry:
           sdicts_country[int(cntry[0])] = cntry[1]
       return sdicts_country


    def get_sector_details(self):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sector_types = []
        sql_sel_sector = "select ID, sector from sector"
        m_cur.execute(sql_sel_sector)
        sector = {}
        res_sector =  m_cur.fetchall()
        for indu in res_sector:
            sdicts = {}
            sdicts['k'] = str(indu[0])
            sdicts['n'] = str(indu[1])
            sector[str(indu[0])] = str(indu[1])
            sector_types.append(sdicts)
        m_conn.close()
        return sector_types, sector
      
    def get_sec_sector_details(self):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sec_sector_types = []
        sql_sel_sector = "select ID, sector from sec_sector"
        m_cur.execute(sql_sel_sector)
        sec_sector_dict = {}
        res_sector =  m_cur.fetchall()
        for indu in res_sector:
            sdicts = {}
            sdicts['k'] = str(indu[0])
            sdicts['n'] = str(indu[1])
            sec_sector_dict[str(indu[0])] = str(indu[1])
            sec_sector_types.append(sdicts)
        m_conn.close()
        return sec_sector_types, sec_sector_dict
        
    def get_sec_industry_details(self):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sec_industry_types = []
        sql_sel_sector = "select ID, industryName from sec_industry"
        m_cur.execute(sql_sel_sector)
        sec_industry_dict = {}
        res_sector =  m_cur.fetchall()
        for indu in res_sector:
            sdicts = {}
            sdicts['k'] = str(indu[0])
            sdicts['n'] = str(indu[1])
            sec_industry_dict[str(indu[0])] = str(indu[1])
            sec_industry_types.append(sdicts)
        m_conn.close()
        return sec_industry_types, sec_industry_dict

    def get_client_dict(self):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_sel_cilent_data = "select row_id, client_name from client_mgmt"
        m_cur.execute(sql_sel_cilent_data)
        res_vals = m_cur.fetchall()
        client_info = []
        if res_vals:
           for client_data in res_vals:
               sdicts ={}
               sdicts['k'] = client_data[0]
               sdicts['n'] = client_data[1]
               client_info.append(sdicts)
        m_conn.close()
        return client_info


    def default_settings(self, ijson):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        month_dicts = []
        acc_std     = [] 
        filing_type = []
        country_list= []
        industry_types =[]
        company_lists_data = []
        #if last_row_id == 0:
        month_dicts , acc_std, filing_type, country_list, industry_types, sicts_acc_std ,sicts_countrys,sicts_industry, sicts_months, sicts_ffdict, currency_lst, industry_currencys , languages,  entity_types, entity_list= self.get_accounting_dtails(m_conn, m_cur)
        sector_types, sector_dict = self.get_sector_details()
        sec_sector_types, sec_sector_dict = self.get_sec_sector_details()
        sec_industry_types, sec_industry_dict = self.get_sec_industry_details()
        state_lists, sdicts_state_in = self.get_state_info()
        client_info = self.get_client_dict()
        print state_lists
        m_conn.close()
        return [{'message':'done','month':month_dicts ,'acc': acc_std,'filing': filing_type,'country': country_list , 'indust': industry_types, 'currency_list': currency_lst, 'languages': languages, 'entity_list': entity_list, 'states': state_lists, 'sector': sector_types, 'sec_sector': sec_sector_types, 'sec_industry': sec_industry_types, 'client_info': client_info}]

    def get_conutry_details(self):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sicts_countrys = {}
        sql_sel_country = "select id, country from country"
        m_cur.execute(sql_sel_country)
        res_cntry = m_cur.fetchall()
        for cntry in res_cntry:
            sdicts = {}
            sdicts['k'] = str(cntry[0])
            sdicts['n'] = str(cntry[1])
            sicts_countrys[str(cntry[0])] = cntry[1]
        m_conn.close()
        return sicts_countrys

    def read_company_info(self, ijson):
        last_row_id = ijson.get('lrid', 0)
        limit       = ijson.get('limit', 100)
        t_cnt       = ijson.get('t_cnt', 0) + 1
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        month_dicts = []
        acc_std     = [] 
        filing_type = []
        country_list= []
        industry_types =[]
        company_lists_data = []
        #read_qry = """ SELECT row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time FROM company_mgmt  WHERE row_id >'%s'"""%(last_row_id)
        if ijson.get('search', '').lower() == 'y':
            read_qry = """ SELECT row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time FROM company_mgmt WHERE row_id >='%s' LIMIT %s """%(last_row_id, limit)
        elif ijson.get('lrids', []):
            read_qry = """ SELECT row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time FROM company_mgmt WHERE row_id >='%s' LIMIT %s """%(last_row_id, limit)
        else:
            read_qry = """ SELECT row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time FROM company_mgmt WHERE row_id >'%s' LIMIT %s """%(last_row_id, limit)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        res_lst = []
        mx_lrid = last_row_id
        mx_tcnt = t_cnt
        total_comps = 0
        sicts_countrys = self.get_conutry_details()
        for row in t_data:
            row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time = row
            if not updated_user:
                updated_user = ''
            if not update_time:
                update_time = ''
            if update_time:
               update_time = str(update_time)
            mx_lrid = max(mx_lrid, row_id)
            mx_tcnt = max(mx_tcnt, t_cnt)
            doct_comp_drp ={'n':company_name,'k':row_id}
            ticker_data = self.ticker_dispaly(row_id)
            ticker_d =''
            for d_t in ticker_data:
                ticker_d = ticker_d+'#'+d_t['n']
            company_lists_data.append(doct_comp_drp)     
            data_dct = {'rid':str(row_id), 'company_id':company_name,'k':str(row_id), 'n':company_display_name, 'user':user_name, 'ticker':ticker_d, 'country':country, 'sn':t_cnt, 'updated_user':updated_user, 'update_time':update_time, 'sel_country': {'k':str(country), 'n': sicts_countrys.get(str(country))}}
            res_lst.append(data_dct)
            t_cnt += 1
            total_comps += 1
        return [{'message':'done', 'data':res_lst, 'lrid':mx_lrid, 'row_cnt':total_comps,  't_cnt':mx_tcnt}]

    def get_client_details(self, rid) :
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)         
        sql_sel = "select client_id , client_name, project_id from client_details where company_id = %s"%(rid)
        m_cur.execute(sql_sel)
        res = m_cur.fetchall()
        lists_dict =[]
        if res:
           for data in res:
               sdicts = {}
               sdicts['id']     = data[0]
               sdicts['Name']   = data[1]
               sdicts['pid']    = data[2]
               lists_dict.append(sdicts)
        m_conn.close()
        return lists_dict

    def address_display(self, row_id):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)         
        state_lists, sdicts_state_in = self.get_state_info()
        country_dicts = self.get_ciuntry_info()
        sql_Sel_add = "select type, address, state,country ,pincode, phone, fax from company_address where company_id = %s"%(row_id)
        ress  = m_cur.execute(sql_Sel_add)
        res_add = m_cur.fetchall()
        lists_address = []
        for add in res_add:
            sdicts_add = {}
            sdicts_add['type']      = add[0]
            sdicts_add['address']   = add[1]
            if add[2] != None:
               sdicts_add['state']     = {'k':int(add[2]), 'n':sdicts_state_in.get(int(add[2]), '')}
            else:
                sdicts_add['state']     = {'k':'', 'n':''}
            if  add[3] != None:
                sdicts_add['country']   = {'k':int(add[3]), 'n':country_dicts.get(int(add[3]), '')}
            else: 
                sdicts_add['country'] = {'k':'', 'n':''}
            sdicts_add['pincode']   = add[4]
            phone_no = add[5]
            ph_lst =[]
            if phone_no !=  None and phone_no !=  '':
               for lsts in phone_no.split('#'):
                   sdicts_p = {}
                   sdicts_p['k'] = lsts
                   sdicts_p['n'] = lsts
                   ph_lst.append(sdicts_p)
            sdicts_add['phone'] = ph_lst
            sdicts_add['fax']   =  add[6] 
            lists_address.append(sdicts_add)
        m_conn.close()
        return lists_address
      
    def read_db_info(self, company_id):
        mysql_sel = mysql.connect(host="172.16.20.52", user="root", passwd = "tas123")
        mycursor  = mysql_sel.cursor()
        mycursor.execute("SHOW DATABASES")
        res_db = mycursor.fetchall()
        db_name = "DataBuilder_"+str(company_id) 
        for dbs in res_db:
           if db_name == str(dbs[0]):
              return 1
        return 0
        
    def read_client_companies(self, m_cur):
        read_qry = """ SELECT company_id FROM client_company_mgmt WHERE client_id=1; """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        client_set = {es[0] for es in t_data}
        return client_set

    def read_company_individually(self, ijson):
        rid = int(ijson['rid'])
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)         
        client_set = self.read_client_companies(m_cur)
        
        month_dicts , acc_std, filing_type, country_list, industry_types, sicts_acc_std ,sicts_countrys,sicts_industry, sicts_months, sicts_ffdict, currency_lst, industry_currencys, languages,  entity_types, entity_list = self.get_accounting_dtails(m_conn, m_cur)
        sector_types, sector_dict = self.get_sector_details()
        sec_sector_types, sec_sector_dict = self.get_sec_sector_details()
        sec_industry_types, sec_industry_dict = self.get_sec_industry_details()
        sel_stmt = """ SELECT row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time , industry , account_standard , financial_year_end, reporting_start_period, reporting_start_year, meta_data, financial_year_start, currency, sec_cik, logo, entity_type, filing_company, sector, sec_industry, sec_sector, sec_name, DB_id FROM company_mgmt WHERE row_id ='%s' """%(rid)
     
        #sel_stmt = """ SELECT meta_data FROM company_mgmt WHERE row_id='%s'  """%(rid)
        m_cur.execute(sel_stmt)
        t_data = m_cur.fetchone()
        inf_dct = {}
        data_dicts ={}
        
        client_chk_flg = 0
        
        if t_data:
               row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time , industry , account_standard , financial_year_end, reporting_start_period, reporting_start_year, meta_data, financial_yearstart, currency, sec_cik, logo , ent_ty, filing_company,  sector, sec_industry, sec_sector, sec_name, DB_id = t_data
               if not DB_id:
                   DB_id    = ''
               #print month_dicts
               #print sicts_months
               ticker_data = self.ticker_dispaly(row_id)
               currency_data = self.currency_dispaly(row_id)
               account_st_data = self.accountancy_display(row_id)
               #print ticker_data
               if row_id in client_set:
                  client_chk_flg = 1
               if meta_data != None:
                  meta_data = eval(meta_data)
               else:
                  meta_data ={}
               holding_info , subsidary_info = self.get_holding_and_subsidary_info(rid)
               company_web, user_relation_web = self.get_website_details(rid)
               client_info = self.get_client_details(rid)
               #data_dicts = {'rid': str(row_id), 'company_id': company_name, 'company_name':company_display_name, 'user':user_name,  'updated_user':updated_user, 'update_time':update_time, 'industry':{'k':industry, 'n':sicts_industry.get(str(industry))}, 'country': {'k':str(country), 'n': sicts_countrys.get(str(country))}, 'financial_year_end': {'n':financial_year_end, 'k':sicts_months.get(financial_year_end)}, 'reporting_start_period': {'n':reporting_start_period, 'k':sicts_ffdict.get(reporting_start_period)}, 'reporting_start_year': {'n':reporting_start_year, 'k':reporting_start_year}, "meta_info": meta_data, 'financial_year_start': {'n':financial_yearstart, 'k':sicts_months.get(financial_yearstart)}, 'curr': {'n':industry_currencys.get(currency), 'k': currency}, 'sec_cik': sec_cik, 'logo': logo, 'ticker': ticker_data, 'currency':  currency_data, 'accessors_standard': account_st_data, 'entity_type': {'n':entity_types.get(ent_ty), 'k': ent_ty},'company_url': company_web, 'holding_company':holding_info, 'subsidary_company':subsidary_info, 'user_relation_url':user_relation_web, 'client_info': client_info, 'filing_company': filing_company,  'sector': {'n':sector_dict.get(str(sector)),k:sector},'sec_industry': {'k':sec_industry, 'n': sec_industry_dict.get(str(sec_industry))}, 'sec_sector': {'k':sec_sector, 'n': sec_sector_dict.get(str(sec_sector))}}
               data_dicts = {'rid': str(row_id), 'company_id': company_name, 'company_name':company_display_name, 'user':user_name,  'updated_user':updated_user, 'update_time':update_time, 'industry':{'k':industry, 'n':sicts_industry.get(str(industry))}, 'country': {'k':str(country), 'n': sicts_countrys.get(str(country))}, 'financial_year_end': {'n':financial_year_end, 'k':sicts_months.get(financial_year_end)}, 'reporting_start_period': {'n':reporting_start_period, 'k':sicts_ffdict.get(reporting_start_period)}, 'reporting_start_year': {'n':reporting_start_year, 'k':reporting_start_year}, "meta_info": meta_data, 'financial_year_start': {'n':financial_yearstart, 'k':sicts_months.get(financial_yearstart)}, 'curr': {'n':industry_currencys.get(currency), 'k': currency}, 'sec_cik': sec_cik, 'logo': logo, 'ticker': ticker_data, 'currency':  currency_data, 'accessors_standard': account_st_data, 'entity_type': {'n':entity_types.get(ent_ty), 'k': ent_ty},'company_url': company_web, 'holding_company':holding_info, 'subsidary_company':subsidary_info, 'user_relation_url':user_relation_web, 'client_info': client_info, 'filing_company': filing_company, 'sector': {'n':sector_dict.get(str(sector)),'k':sector}, 'sec_industry': {'k':sec_industry, 'n': sec_industry_dict.get(str(sec_industry))}, 'sec_sector': {'k':sec_sector, 'n': sec_sector_dict.get(str(sec_sector))}, 'sec_name':sec_name, 'DB_id':DB_id}
               inf_dct = meta_data
        m_conn.commit()
        m_conn.close()
        recent_updates = self.read_user_log(rid)
        filing_frequency, s_d = self.get_filng_freq_details_new(rid)
        address_info = self.address_display(row_id)
        print "AD:::::::::::::::::::",address_info
        #holding_info , subsidary_info = self.get_holding_and_subsidary_info(rid)
        db_status = self.read_db_info(row_id)
        print "db status:::::::::::::::", db_status
        
        res = [{'message':'done', 'data':inf_dct, 'recent_updates':recent_updates, 'filing_frequency': filing_frequency, 'filing_frequency_d':s_d, "company_mgmt": data_dicts, 'address': address_info, 'db_status':db_status}]
        if DB_id:
            res[0]['DB_id'] = DB_id
        elif client_chk_flg:
            res[0]['DB_id'] = '{0}_1'.format(row_id)
        return res

    def get_website_details(self, rid):
        
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_sel = "select  row_id ,website, web_type  from company_website where company_id = %s"%(int(rid))
        m_cur.execute(sql_sel)
        res_data = m_cur.fetchall()
        company = {}
        user_web =[]
        for data in res_data:
            if data[2] =='company_website':
               #user_web.append({'k': data[0], 'n':data[1]})
               company = {'k':data[0], 'n':data[1]}      
            else:
               #company = {'k':data[0], 'n':data[1]}      
               user_web.append({'k': data[0], 'n':data[1]})
        return  company, user_web
        m_conn.commit()
        m_conn.close()

    def get_filng_freq_details_new(self, rid):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        month_id_dict = self.get_month_id()
        sql_sel_data = """select filing_type, from_month, to_month from Filing_Frequency where company_row_id = %s"""%(rid)
        m_cur.execute(sql_sel_data)
        res_all = m_cur.fetchall()
        sdicts =[]
        s_d = {}
        for data in res_all:
            sdicts.append((data[0], {'k':str(month_id_dict[data[1]]), 'n': data[1]}, {'k':str(month_id_dict[data[2]]), 'n': data[2]}))
            s_d[data[0]]    = 1
        return sdicts, s_d


    def insert_or_update_to_mgmt(self, ijson):
        #sys.exit()
        i_cols = ['row_id', 'company_name', 'company_display_name', 'meta_data', 'ticker', 'country', 'logo', 'industry,country' ,'account_standard','financial_year_end','reporting_start_period' ,'reporting_start_year']
        u_cols = ['row_id', 'company_name', 'company_display_name', 'meta_data', 'ticker', 'country', 'logo', 'industry,country' ,'account_standard','financial_year_end','reporting_start_period' ,'reporting_start_year'] 
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        row_id     = ijson['row_id']
        all_data   = ijson['cmp_mgmt']
        table_data = ijson['table_data']
        table_name = 'company_mgmt'
        user_name =   ijson.get('user', "")
        #k_id      = ijson['k_id']

        #company_name = all_data['company_name']
        #all_data['company_display_name'] = company_name
        #all_data['company_name']         = ''.join(company_name.split())
        for kk, vv in all_data.items():
            if kk == 'logo':
               data = vv.encode('utf-8')
               all_data['logo'] = data
            if kk == 'meta_data':
               all_data['meta_data'] = json.dumps(vv)
            if kk == 'company_name':
               company_name = all_data['company_name']
               all_data['company_display_name'] = company_name
               all_data['company_name']         = ''.join(company_name.split())
        #u_cols = all_data.keys()
        if row_id == 'new':
           company_display_name  = all_data['company_name']
           all_data['company_display_name'] = company_display_name
           print all_data
           column_hd = ', '.join(all_data.keys())
           column_hd1 = ', '.join(['%s'] * len(all_data.keys()))
                              
           column_dt = ', '.join(all_data.values())
           column_dt1 = all_data.values()
           #column_dt = ', '.join(all_data.values())
           print column_dt, column_dt1
           insert_stmt = """insert into %s (%s) values (%s)"""%(table_name, column_hd, column_hd1)
        
           #try:
           m_cur.execute(insert_stmt, column_dt1)
           #except:s = ''
           m_conn.commit()
           sql_row_id = "SELECT LAST_INSERT_ID()";
           m_cur.execute(sql_row_id)
           res = m_cur.fetchone()
           row_id = res[0]
 
           self.update_insert_user_log(m_conn, m_cur, i_cols, table_name, 1, 'insert', user_name, row_id)
           self.update_redis_info(company_display_name, row_id)
 
        else:
           all_data['row_id'] = row_id
           for key , val in all_data.items():
                 sql_update = "update company_mgmt set "+key+"='%s' where row_id = '%s'"%(val, row_id)
                 print "update:::::::::::",sql_update, '\n'
                 m_cur.execute(sql_update)
           m_conn.commit()
           self.update_insert_user_log(m_conn, m_cur, u_cols, table_name, 1, 'update', user_name, row_id)
        m_conn.close()   

        accountency_standard = table_data.get('accessors_standard', -1)
        if accountency_standard != -1:
           self.insert_accounting_std(accountency_standard, row_id)

        filing_frequency     = table_data.get('filing_frequency', -1)
        if filing_frequency != -1:
           self.insert_filing_ff(filing_frequency, row_id)

           #########insert filing frequency
        currency = table_data.get('currency', -1)    
        if currency != -1:
           self.insert_currency_in(currency, row_id)
            ############insert currency
        holding_company = table_data.get('holding_company', -1)
        if holding_company != -1:
           self.insert_into_holding(holding_company, row_id)
           #######insert holding company

        subsidary_company = table_data.get('subsidary_company', -1)
        if subsidary_company != -1:
           self.insert_into_subsidary_country(subsidary_company, row_id)

           #######insert subsidary comapny
        ticker = table_data.get('ticker', -1)
        if ticker != -1:
           self.insert_ticker_values(ticker, row_id)

        company_url = table_data.get('company_url', -1)
        if company_url != -1:
           self.insert_into_comapny_url(company_url, row_id)
        
        user_relation_url = table_data.get('user_relation_url', -1)
        if user_relation_url != -1:
           self.insert_into_user_rel_url(user_relation_url, row_id)

        client_info = table_data.get('id', -1)
        if client_info != -1:
           self.insert_into_client_table(client_info, row_id) 

        address_data = table_data.get('address', -1)
        if address_data != -1:
           self.insert_into_address(address_data, row_id)
        
        ############## update redis info ###################
        if 'company_name' in all_data:
            cnam = all_data['company_name']
            #print ['CCCCCCCCCCCC', cnam, row_id]
            self.update_redis_info(cnam, row_id)
        return [{'message':'done'}] 
    
    def update_redis_info(self, company_name, row_id):
        import redis_insert as ri
        r_Obj = ri.redis_insert()
        r_Obj.add_new_one(row_id, company_name)
        return
        

    def insert_accounting_std(self, account_standard, row_id):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_del ="delete from company_accounting_standard where company_id = %s"%(row_id)
        m_cur.execute(sql_del)
        for data in account_standard:
            sql_insert = "insert into company_accounting_standard(company_id, account_st_id) values(%s, %s)"%(row_id, data['k'])
            print sql_insert
            m_cur.execute(sql_insert)
        m_conn.commit()
        m_conn.close()
        pass

    def insert_currency_in(self, currency_lists, row_id):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_del ="delete from company_currency where company_id = %s"%(int(row_id))
        m_cur.execute(sql_del)
        for data in currency_lists:
            sql_insert = "insert into  company_currency(company_id, currency_id) values(%s, %s)"%(int(row_id), int(data['k']))
            m_cur.execute(sql_insert)
        m_conn.commit()
        m_conn.close()
        pass

    def insert_filing_ff(self, filing_frequency, row_id):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        lists_col  = ['company_row_id','filing_type','from_month','to_month']
        table_name = "Filing_Frequency"
        if 1:
         sql_sels = "delete from Filing_Frequency where company_row_id = %s"%(int(row_id))
         m_cur.execute(sql_sels)
         for fil_frq_data in filing_frequency:
            st = fil_frq_data[0]
            fm = fil_frq_data[1]['n']
            to = fil_frq_data[2]['n']
            sql_insert = """insert into Filing_Frequency (company_row_id,filing_type,from_month,to_month)values('%s', '%s', '%s', '%s')"""%(row_id, st, fm, to)
            m_cur.execute(sql_insert)
         self.update_insert_user_log(m_conn, m_cur, lists_col, table_name, 1, 'update', 'demo', row_id)
        m_conn.commit()
        m_conn.close()
    
    def insert_into_holding(self, data_dict, row_id):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        lists_col = ['company_id','row_id','other_company','types']
        table_name = 'holding_and_subsidary_company'
        if 1:
           other_company_id = int(data_dict['k'])
           sql_sel = "delete from holding_and_subsidary_company where company_id = %s and types = 'holding'"%(row_id)
           m_cur.execute(sql_sel)
           sql_insert = "insert into holding_and_subsidary_company(company_id,other_company,types) values(%s, %s, '%s')"%(int(row_id),other_company_id,'holding')
           m_cur.execute(sql_insert)
        self.update_insert_user_log(m_conn, m_cur, lists_col, table_name, 1, 'update', 'demo', row_id)
        m_conn.commit()
        m_conn.close()

    def insert_into_subsidary_country(self, data_dict, row_id):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        lists_col = ['company_id','row_id','other_company','types']
        table_name = 'holding_and_subsidary_company'
        if 1:
           sql_sel = "delete from holding_and_subsidary_company where company_id = %s and types = 'subsidary'"%(row_id)
           m_cur.execute(sql_sel)
           for info in data_dict:
               other_company_id = int(info['k'])
               sql_insert = "insert into holding_and_subsidary_company(company_id,other_company,types) values(%s, %s, '%s')"%(int(row_id),other_company_id,'subsidary')
               m_cur.execute(sql_insert)
        self.update_insert_user_log(m_conn, m_cur, lists_col, table_name, 1, 'update', 'demo', row_id)
        m_conn.commit()
        m_conn.close()
           
    def insert_into_comapny_url(self, company_url, row_id):
        lists_col = ['company_id','row_id','website', 'web_type'] 
        table_name = 'company_website'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_sel = "delete from company_website where company_id = %s and web_type = 'company_website'"%(int(row_id))
        print sql_sel
        m_cur.execute(sql_sel)
        url = company_url['n']
        sql_insert = """insert into company_website(company_id, website, web_type) values(%s, '%s', '%s')"""%(int(row_id), url, 'company_website')
        m_cur.execute(sql_insert)
        self.update_insert_user_log(m_conn, m_cur, lists_col, table_name, 1, 'update', 'demo', row_id)
        m_conn.commit()
        m_conn.close()

    def insert_into_user_rel_url(self, company_url, row_id):
        lists_col = ['company_id','row_id','website', 'web_type']
        table_name = 'company_website'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_sel = "delete from company_website where company_id = %s and web_type = 'user_relation_website'"%(row_id)
        m_cur.execute(sql_sel)
        
        for urls in company_url:
           url = urls['n']
           sql_insert = """insert into company_website(company_id, website, web_type) values(%s, '%s', '%s')"""%(int(row_id), url, 'user_relation_website')
           m_cur.execute(sql_insert)
        self.update_insert_user_log(m_conn, m_cur, lists_col, table_name, 1, 'update', 'demo', row_id)
        m_conn.commit()
        m_conn.close()

    def insert_ticker_values(self, ticker_vals,  row_id):
        lists_col =['company_id','row_id','ticker']
        table_name = 'Ticker'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_del = "delete from Ticker where company_id = %s"%(row_id)
        m_cur.execute(sql_del)
        m_conn.commit()
        if len(ticker_vals)>0:
          for data in  ticker_vals:
               sql_insert = "insert into Ticker(company_id, ticker)values(%s, '%s')"%(row_id, data['n'])
               m_cur.execute(sql_insert)
        self.update_insert_user_log(m_conn, m_cur, lists_col, table_name, 1, 'update', 'demo', row_id)
        m_conn.commit()
        m_conn.close()

    def insert_into_client_table(self, client_detilas, row_id):
        lists_col =['company_id','row_id','client_id', 'client_name']
        table_name = 'client_details'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_del = "delete from client_details  where company_id = %s"%(row_id)
        m_cur.execute(sql_del)
        m_conn.commit()
        for client_det in client_detilas:
            client_id   =   client_det['id']
            client_name =   client_det['Name']
            project_id =   client_det['pid']
            sql_insert = """insert into client_details(company_id, client_id, client_name, project_id)values(%s, '%s', '%s', '%s')"""%(row_id, client_id, client_name, project_id)
            m_cur.execute(sql_insert)
        self.update_insert_user_log(m_conn, m_cur, lists_col, table_name, 1, 'update', 'demo', row_id)
        m_conn.commit()
        m_conn.close()
         
    def insert_into_address(self, address_data, row_id): 
        lists_col =['company_id','row_id', 'type','address','state','country','pincode','phone','fax']
        table_name = 'company_address'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql_del = "delete from company_address  where company_id = %s"%(row_id)
        m_cur.execute(sql_del)
        m_conn.commit()
        for add_info in address_data:
            type_d  =  add_info.get('type', '').get('v', '').get('n', '')
            address =  add_info['address']['v'].encode('utf-8')
            country =  add_info.get('country', '').get('v', '').get('k', 0)
            state   =  add_info.get('state', '').get('v', '').get('k', 0)
            pincode =  add_info['pincode']['v']
            phone_no = add_info.get('phone', '').get('val', [])
            phone_no_str = ''
            phone_no_lst =[]
            if len(phone_no)> 0:
               for data_p in phone_no:
                   phone_no_lst.append(data_p['n'])
            fax  = add_info['fax']['v']
            
            sql_insert = """insert into company_address(company_id, type, address, country, state, pincode, phone, fax) values(%s, '%s', '%s', %s, %s, '%s', '%s', '%s')"""%(row_id, type_d, address, country, state, pincode, '#'.join(phone_no_lst), fax)
            print "insert:::::::::::::::", sql_insert
            m_cur.execute(sql_insert)
        self.update_insert_user_log(m_conn, m_cur, lists_col, table_name, 1, 'update', 'demo', row_id)
        m_conn.commit()
        m_conn.close()

    def select_company_info_with_respect_to_client_id(self, ijson):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        client_id = ','.join(ijson['client_id'])
        sql_sels = "select distinct(company_id) from client_company_mgmt where client_id in (%s)"%(client_id)
        print sql_sels
        
        m_cur.execute(sql_sels)
        res_dict = m_cur.fetchall()
        lists_d = []
        if res_dict:
            for res_d in res_dict:
                lists_d.append(int(res_d[0]))
        str_data = ','.join([str(i) for i in lists_d]) 
        month_dicts = []
        acc_std     = [] 
        filing_type = []
        country_list= []
        industry_types =[]
        company_lists_data = []
        read_qry = """ SELECT row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time FROM company_mgmt  WHERE row_id  in (%s)"""%(str_data)
        print 'RRRRRRR', read_qry
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        res_lst = []
        t_cnt = 1 
        total_comps = 0
        sicts_countrys = self.get_conutry_details()
        for row in t_data:
            row_id, company_name, company_display_name, user_name, ticker, country, updated_user, update_time = row
            if not updated_user:
                updated_user = ''
            if not update_time:
                update_time = ''
            if update_time:
               update_time = str(update_time)
            doct_comp_drp ={'n':company_name,'k':row_id}
            ticker_data = self.ticker_dispaly(row_id)
            ticker_d =''
            for d_t in ticker_data:
                ticker_d = ticker_d+'#'+d_t['n']
            company_lists_data.append(doct_comp_drp)     

            data_dct = {'rid':str(row_id), 'company_id':company_name,'k':str(row_id), 'n':company_display_name, 'user':user_name, 'ticker':ticker_d, 'country':country, 'sn':t_cnt, 'updated_user':updated_user, 'update_time':update_time, 'sel_country': {'k':str(country), 'n': sicts_countrys.get(str(country))}}
            res_lst.append(data_dct)
            t_cnt += 1
            total_comps += 1
        m_conn.close()
        #return [{'message':'done', 'data':res_lst, 'lrid':mx_lrid, 'row_cnt':total_comps,  't_cnt':mx_tcnt}]
        return [{'message':'done', 'data':res_lst}]
        

    def get_sts_info(self, ijson):
        company_id = ijson['company_id']
        conn     = httplib.HTTPConnection("172.16.20.229:7654")
        fs_in    = {"cmd_id":232,"company_id": company_id}
        fs_in    = "full_data="+json.dumps(fs_in)
        #print fs_in
        headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
        conn.request("POST","/post_method", fs_in, headers)
        response = conn.getresponse()
        data = json.loads(response.read())
        conn.close()
        return data
        
    def read_DB_status(self, ijson):
        sys.path.insert(0, '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/')
        import inc_api as inc
        inc_obj = inc.INCAPI()
        res = inc_obj.api_for_data_builder_confirmation(ijson)
        return res 
     
    def create_database(self, ijson):
        res = create_db.run(ijson)
        if res == 'done':
           return [{'message':'Done'}]
        return [{'message':'Error'}]

    def upload_sel_docs(self, ijson):
        doc_id_lists = ijson['doc_id_list']
        company_id = int(ijson['row_id'])
        user_id = ijson['user_id']
        doc_id_lst = '#'.join([str(i) for i in doc_id_lists])
        res_d = html_conn.get_doc_res(company_id, doc_id_lst)
        inc_doc_id = []
        if res_d == []:
            excel_paths = "/var/www/html/WorkSpaceBuilder_DB/"+str(company_id)+'/1/upload/'+str(company_id)+'.xlsx'
            db_name = 'DataBuilder_'+str(company_id)
            #{"oper_flag":97030,"excel_name":"/var/www/html/WorkSpaceBuilder_DB/1406/1/upload/1406.xlsx","stage_lst":"1~6~7","project_id":1406,"db_name":"DataBuilder_1406","ws_id":1,"meta_data":{},"doc_type":"HTML2PDF","user_id":"demo_user1"} 
            ijson = {"oper_flag":97030,"excel_name":excel_paths, "stage_lst":"1~6~7","project_id":company_id, "db_name":db_name, "ws_id":1,"meta_data":{},"doc_type":"HTML2PDF","user_id":user_id}
            res_p = html_conn.execute_url(ijson)
            if res_p:
               doc_id_dict = self.read_doc_id_details_master(company_id)
               for doc_i in doc_id_lists:
                   doc = doc_id_dict.get(doc_i, -1)
                   if doc != -1:
                      inc_doc_id.append(str(doc))
               if len(inc_doc_id) >0:
                  doc_id_inc = '~'.join(inc_doc_id)
                  p_json = {"oper_flag":97026,"doc_lst": doc_id_inc,"stage_lst":"1~6~7","project_id":company_id,"db_name":db_name,"ws_id":1,"doc_type":"HTML2PDF","p_type":"r","meta_data":{},"lang":"en","ocr":"N","pdftype":"1","selected_pages":"0","ocr_chk":"N","lc":"1","pd":"0","pp":"99997","user_id": user_id}
                  res_pjson = 'DONE' #html_conn.execute_url(p_json)
                  if res_pjson:
                     return [{'message':'Done'}]
        return [{'message':'Error'}]

        #{"oper_flag":97030,"excel_name":"/var/www/html/WorkSpaceBuilder_DB/1406/1/upload/1406.xlsx","stage_lst":"1~6~7","project_id":1406,"db_name":"DataBuilder_1406","ws_id":1,"meta_data":{},"doc_type":"HTML2PDF","user_id":"demo_user1"}
    def create_and_download_excel(self, ijson):
        doc_id_lists = ijson['doc_id_list']
        company_id = ijson['row_id']
        user_id = ijson['user_id']
        doc_id_lst = '#'.join([str(i) for i in doc_id_lists])
        res_d = html_conn.get_doc_res(company_id, doc_id_lst)
        if res_d == []:
            excel_paths = "/var/www/html/WorkSpaceBuilder_DB/"+str(company_id)+'/1/upload/'+str(company_id)+'.xlsx'
            if os.path.exists(excel_paths):
               return [{'message':'Done'}]
        return [{'message':'Error'}]

    def reporocess_document(self, ijson):
        doc_id_lists = ijson['doc_id_list']
        company_id = ijson['row_id']
        user_id = ijson['user_id']
        doc_id_lst = '~'.join([str(i) for i in doc_id_lists])
        db_name = 'DataBuilder_'+str(company_id)
            #{"oper_flag":97030,"excel_name":"/var/www/html/WorkSpaceBuilder_DB/1406/1/upload/1406.xlsx","stage_lst":"1~6~7","project_id":1406,"db_name":"DataBuilder_1406","ws_id":1,"meta_data":{},"doc_type":"HTML2PDF","user_id":"demo_user1"} 
        ijson = {"oper_flag":97026,"doc_lst": doc_id_lst ,"stage_lst":"1~6~7","project_id": company_id,"db_name":db_name,"ws_id":1,"doc_type":"HTML2PDF","p_type":"r","meta_data":{},"lang":"en","ocr":"N","pdftype":"1","selected_pages":"0","ocr_chk":"N","lc":"1","pd":"0","pp":"99997","user_id":user_id}
       # {"oper_flag":97026,"doc_lst":"34","stage_lst":"1~6~7","project_id":1472,"db_name":"DataBuilder_1472","ws_id":1,"doc_type":"HTML2PDF","p_type":"r","meta_data":{},"lang":"en","ocr":"N","pdftype":"1","selected_pages":"0","ocr_chk":"N","lc":"1","pd":"0","pp":"99997","user_id":"sunil"}
        print "ijson:::::::::::", ijson
        res_p = html_conn.execute_url(ijson)
        if res_p:
               return [{'message':'Done'}]
        return [{'message':'Error'}]

    def create_company_details_current_dir(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        
        import SEC_INFO.update_SEC_acc_no as update_SEC_acc_no
        obj = update_SEC_acc_no.Extract()
        res = obj.validate_docs([str(company_id)], project_id)        
        if res and res[0]['message'] != 'done':
            return res
        os.chdir('/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc')
        #sys.path.insert(0, '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/')  
        #import final_op_txt as fot
        #f_Obj = fot.Generate_Project_Txt()
        #res = f_Obj.generate_all_txt_files(company_id, project_id, ijson)
        #return res
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql = "select client_id from client_details where project_id=%s and company_id=%s"%(project_id, company_id)
        m_cur.execute(sql)
        res = m_cur.fetchone()
        cmp_name    = res[0]
        m_conn.close()
        from datetime import tzinfo, timedelta, datetime, date
        datetime_val = datetime.now()
        datetime = datetime.strptime(str(datetime_val).split('.')[0], "%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%dT%H:%M:%S')
        f_path = '/var/www/html/KBRA_output/{0}/{1}/{2}_{3}.zip'.format(project_id, company_id, cmp_name, datetime) #'/var/www/html/prashant/{0}.zip'.format(company_id)
        os.system('rm -rf "%s"'%(f_path))
        cmd = 'python final_op_txt.py %s %s %s'%(company_id, project_id, datetime) 
        print cmd
        #process = subprocess.Popen(cmd, stderr=subprocess.PIPE, shell=True)
        #error = process.communicate()
        #print error
        #print [f_path, os.path.exists(f_path)]
        disableprint() 
        import gfiles.final_op_txt as f_o
        r_Obj = f_o.Generate_Project_Txt()
        r_Obj.generate_all_txt_files(company_id, project_id, datetime)

         

        df_path = '/var_www_html/KBRA_output/{0}/{1}/{2}_{3}.zip'.format(project_id, company_id, cmp_name, datetime) #'/var/www/html/prashant/{0}.zip'.format(company_id)
        if os.path.exists(f_path):
            res = [{'message':'done', 'path':df_path}]
        else:
            res = [{'message':'Error '}]
        return res 
        
    def create_company_details(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        
        import SEC_INFO.update_SEC_acc_no as update_SEC_acc_no
        obj = update_SEC_acc_no.Extract()
        res = obj.validate_docs([str(company_id)], project_id)        
        if res and res[0]['message'] != 'done':
            return res
        os.chdir('/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc')
        #sys.path.insert(0, '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/')  
        #import final_op_txt as fot
        #f_Obj = fot.Generate_Project_Txt()
        #res = f_Obj.generate_all_txt_files(company_id, project_id, ijson)
        #return res
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql = "select client_id from client_details where project_id=%s and company_id=%s"%(project_id, company_id)
        m_cur.execute(sql)
        res = m_cur.fetchone()
        cmp_name    = res[0]
        m_conn.close()
        from datetime import tzinfo, timedelta, datetime, date
        datetime_val = datetime.now()
        datetime = datetime.strptime(str(datetime_val).split('.')[0], "%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%dT%H:%M:%S')
        f_path = '/var/www/html/KBRA_output/{0}/{1}/{2}_{3}.zip'.format(project_id, company_id, cmp_name, datetime) #'/var/www/html/prashant/{0}.zip'.format(company_id)
        os.system('rm -rf "%s"'%(f_path))
        cmd = 'python final_op_txt.py %s %s %s'%(company_id, project_id, datetime) 
        print cmd
        process = subprocess.Popen(cmd, stderr=subprocess.PIPE, shell=True)
        error = process.communicate()
        print error
        print [f_path, os.path.exists(f_path)]
        df_path = '/var_www_html/KBRA_output/{0}/{1}/{2}_{3}.zip'.format(project_id, company_id, cmp_name, datetime) #'/var/www/html/prashant/{0}.zip'.format(company_id)
        if os.path.exists(f_path):
            res = [{'message':'done', 'path':df_path}]
        else:
            res = [{'message':'Error '}]
        return res 

    def update_filing_freq_cmp(self, cmp_id):
        conn, cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql     = "select period, year, document_from, document_to from document_master where company_id=%s"%(cmp_id)
        cur.execute(sql)
        res     = cur.fetchall ()
        i_ar    = []
        f_d = {}
        for r in res:
            period, year, document_from, document_to    = r
            if period  and document_from and document_to:
                if period not in f_d:
                    f_d[period] = 1
                    i_ar.append((cmp_id, period, document_from.strftime('%B'), document_to.strftime('%B')))
        #conn, cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        #sql = "delete from Filing_Frequency where company_row_id=%s"%(cmp_id)
        #cur.execute(sql)
        print 'INSERT ', [cmp_id, len(i_ar)]
        cur.executemany("insert into Filing_Frequency(company_row_id, filing_type, from_month,  to_month) values(%s, %s, %s, %s)", i_ar)
        conn.commit()
        conn.close()
        
        
                

    def update_filing_freq(self):
        conn, cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo)
        sql = "select company_id from client_details where project_id=3"
        cur.execute(sql)
        res = cur.fetchall()
        sql = "select distinct(company_row_id) from Filing_Frequency"
        cur.execute(sql)
        tmpres = cur.fetchall()
        conn.close()
        f_d = {}
        for r in tmpres:
            f_d[str(r[0])]  = 1
        for i, r in enumerate(res):
            print 'Running ', i, '/', len(res), [r]
            if str(r[0]) in f_d:continue
            try:
                self.update_filing_freq_cmp(r[0])
            except:
                print 'Error ', [r]
                PrintException()
            #break

    def update_tas_sec_acc_no(self, ijson):
        import SEC_INFO.update_SEC_acc_no as update_SEC_acc_no
        obj = update_SEC_acc_no.Extract()
        return obj.ft_read_batch_cik_doc_info(map(lambda x:str(x), ijson['cids']))        

    def validate_builder_docs(self, ijson):
        import SEC_INFO.update_SEC_acc_no as update_SEC_acc_no
        obj = update_SEC_acc_no.Extract()
        return obj.validate_docs(map(lambda x:str(x), ijson['cids']))        

    def download_sec_docs(self, ijson):
        import SEC_INFO.company_wise_extract_access_no as update_SEC_docs
        obj = update_SEC_docs.Extract()
        return obj.update_company_plus_doc_meta_data_changed(map(lambda x:str(x), ijson['cids']))        

if __name__ == '__main__':
    p_Obj = PYAPI()
    p_Obj.update_filing_freq()
    #p_Obj.create_company_details({'company_id':'', 'project_id':''})
    #for iD in range(2123, 2151):
    #    p_Obj.delete_company_from_company_mgmt({'rid':iD, 'project_id':''})

