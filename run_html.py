import json, os, xlsxwriter
import compute_period_and_date
import url_execution as ue

compute_period_and_date_obj = compute_period_and_date.PH()

class run_html:
    def __init__(self):
        self.storage_ip = "172.16.20.229##root##tas123"
        self.cmp_table  = "project_company_mgmt_db_test"     
        
    def mysql_connection(self, db_data_lst):
        import MySQLdb
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur
        
    def read_doc_ids(self, company_id): 
        ip, host_info, password = self.storage_ip.split('##')
        db_data_lst = [ip, host_info, password, self.cmp_table] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT cm.company_name, dm.doc_id, dm.company_id , dm.document_type , dm.filing_type , dm.period, dm.year, dm.document_name, dm.document_release_date, dm.document_from, dm.document_to, dm.document_download_date, dm.previous_release_date, dm.language, dm.meta_data, dm.user_name, dm.date, dm.time, dm.assension_number, dm.source, dm.next_release_date, dm.other_info , dm.url_info, dm.sec_filing_number , dm.source_type, dm.fye, dm.document_name FROM document_master as dm INNER JOIN company_mgmt as cm on dm.company_id=cm.row_id WHERE dm.company_id='%s'; """%(company_id)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        return t_data
        
    
    def create_xl(self, company_id, doc_data_lst):
        print "create excel:::::::::::"
        print doc_data_lst
        #header_lst = ['UrlName', 'CompanyName', 'DocType', 'FilingType', 'Period', 'Financial Year', 'Document Release Date', 'Document From', 'Document To', 'Document Download Date', 'PreviousReleaseDate', 'NextReleaseDate', 'FYE', 'SourceInfo']
        
        header_lst = ['UrlName', 'CompanyName', 'DocType', 'FilingType', 'Period', 'Financial Year', 'DocumentName', 'Document Release Date', 'Document From (mm/dd/yyyy)', 'Document To (mm/dd/yyyy)', 'Document Download Date (mm/dd/yyyy)', 'PreviousReleaseDate', 'NextReleaseDate', 'FYE', 'Language', 'Currency', 'Scale', 'SEC Accession No', 'SourceInfo', 'Old_Deal_Id']
        
        xl_path = '/root/databuilder_train_ui/tenkTraining/Company_Management/pysrc/'
        xl_path = os.path.join(xl_path, 'company_excels')
        if not os.path.exists(xl_path):
            os.system('mkdir -p %s'%(xl_path))
        xl_file_path = os.path.join(xl_path, '%s.xlsx'%(company_id))
        workbook = xlsxwriter.Workbook(xl_file_path)
        worksheet = workbook.add_worksheet('Document_Meta_Data')
        cell_format = workbook.add_format({'bg_color': '#B0E0E6'})
        gv_format = workbook.add_format({'align':'right'})
        for col_idx, hdr in enumerate(header_lst):
            worksheet.write_string(0, col_idx, hdr, cell_format) 
        
        for row_ix, row_tup in enumerate(doc_data_lst, 1):
            print row_ix, row_tup
            for col_id, col_el in enumerate(row_tup):
                if col_el is None:continue
                worksheet.write(row_ix, col_id, col_el, gv_format)
        workbook.close()    
        if 1:
            d_path = '/var/www/html/WorkSpaceBuilder_DB/%s/1/upload/'%(company_id)
            if not os.path.exists(d_path):
                os.system('mkdir -p %s'%(d_path))
            excel_paths = d_path+'%s.xlsx'%(company_id)
            if os.path.exists(excel_paths):
               cmd_rm = "rm -rf "+excel_paths
               os.system(cmd_rm)
            cp_cmd = 'cp -r %s %s'%(xl_file_path, d_path)
            print cp_cmd
            os.system(cp_cmd)
        #except:pass
        return 1
    
    def dt_format(self, data_data):
        data_data = str(data_data)
        #print 'DDDDD', data_data
        yy, mm, dd = data_data.split('-')        
        r_inf = '/'.join((mm, dd, yy))
        return r_inf

    def get_doc_res(self, company_id, db_obj= ''):
        doc_lst = []
        if db_obj:
            doc_lst = db_obj.split('#')
    
        FYE_num = 12
        filing_map  = {
                        
                        }
        docs_res = self.read_doc_ids(company_id)
        print "docs_res::::::::::", docs_res
        # UrlName   CompanyName DocType FilingType  Period  Financial Year  Document Release Date   Document From   Document To Document Download Date  PreviousReleaseDate NextReleaseDate FYE

        # CompanyName   DocType FilingType  Period  Financial Year  DocumentName    Document Release Date   Document From (mm/dd/yyyy)  Document To (mm/dd/yyyy)    Document Download Date (mm/dd/yyyy) PreviousReleaseDate NextReleaseDate FYE Language    Currency    Scale   SEC Accession No
        doc_data_lst = []
        for row in docs_res:
            company_name, doc_id, company_id ,document_type ,filing_type , period, year, document_name, document_release_date, document_from, document_to, document_download_date, previous_release_date, language, meta_data, user_name, date, time, assension_number, source, next_release_date, other_info ,url_info, sec_filing_number ,source_type, FYE_num, document_name = row
            fye = FYE_num
            document_type = ''.join(document_type.split())
            if doc_lst:
                print "doc_id:::::::", doc_id
                print doc_lst
                if str(doc_id) not in doc_lst:continue
            if not document_to:
                ptype, year = '', '' 
            else:
                if not doc_lst and int(document_to.strftime('%Y')) <2009:continue
                print [company_id, document_to, FYE_num, filing_type, sec_filing_number, assension_number]
                ptype, ylar = compute_period_and_date_obj.get_ph_from_date(document_to, FYE_num, filing_type)
                print 'PHS ', [ptype, ylar]
                if  not doc_lst and (ylar and int(ylar) < 2009):continue
            e_info = '%s_%s'%(company_id, doc_id)

            try:
                document_release_date = self.dt_format(document_release_date)
            except:pass 
            try:
                document_to = self.dt_format(document_to) 
            except:pass
            document_release_date = str(document_release_date)
            document_to = str(document_to)
            d_name = '_'.join(map(str, (company_name, document_type, filing_type, ptype, ylar, '.html')))
            #print [ylar]
            doc_tup = (url_info, company_name, document_type, filing_type, ptype, ylar, d_name, str(document_release_date), document_from, str(document_to), document_download_date, previous_release_date, next_release_date, fye, 'English', '', '', assension_number, e_info)
            doc_data_lst.append(doc_tup)
        res_exc = self.create_xl(company_id, doc_data_lst) 
        if res_exc:
           return []
        return False

    def execute_url(self, ijson):
        j_ijson = json.dumps(ijson)
        u_Obj = ue.Request()
        url_info = 'http://172.16.20.52:5010/tree_data?input=[%s]'%(j_ijson)
        print url_info
        txt, txt1   = u_Obj.load_url(url_info, 120)
        data = json.loads(txt1)
        print ['FFFFFFFFFFFFFFFFFFFFF', data]
        return data#[{'message':'done', 'data':data}]
        
    def run(self, ijson):
        filter_ids = ijson.get('row_ids', [])
        import db_api
        db_obj = db_api.db_api(self.storage_ip+"##"+self.cmp_table)
        client_id = 1
        cmp_info = db_obj.read_company_ids(client_id)
        cmp_id_str = []
        for each in cmp_info:
            row_id , company_id = each
            cmp_id_str.append(str(int(company_id))) 
        cmp_id_str1 = ','.join(cmp_id_str)
        all_cmp_info = db_obj.read_meta_info(cmp_id_str1)
        for each_cmp in all_cmp_info:
            row_id, company_name, company_display_name, meta_data  = each_cmp       
            if row_id not in filter_ids:continue
            meta_data = eval(meta_data)
            row_id = int(row_id)
            ijson_1 = {"user_id":"sunil", "ProjectName": company_display_name, "oper_flag":622, "ProjectID":row_id}
            #{"oper_flag":97030,"excel_name":"/var/www/html/WorkSpaceBuilder_DB/1406/1/upload/1406.xlsx","stage_lst":"1~6~7","project_id":1406,"db_name":"DataBuilder_1406","ws_id":1,"meta_data":{},"doc_type":"HTML2PDF","user_id":"demo_user1"} 
            
            #ijson_2 = {"user_id":"sunil", "ProjectID":row_id, "WSName":company_display_name ,"db_name":"DataBuilder_%s"%(row_id),"oper_flag":90014}
            #print ijson_1
            #print ijson_2
            #res1 = self.execute_url(ijson_1)
            #res2 = self.execute_url(ijson_2)
        db_obj.con.close()
        return 'done'

if __name__ == '__main__':
    obj = run_html()
    ijson = {"row_ids": []}
    for rowid in [1472]:#1403, 1407, 1408, 1409, 1410, 1411, 1412]:
        docs_res = obj.get_doc_res(str(rowid))  
    #res = obj.run(ijson)
    #docs_res = obj.get_doc_res("1405")  
    #print docs_res
