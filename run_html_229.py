import json, os, subprocess, sys
import compute_period_and_date
compute_period_and_date_obj = compute_period_and_date.PH()
import url_execution as ue

class run_html:
    def __init__(self):
        self.storage_ip = "172.16.20.229##root##tas123"
        self.cmp_table  = "project_company_mgmt_db_test"     

    def get_doc_res(self, row_id, db_obj= ''):
        if not db_obj:
            import db_api
            db_obj = db_api.db_api(self.storage_ip+"##"+self.cmp_table)
        docs_res = db_obj.read_doc_meta_info(row_id)  
        FYE_num = 12
        filing_map  = {
                        
                        }
        for row in docs_res:
            doc_id, company_id ,document_type ,filing_type , period, year, document_name, document_release_date, document_from, document_to, document_download_date, previous_release_date, language, meta_data, user_name, date, time, assension_number, source, next_release_date, other_info ,url_info, sec_filing_number ,source_type = row
            if not document_to:
                year, ptype = '', ''
            else:
                year, ptype = compute_period_and_date_obj.get_ph_from_date(document_to, FYE_num, filing_type)
            #print [doc_id, document_type, filing_type, document_release_date, year, ptype, document_to.strftime('%d-%b-%Y')]
        return []
        
    def execute_url(self, ijson):
        #print "execute urls:::::::::::::", ijson
        j_ijson = json.dumps(ijson)
        u_Obj = ue.Request()
        url_info = 'http://172.16.20.52:5010/tree_data?input=[%s]'%(j_ijson)
        #print url_info
       
        txt, txt1   = u_Obj.load_url(url_info, 120)
        #print txt1
        data = json.loads(txt1)
        return data#[{'message':'done', 'data':data}]

    def run(self, ijson):
        filter_ids = ijson.get('row_ids', [])
        import db_api
        db_obj = db_api.db_api(self.storage_ip+"##"+self.cmp_table)
        client_id = None
        cmp_info = db_obj.read_company_ids(client_id)
        cmp_id_str = []
        for each in cmp_info:
            row_id , company_id = each
            print row_id , company_id  
            print type(row_id), int(row_id)
            print "filter_ids::::::::", filter_ids
            if int(row_id) not in filter_ids:continue
            cmp_id_str.append(str(int(company_id))) 
        cmp_id_str1 = ','.join(cmp_id_str)
        all_cmp_info = db_obj.read_meta_info(', '.join(map(lambda x:str(x), filter_ids)))
        for each_cmp in all_cmp_info:
            print('D::::::::::::::', each_cmp)
            row_id, company_name, company_display_name, meta_data  = each_cmp       
            print "1::::::::::::::::::::::::", row_id, company_name, company_display_name, meta_data
            print "filter ids:::::::::::::", filter_ids
            if row_id not in filter_ids:continue
            print "y::::::::::::::::::::::", row_id
            meta_data = eval(meta_data) if meta_data else {}
            row_id = int(row_id)
            print "G:::::::::::::", row_id
            company_display_name    = company_display_name.replace('&amp;', '&')
            company_display_name    = company_display_name.replace('&', ' And ')
            company_display_name    = ' '.join(company_display_name.split())
            ijson_1 = {"user_id":"sunil", "ProjectName": company_display_name, "oper_flag":622, "ProjectID":row_id} #http://172.16.20.52:5010/tree_data
            ijson_2 = {"user_id":"sunil", "ProjectID":row_id, "WSName":company_display_name ,"db_name":"DataBuilder_%s"%(row_id),"oper_flag":90014} #http://172.16.20.52:5010/tree_data
            ijson_3 =  {"user_id":"sunil","oper_flag":97031,"ws_id":1,"project_id":row_id}  #http://172.16.20.52:5010/tree_data
            self.execute_url(ijson_1)
            self.execute_url(ijson_2)
            self.execute_url(ijson_3)
     #       cmd = "python cgi_wrapper_python.py '%s'"%(json.dumps(ijson_1)) #
      #      cmd1 = "python cgi_wrapper_python.py '%s'"%(json.dumps(ijson_2))
      #      cmd3 = "python cgi_wrapper_python.py '%s'"%(json.dumps(ijson_3))
      #      print 'cmd 1', [cmd]
      #      res1 = os.system(cmd) #CALL AJAX
     #       print 'cmd 2', [cmd1] 
     #       res2 = os.system(cmd1)   #CALL JAX
     #       print 'cmd 3', [cmd3]
     #       res3 = os.system(cmd3)   #CALL AJAX  
        db_obj.con.close()
        return 'done'
        
    def read_doc_lst(self, project_id):
        import sys
        read_data_cmd =  """ python cgi_wrapper_python.py '{"oper_flag":97030,"excel_name":"%s.xlsx","stage_lst":"1~6~7","project_id":%s,"db_name":"DataBuilder_%s","ws_id":1,"meta_data":{},"doc_type":"HTML2PDF","user_id":"demo_user1", "lc":1, "pd":"T"}' """%(project_id, project_id, project_id)
        process = subprocess.Popen(read_data_cmd, stdout=subprocess.PIPE, shell=True)
        output = process.communicate()
        sys.exit()
        output_lst = eval(output[0])[0]['data'][1]
        
        lst_of_docs = map(lambda x:str(x[0]), output_lst)
        import time 
        doc_cnt = 0
        while doc_cnt < len(lst_of_docs):   
            doc_str = '~'.join([lst_of_docs[doc_cnt], lst_of_docs[doc_cnt+1]])
            #time.sleep(2) 
            cmd = """ python cgi_wrapper_python.py '{"doc_type":"HTML2PDF","ws_id":1,"user_id":"demo_user1","lc":"1","meta_data":{"periodtype":"FY","Company":"test","Year":"2019","FYE":"12"},"db_name":"DataBuilder_%s","stage_lst":"1~6~7","oper_flag":97026,"project_id":%s,"p_type":"r","doc_lst":"%s"}' """%(project_id, project_id, doc_str)
            print cmd
            ps = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
            opt = ps.communicate()       
            doc_cnt += 2
    
        #doc_str = '~'.join(map(lambda x:str(x[0]), output_lst[2:4])) 
                
         
        
        #cmd = """ python cgi_wrapper_python.py '{"doc_type":"HTML2PDF","ws_id":1,"user_id":"demo_user1","lc":"1","meta_data":{"periodtype":"FY","Company":"test","Year":"2019","FYE":"12"},"db_name":"DataBuilder_%s","stage_lst":"1~6~7","oper_flag":97026,"project_id":%s,"p_type":"r","doc_lst":"%s"}' """%(project_id, project_id, doc_str)
        #print cmd
        #ps = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        #opt = ps.communicate()       
        #print opt




if __name__ == '__main__':
    obj = run_html()
    ijson = {"row_ids": [1568, 1569, 1570]}
    #obj.read_doc_lst(1566) 
    res = obj.run(ijson)
    #docs_res = obj.get_doc_res("1406")  
    #print docs_res
