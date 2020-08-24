import MySQLdb
class db_api:

    def __init__(self, db_name):
        self.con, self.cur = self.get_connection(db_name)
 
    def get_connection(self, dbstr):
        khost, kuser , kpasswd, kdb  = dbstr.split('##')
        con = MySQLdb.connect(khost, kuser, kpasswd, kdb)
        cur = con.cursor()
        return con, cur

    def get_project(self):
        sql = "select ProjectID,ProjectName,ProjectCode from ProjectMaster where ProjectStatus='Y'"
        self.cur.execute(sql)
        data = self.cur.fetchall()
        return data

    def get_document_name(self, docs):
        sql = "select doc_id, doc_name, doc_type, meta_data from batch_mgmt_upload where doc_id in (%s)"%(docs)
        self.cur.execute(sql)
        data = self.cur.fetchall()
        return data

    def get_docs_from_company(self, docs):
        sql = "select doc_id from batch_mgmt_upload where batch ='%s'"%(docs)
        self.cur.execute(sql)
        data = self.cur.fetchall()
        return data

    def get_company(self):
        sql = "select doc_id,doc_name,doc_type,meta_data from batch_mgmt_upload where status in ('10N', '1P')"
        self.cur.execute(sql)
        data = self.cur.fetchall()
        return data        

    def get_grid_id(self, doc_id, page):
        sql = "select max(groupid) from db_data_mgmt_grid_slt where docid=%s and pageno =%s and groupid<1000"%(doc_id, page)
        self.cur.execute(sql)
        data = self.cur.fetchone()
        if data and data[0]:
           return data[0]
        return 0

    
    def insert_inc_grid(self, lst):
        sql = "INSERT INTO db_data_mgmt_grid_slt(docid, pageno, groupid, active_status, udata, userid, sdata) VALUES(%s, %s, %s, '%s', '%s', '%s', '%s')"%lst        
        self.cur.execute(sql)
        self.con.commit()
        return 1 

    def get_classification(self, docs):
        sql = "select distinct(a.table_name) from Focus_Data_mgmt as a inner join db_data_mgmt_grid_slt as b ON a.doc_id = b.docid and a.page_no = b.pageno and a.grid_id = b.groupid   where doc_id in (%s) and b.active_status='Y'"%(docs)
        self.cur.execute(sql)
        data = self.cur.fetchall()
        if not data:
            return []
        return data
 
    def insert_classification(self, lst):
        #print lst
        sql = "insert into Focus_Data_mgmt (doc_id, company, table_name, page_no, grid_id, date_time, user_id, tableid) values(%s, '%s', '%s', %s, %s, now(), '%s', '%s')"%lst
        self.cur.execute(sql)
        self.con.commit()
        return '1'


    def fetch_divide_mgmt_data(self, docs):
        sql = "select doc_id from Dividend_Topic_Master where doc_id in (%s) and extracted_val like '%s' GRoup By doc_id"%(docs , '%_%')
        #sql = "select doc_id from Dividend_Topic_Master where doc_id in (%s) and extracted_val is not null and extracted_val != '' GRoup By doc_id"%(docs)
        #print sql
        self.cur.execute(sql)
        data = self.cur.fetchall()
        if data and data[0]:
           return data
        return []

    def read_reject_divident(self, docs):
        sql = "select doc_id, xlm_ids, char_idx from Dividend_Topic_Master_Reject where doc_id in (%s)"%(docs)
        self.cur.execute(sql)
        data = self.cur.fetchall()
        if data:
           return data
        return []

    def get_sentence_grids(self, doc_id , page_no):
        sql = "select docid, pageno, groupid, udata  from db_data_mgmt_grid_slt where active_status='Y' and  docid=%s and pageno =%s and userid='From Sentence'"%(doc_id , page_no)
        self.cur.execute(sql)
        data = self.cur.fetchall()
        if data:
           return data
        return []
       
    def delete_inc_grid_focsusDMgt(self, doc_id , pageno, grid_id):
        sql =  "delete from db_data_mgmt_grid_slt where docid=%s and pageno =%s and userid='From Sentence' and groupid=%s"%(doc_id , pageno, grid_id)
        sql1 =  "delete from Focus_Data_mgmt where doc_id=%s and page_no =%s and grid_id=%s"%(doc_id , pageno, grid_id)
        #print sql, sql1
        #return []
        self.cur.execute(sql)
        self.cur.execute(sql1)
        self.con.commit()
        return '1'
      
    def fetch_divide_mgmt_data_flg(self, docs):
        sql = "select distinct(slt_data) from Dividend_Topic_Master where doc_id in (%s) and extracted_val like '%s'"%(docs , '%_%')
        self.cur.execute(sql)
        data = self.cur.fetchall()
        if data and data[0]:
           return data
        return []

    def fetch_divide_mgmt_data_flgs(self, docs, taxos):
        sql = "select doc_id from Dividend_Topic_Master where doc_id in (%s) and extracted_val like '%s' and category='%s' and section_title='%s' and taxoname = '%s' GRoup By doc_id"%(docs , '%_%', taxos[0], taxos[1], taxos[2])
        self.cur.execute(sql)
        data = self.cur.fetchall()
        if data and data[0]:
           return data
        return []

    def read_company_ids(self, client_id):
        if client_id == None:
            sql = "select row_id, company_id from client_company_mgmt"
        else:
            sql = "select row_id, company_id from client_company_mgmt where client_id = %s"%(client_id)
        self.cur.execute(sql)
        data = self.cur.fetchall()
        if data and data[0]:
           return data
        return []
        
    def read_meta_info(self, cmp_ids):
        sql = "select row_id, company_name, company_display_name, meta_data from company_mgmt where row_id in (%s)"%(cmp_ids)
        print sql
        self.cur.execute(sql)
        data = self.cur.fetchall()
        if data and data[0]:
           return data
        return []

    def read_doc_meta_info(self, cmp_id):
        sql = "select doc_id, company_id ,document_type ,filing_type , period, year, document_name, document_release_date, document_from, document_to, document_download_date, previous_release_date, language, meta_data, user_name, date, time, assension_number, source, next_release_date, other_info ,url_info, sec_filing_number ,source_type from document_master  where company_id=%s"%(cmp_id)
        self.cur.execute(sql)
        data = self.cur.fetchall()
        if data and data[0]:
           return data
        return []
