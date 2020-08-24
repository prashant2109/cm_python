import MySQLdb, sys, os
import sqlite3
from collections import OrderedDict, defaultdict as dd
class Company_docTablePh_details():#companyAnd_docTablePh_details

    def __init__(self):
        self.dbname = "tfms_urlid"
        dbconnstr = "172.16.20.229#root#tas123"
        self.ip_addr, self.uname, self.passwd = dbconnstr.split('#')
 
    def docTablePage_tup(self, company_id):
        project_id, url_id  = company_id.split('_')    
     
        db=MySQLdb.connect(self.ip_addr,self.uname,self.passwd,self.dbname+"_"+str(project_id)+"_"+str(url_id)+"")
        cursor=db.cursor()
        sql = "select d.resid,n.norm_resid,n.docid,n.pageno from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid and d.taxoname not in ('Grid_Index','Grid@~@Grid Header','Grid@~@Parent Grid Header','Grid@~@Grid Footer','Grid@~@Parent Vertical Header','Grid@~@Vertical Grid Header','Non_Financial_Grid')" ;
        cursor.execute(sql)
        results = cursor.fetchall()
        db.close()
        norm_resid_lst = []
        for r in results:
            resid = str(r[0])
            norm_resid = str(r[1])
            docid = str(r[2])
            pageno = str(r[3])
            norm_resid_lst.append((docid, pageno,norm_resid))
        return norm_resid_lst
    
    def getDocPage_passingTableId(self, company_id):
        project_id, url_id  = company_id.split('_')    
     
        db=MySQLdb.connect(self.ip_addr,self.uname,self.passwd,self.dbname+"_"+str(project_id)+"_"+str(url_id)+"")
        cursor=db.cursor()
        sql = "select d.resid,n.norm_resid,n.docid,n.pageno from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid and d.taxoname not in ('Grid_Index','Grid@~@Grid Header','Grid@~@Parent Grid Header','Grid@~@Grid Footer','Grid@~@Parent Vertical Header','Grid@~@Vertical Grid Header','Non_Financial_Grid')" ;
        cursor.execute(sql)
        results = cursor.fetchall()
        db.close()
        norm_resid_dct = {}
        for r in results:
            resid = str(r[0])
            norm_resid = str(r[1])
            docid = str(r[2])
            pageno = str(r[3])
            norm_resid_dct[norm_resid] = (docid, pageno)
        return norm_resid_dct

    def getDistinct_docIds(self, company_id):
        project_id, url_id  = company_id.split('_')
        db=MySQLdb.connect(self.ip_addr,self.uname,self.passwd,self.dbname+"_"+str(project_id)+"_"+str(url_id)+"")
        cursor=db.cursor()
        sql = "select distinct(n.docid) from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid and d.taxoname not in ('Grid_Index','Grid@~@Grid Header','Grid@~@Parent Grid Header','Grid@~@Grid Footer','Grid@~@Parent Vertical Header','Grid@~@Vertical Grid Header','Non_Financial_Grid')" ;
        cursor.execute(sql)
        results = cursor.fetchall()
        db.close()
        distinct_docs = []
        for row in results:
            doc_id = int(str(row[0]))
            #if doc_id in (7, ):continue
            distinct_docs.append(doc_id)
        get_sorted_docs = sorted(distinct_docs)
        doc_ids_lst         = map(str, get_sorted_docs)
        return doc_ids_lst
    
    def getDistinct_tableIds(self, company_id):
        project_id, url_id  = company_id.split('_')
        db=MySQLdb.connect(self.ip_addr,self.uname,self.passwd,self.dbname+"_"+str(project_id)+"_"+str(url_id)+"")
        cursor=db.cursor()
        sql = "select n.norm_resid from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid and d.taxoname not in ('Grid_Index','Grid@~@Grid Header','Grid@~@Parent Grid Header','Grid@~@Grid Footer','Grid@~@Parent Vertical Header','Grid@~@Vertical Grid Header','Non_Financial_Grid')" ;
        cursor.execute(sql)
        results = cursor.fetchall()
        db.close() 
        distinct_tables = []
        for row in results:
            table_id = int(str(row[0]))
            distinct_tables.append(table_id)
        get_sorted_tables = sorted(distinct_tables)
        table_ids_lst     = map(str, get_sorted_tables)
        return table_ids_lst
    
    def get_tableList_passing_doc(self, company_id):
        project_id, url_id  = company_id.split('_')
        db=MySQLdb.connect(self.ip_addr,self.uname,self.passwd,self.dbname+"_"+str(project_id)+"_"+str(url_id)+"")
        cursor=db.cursor()
        sql = "select d.resid,n.norm_resid,n.docid,n.pageno from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid and d.taxoname not in ('Grid_Index','Grid@~@Grid Header','Grid@~@Parent Grid Header','Grid@~@Grid Footer','Grid@~@Parent Vertical Header','Grid@~@Vertical Grid Header','Non_Financial_Grid')" ;
        cursor.execute(sql)
        results = cursor.fetchall()
        db.close()
        
        table_list_dct = {}
        for row in results:
            table_id, doc_id = str(row[1]), str(row[2])

            if doc_id not in table_list_dct:
                table_list_dct[doc_id] = []
             
            table_list_dct[doc_id].append(table_id)
         
        lst_doc_ids = sorted(map(int, table_list_dct.keys()))

        doc_table_orderedDict = OrderedDict()
        for doc_id in lst_doc_ids:
            doc_id = str(doc_id)
            lst_tabs = table_list_dct.get(doc_id, [])
            tables_lst  = map(str, sorted(map(int, lst_tabs)))
            doc_table_orderedDict[doc_id] = tables_lst
        return doc_table_orderedDict

    def get_docId_passing_tableId(self, company_id):
        project_id, url_id  = company_id.split('_')
        db=MySQLdb.connect(self.ip_addr,self.uname,self.passwd,self.dbname+"_"+str(project_id)+"_"+str(url_id)+"")
        cursor=db.cursor()
        sql = "select d.resid,n.norm_resid,n.docid,n.pageno from norm_data_mgmt n,data_mgmt d where n.process_status= 'Y' and n.active_status= 'Y' and n.review_flag='Y' and d.process_status='Y' and d.active_status='Y' and d.resid=n.ref_resid and d.pageno=n.pageno and d.docid=n.docid and d.taxoname not in ('Grid_Index','Grid@~@Grid Header','Grid@~@Parent Grid Header','Grid@~@Grid Footer','Grid@~@Parent Vertical Header','Grid@~@Vertical Grid Header','Non_Financial_Grid')" ;
        cursor.execute(sql)
        results = cursor.fetchall()
        db.close() 
        table_doc_map = {}
        for row in results:
            table_id, doc_id = str(row[1]), str(row[2])
            table_doc_map[table_id] = doc_id
       
        table_lst = map(str, sorted(map(int, table_doc_map.keys())))

        table_map_doc_orderedDict = OrderedDict()
        for table_id in table_lst:
            doc_id = table_doc_map[table_id]
            table_map_doc_orderedDict[table_id] = doc_id    
 
        return table_map_doc_orderedDict


    def getCN_MID(self):
        cnmid_txtPath  = '/mnt/eMB_db/dealid_company_info.txt'
        f = open(cnmid_txtPath, 'r') 
        data_txt = f.readlines()
        f.close()
        getCNMID = {}
        for cnmid in data_txt:
            company_id, company_name, ipaddr = cnmid.rstrip().split(':$$:')
            machine_id = ipaddr.split('.')[-1]
            getCNMID[company_id] = (company_name, machine_id)
        return getCNMID

    def docId_map_ph(self, company_id):
        company_name, machine_id  = self.getCN_MID()[company_id]
        model_number = company_id.split('_')[0]
        db_path = '/mnt/eMB_db/{0}/{1}/tas_company.db'.format(company_name, model_number)
        print db_path
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        qry = 'select doc_id, (period || reporting_year) as ph from company_meta_info where doc_id != "";'
        try:
            cur.execute(qry)
        except Exception as e:
            print e

        Table_data = cur.fetchall()
        
        conn.close()

        doc_map_ph = {}
        for row in Table_data:
            doc_id, ph = map(str, row)
            #if ph[:-4] != 'FY':continue
            #if ph != 'FY2008':continue
            doc_map_ph[doc_id] = ph
        return doc_map_ph
    
    def getPH_in_order(self, data_year_sequence):
        
        def year_sort(data_year_sequence):
           hist_dict, forecast_dict = get_historical_forecast_year_dict(data_year_sequence)
           hist_li = get_sorted_historical_and_forecast_year_li(hist_dict)
           forecast_li = get_sorted_historical_and_forecast_year_li(forecast_dict)
           return hist_li+forecast_li

        def get_historical_forecast_year_dict(data_year_sequence):
            hist_dict = {}
            forecast_dict = {}
            for each_year in data_year_sequence:
                each = each_year    
                if not each:continue
                if 'E' == each[-1]:
                    x = int(each[-5:-1])
                    if not forecast_dict.get(x, {}):
                        forecast_dict[x] = {} 
                    y = each[:-5] 
                    if not forecast_dict[x].get(y, []):
                        forecast_dict[x][y] = []
                    forecast_dict[x][y].append(each_year)
                else:
                    try:
                       x = int(each[-4:])
                    except: continue
                    if not hist_dict.get(x, {}):
                        hist_dict[x] = {}
                    y = each[:-4] 
                    if not hist_dict[x].get(y, {}):
                        hist_dict[x][y] = []
                    hist_dict[x][y].append(each_year)
            return hist_dict, forecast_dict

        def get_sorted_historical_and_forecast_year_li(tmp_dict):
            ks = tmp_dict.keys()
            ks.sort()
            qhyear = ['Q1', 'Q2', 'H1']
            qmhyear = ['Q3', 'M9', 'Q4', 'H2']
            fyear = ['FY']

            final_sort_years = []

            for k in ks:
                vs = tmp_dict[k]
                vks = vs.keys()
                if 'Q1' in vks or 'Q2' in vks or 'H1' in vks:
                    for vk in qhyear:
                        if tmp_dict[k].get(vk, {}):
                            final_sort_years += tmp_dict[k][vk]

                if 'Q3' in vks or 'M9' in vks or 'Q4' in vks or 'H2' in vks:
                    for vk in qmhyear:
                        if tmp_dict[k].get(vk, {}):
                            final_sort_years += tmp_dict[k][vk]

                if 'FY' in vks: 
                    for vk in fyear:
                        if tmp_dict[k].get(vk, {}):
                            final_sort_years += tmp_dict[k][vk]
            return final_sort_years
        ordered_phs  = year_sort(data_year_sequence)
        return ordered_phs
    
    def distinct_phs(self, company_id):
        company_name, machine_id  = self.getCN_MID()[company_id]
        model_number = company_id.split('_')[0]
        db_path = '/mnt/eMB_db/{0}/{1}/tas_company.db'.format(company_name, model_number)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        qry = 'select distinct(period || reporting_year) from company_meta_info;'
        try:
            cur.execute(qry)
        except Exception as e:
            print e

        Table_data = cur.fetchall()
        
        conn.close()
        
        distinct_phs = [str(phs[0]) for phs in Table_data]
        return distinct_phs 
    
    def ordered_phs(self, company_id):
        distinct_phs = self.distinct_phs(company_id)
        ordered_phs  = self.getPH_in_order(distinct_phs)         
        return ordered_phs

    def getTablesStr_passingDocs(self, company_id, doc_str):

        doc_tableList_map  = self.get_tableList_passing_doc(company_id)    
     
        table_str_lst = []
        for doc in doc_str.split('#'):
            table_str_lst += doc_tableList_map[doc]
        table_str = '#'.join(table_str_lst)

        return table_str
        
    def get_company_id_pass_company_name(self, company_id):
        p_id, d_id = company_id.split('_')
        db_path  = '/mnt/eMB_db/company_info/compnay_info.db'
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = 'SELECT company_name, toc_company_id, project_id FROM company_info WHERE project_id="%s";'%(p_id)
        cur.execute(read_qry)
        table_data = cur.fetchall()
        conn.close()
        c_details = {}
        for row in table_data:
            company_name, toc_company_id, project_id = map(str, row)
            c_details[project_id+'_'+toc_company_id] = (company_name, '229')
        return c_details
    
    def getDocStr_passingDoc(self, company_id, table_str):
        table_map_doc  = self.get_docId_passing_tableId(company_id)
        doc_str_set   = set()
        for table in table_str.split('#'):
            doc_str_set.add(table_map_doc[table])
        doc_str = '#'.join(doc_str_set)
        return doc_str
    
    def getTableIdLst_passingTableType(self, company_id):
        model_number, dl = company_id.split('_')
        # get company_name and machine_id
        #getCn_mId = self.getCN_MID()
        getCn_mId  = self.get_company_id_pass_company_name(company_id)
        company_name, machine_id  = getCn_mId[company_id]
        #model_number = '1'

        # get table_type
        tableId_tableType_map = self.get_table_sheet_map(company_name, model_number, company_id)

        # making tableType_tableIdLst_map
        tableType_tableIdLst_map = {}
        for table_id, tableType in tableId_tableType_map.items():
            tableType_tableIdLst_map.setdefault(tableType, []).append(table_id)
            #tableType_tableIdLst_map[tableType].append(table_id)
        return tableType_tableIdLst_map
         
    def getTableIdLst_passingTableType_lst(self, company_id):
        # get company_name and machine_id
        getCn_mId = self.getCN_MID()
        company_name, machine_id  = getCn_mId[company_id]
        model_number = '1'

        # get table_type
        tableId_tableType_map = self.get_table_sheet_map_lst(company_name, model_number, company_id)

        # making tableType_tableIdLst_map
        tableType_tableIdLst_map = {}
        for table_id, tableType_lst in tableId_tableType_map.items():
            for tableType in tableType_lst:
                tableType_tableIdLst_map.setdefault(tableType, []).append(table_id)
        return tableType_tableIdLst_map

    def get_sheet_id_map(self):
        db_file     = '/mnt/eMB_db/node_mapping.db'
        conn        = sqlite3.connect(db_file)
        cur      =   conn.cursor() 
        sql   = "select sheet_id, node_name from node_mapping where review_flg = 0"
        try:
            cur.execute(sql)
            tres        = cur.fetchall()
        except:
            tres    = []
        conn.close()
        #print rr, len(tres)
        ddict = dd()
        for tr in tres:
            sheet_id, node_name = map(str, tr)
            ddict[sheet_id] = node_name
        return ddict

    def get_table_sheet_map(self, company_name, model_number, company_id):
        sheet_table_type_map = self.get_sheet_id_map()
        db_file     = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        print db_file
        conn        = sqlite3.connect(db_file)
        cur         = conn.cursor()
        sql   = "select sheet_id, table_id from table_group_mapping;"
        try:
            cur.execute(sql)
            tres        = cur.fetchall()
        except:
            tres    = []
        conn.close()
        table_sheet_map = {} 
        for row in tres:
            sheet_id, table_id_str = map(str, row)
            for table_id in table_id_str.split('^!!^'):
                if table_id == '':continue
                try:
                    get_tt = sheet_table_type_map[sheet_id]
                except:
                    continue
                table_sheet_map[table_id] = get_tt 
        return table_sheet_map

    def get_table_sheet_map_lst(self, company_name, model_number, company_id):
        sheet_table_type_map = self.get_sheet_id_map()
        db_file     = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn        = sqlite3.connect(db_file)
        cur         = conn.cursor()
        sql   = "select sheet_id, table_id from table_group_mapping;"
        try:
            cur.execute(sql)
            tres        = cur.fetchall()
        except:
            tres    = []
        conn.close()
        all_tables = dict.fromkeys(self.getDistinct_tableIds(company_id), 1)
        table_sheet_map = {} 
        for row in tres:
            sheet_id, table_id_str = map(str, row)
            for table_id in table_id_str.split('^!!^'):
                if table_id not in  all_tables:continue
                #if table_id == '':continue
                try:
                    get_tt = sheet_table_type_map[sheet_id]
                except:
                    continue
                if table_id not in table_sheet_map:
                    table_sheet_map[table_id] = []
                table_sheet_map[table_id].append(get_tt) 
        return table_sheet_map
    
    def get_all_classified_tables(self, company_id):
        all_table_lst = []
        tt_table_dict = self.getTableIdLst_passingTableType(company_id)
        #print tt_table_dict.keys()
        for k , v in tt_table_dict.iteritems():
            #if k not in ('OIE', 'FI'):continue
            #if k not in ('RBS', ):continue
            #print k
            all_table_lst += v
        #print len(all_table_lst)
        #ts = ["'"+str(e)+"'" for e in all_table_lst] 
        return all_table_lst #', '.join(ts)


    def auto_inc_get_all_classified_tables(self, company_id):
        all_table_dct = {}
        tt_table_dict = self.getTableIdLst_passingTableType(company_id)
        #print tt_table_dict.key
        for k , v in tt_table_dict.iteritems():
            #if k not in ('MovementInImpairedLoans,AdvancesAndFinancing', ):continue
            for tb in v:
                all_table_dct[tb] = k
        return all_table_dct #', '.join(ts)

    def delete_not_classified(self, company_id):
        
        tabs = self.get_all_classified_tables(company_id)
        
        project_id, url_id  = company_id.split('_')
        db=MySQLdb.connect(self.ip_addr,self.uname,self.passwd,self.dbname+"_"+str(project_id)+"_"+str(url_id)+"")
        cursor=db.cursor()
        del_stmt = """ DELETE FROM norm_data_mgmt WHERE norm_resid not in (%s) """%(tabs)
        print del_stmt
        #cursor.execute(del_stmt)
        db.commit()
        db.close()

    def auto_inc_get_table_sheet_map(self, company_name, model_number, company_id):
        sheet_table_type_map = self.get_sheet_id_map()
        db_file     = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        print db_file
        conn        = sqlite3.connect(db_file)
        cur         = conn.cursor()
        sql   = "select sheet_id, table_id from table_group_mapping;"
        try:
            cur.execute(sql)
            tres        = cur.fetchall()
        except:
            tres    = []
        conn.close()
        table_sheet_map = {} 
        for row in tres:
            sheet_id, table_id_str = map(str, row)
            for table_id in table_id_str.split('^!!^'):
                if table_id == '':continue
                try:
                    get_tt = sheet_table_type_map[sheet_id]
                except:
                    continue
                table_sheet_map[table_id] = (get_tt, sheet_id) 
        return table_sheet_map

    def tally_get_all_classified_tables(self, company_id):
        all_table_lst = []
        tt_table_dict = self.getTableIdLst_passingTableType(company_id)
        for k , v in tt_table_dict.iteritems():
            all_table_lst += v
        return all_table_lst 

    def get_sheet_id_mapi_auto_inc(self):
        db_file     = '/mnt/eMB_db/node_mapping.db'
        conn        = sqlite3.connect(db_file)
        cur      =   conn.cursor() 
        sql   = "select sheet_id, node_name, description from node_mapping where review_flg = 0"
        try:
            cur.execute(sql)
            tres        = cur.fetchall()
        except:
            tres    = []
        conn.close()
        #print rr, len(tres)
        ddict = dd()
        for tr in tres:
            sheet_id, node_name, desc = map(str, tr)
            ddict[node_name] = desc
        return ddict
    
    def doc_wise_classified_table_diff(self, company_id, doc_id):
        all_doc_wise_tables = self.get_tableList_passing_doc(company_id)[doc_id] 
        all_classy =  self.get_all_classified_tables(company_id)
        doc_classy = []
        for tab in all_doc_wise_tables:
            if tab in all_classy:
                doc_classy.append(tab)
        print doc_classy
        return 
    
if __name__ == '__main__':
    company_id  = '1_22'
    #doc_str     = '45#46#47#48#49#50#51#42'
    #doc_str     = '2012'
    #table_str    = ''
    doc_id      = '34'
    cObj    = Company_docTablePh_details()
    #print len(cObj.get_all_classified_tables(company_id))
    #cObj.doc_wise_classified_table_diff(company_id, doc_id)
    #print cObj.auto_inc_get_all_classified_tables(company_id)
    #print cObj.delete_not_classified(company_id)
    #print cObj.getTablesStr_passingDocs(company_id, doc_str)
    #company_name, model_number, company_id = 'IliadSA', '1', '1_39' 
    #print cObj.get_table_sheet_map_lst(company_name, model_number, company_id)
    #print cObj.getDocPage_passingTableId(company_id)
    #print  '#'.join(cObj.getTableIdLst_passingTableType(company_id)['IS'])
    #print '#'.join(cObj.get_all_classified_tables(company_id))
    #print  cObj.getTableIdLst_passingTableType_lst(company_id)
    #print  cObj.getDocStr_passingDoc(company_id, table_str)
    #print  cObj.getTablesStr_passingDocs(company_id, '1')
    #print  cObj.docTablePage_tup(company_id)
    print  cObj.getDistinct_docIds(company_id)
    #print  len(cObj.getDistinct_tableIds(company_id))
    #print cObj.getDistinct_tableIds(company_id)
    #print cObj.getDocStr_passingDoc(company_id)
    #print   cObj.getDistinct_tableIds(company_id)
    #print  '#'.join(cObj.get_tableList_passing_doc(company_id)['2351'])
    #print cObj.get_docId_passing_tableId(company_id).iteritems():
    #print  cObj.docId_map_ph(company_id)
    #print  cObj.getCN_MID()[company_id]
    #print  cObj.getPH_in_order(['H22017', 'Q12016', 'H12017', 'FY2018', 'Q22016', 'M92015', 'Q12018', 'M92014', 'FY2014', 'Q32015', 'Q42015']) 
    #print  cObj.distinct_phs(company_id)
    #print  cObj.ordered_phs(company_id)
