import json, sys, os
from pyapi import PYAPI
import modules
m_obj = modules.Modules()
import db.get_conn as get_conn
conn_obj = get_conn.DB()
def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    sys.stdout = sys.__stdout__
class WebAPI(PYAPI):
    def __init__(self):
        PYAPI.__init__(self)

    def process(self, cmd_id, ijson):
        res     = []
        if 1 == cmd_id:
            res = self.get_url_stats(ijson)
        elif 2 == cmd_id:
            import project_company_mgmt as pyf
            pObj = pyf.PC_mgmg()
            res = pObj.rm_user_filter_read_project_comapny_mgmt(ijson)
            #res = pObj.read_project_comapny_mgmt(ijson)
        elif 3 == cmd_id:
            import update_project_company_mgmt_pyapi as pyf
            pObj = pyf.PYAPI()
            res = pObj.update_insert_uploaded_doc_info(ijson)
        elif 4 == cmd_id:
            import project_company_mgmt as pyf
            pObj = pyf.PC_mgmg()
            res = pObj.only_doc_info(ijson)
        elif 5 == cmd_id:
            res = self.execute_url(ijson)
        elif 6 == cmd_id:
            res = self.setup_new_url(ijson)
        elif 7 == cmd_id:
            import module_storage_info_project_company_mgmt as pyf
            p_Obj  = pyf.PC_mgmg() 
            res = p_Obj.read_all_modules(ijson)
        elif 8 == cmd_id:
            import module_storage_info_project_company_mgmt as pyf
            p_Obj  = pyf.PC_mgmg() 
            res = p_Obj.user_save(ijson)
        elif 9 == cmd_id:
            import module_storage_info_project_company_mgmt as pyf
            p_Obj  = pyf.PC_mgmg() 
            res = p_Obj.read_saved_info(ijson)
        elif 10 == cmd_id:
            res = self.upload_document_info(ijson)
        elif 11 == cmd_id:
            res = self.read_all_company_info(ijson)
        elif 12 == cmd_id:
            res = self.project_configuration(ijson) 
        elif 13 == cmd_id:
            res = self.scheduler_process_mgmt_insert(ijson) 
        elif 14 == cmd_id:
            res = self.insert_url_name_data(ijson)
        elif 15 == cmd_id:
            res = self.delete_url_data(ijson) 
        elif 16 == cmd_id:
            import project_company_mgmt as pyf
            pObj = pyf.PC_mgmg()
            res = pObj.read_stage_list_info(ijson) 
        elif 17 == cmd_id:
            import status_update as pyf
            pObj = pyf.PC_mgmg()
            res = pObj.status_update(ijson) 
        elif 18 == cmd_id:
            res = self.get_schedule_info(ijson) 
        elif 19 == cmd_id:
            res = self.update_meta_10(ijson)
        elif 20 == cmd_id:
            res = self.validate_login(ijson)
        elif 21 == cmd_id:
            res = self.project_wise_doc_info_10(ijson)
        elif 22 == cmd_id:
            res = self.data_path_method_url_execution(ijson)
        elif 23 == cmd_id:
            res = self.add_new_company_docs(ijson)
        elif 24 == cmd_id:
            res = self.remove_company_docs(ijson)
        ###For Modules
        elif 25 == cmd_id:
            res = m_obj.get_modules()
        elif 26 == cmd_id:
            res = m_obj.save_modules(ijson)
        elif 27 == cmd_id:
            res = self.send_user_lst()
        elif 28 == cmd_id:
            import project_company_mgmt as pyf
            pObj = pyf.PC_mgmg()
            #res = pObj.rm_user_filter_read_project_comapny_mgmt(ijson)
            res = pObj.read_project_comapny_mgmt(ijson)
        elif 29 == cmd_id:
            res = self.save_user_configurations(ijson)
        elif 30 == cmd_id:
            res = self.user_wise_cofigured_project_data(ijson)
        elif 31 == cmd_id:
            res = m_obj.update_modified_module_mgmt(ijson)
        elif 32 == cmd_id:
            res = m_obj.rpurpose_modules()
        elif 33 == cmd_id:
            res = m_obj.read_updated_modules()
        elif 34 == cmd_id: # pre_taxo
            import pre_taxo_pyapi as pyf
            pObj = pyf.pyf()
            res = pObj.read_all_excel_ids(ijson)
        elif 35 == cmd_id:
            res = self.get_uml_data(ijson)
        elif 9999 == cmd_id:
            import config
            import login.user_info as login
            obj = login.Login(config.Config.s_dbinfo)
            res = obj.save_user_details(ijson)
        elif 36 == cmd_id:
            res = self.read_company_mgmt(ijson)
        elif 37 == cmd_id:
            #res = self.cp_update_insert_company_mgmt(ijson)
            #res = self.update_insert_company_mgmt_user_log(ijson)
            res = self.icp_update_insert_company_mgmt(ijson)
        elif 38 == cmd_id:
            import pre_taxo_tree_pyapi as pyf
            tp_Pobj = pyf.PYAPI() 
            res = tp_Pobj.data_builder_structure_tree_data_wseq(ijson)
        elif 39 == cmd_id: # pre_taxo_20.10 cmd_id(2)
            import pre_taxo_pyapi as pyf
            tp_Pobj = pyf.PYAPI() 
            res = tp_Pobj.read_all_excel_ids()
        elif 40 == cmd_id: # pre_taxo_20.10 cmd_id(3)
            import pre_taxo_pyapi as pyf
            tp_Pobj = pyf.PYAPI() 
            res = tp_Pobj.jn_read_excel_data_info_tree(ijson)
        elif 41 == cmd_id: # pre_taxo_20.10 cmd_id(4)
            import pre_taxo_pyapi as pyf
            tp_Pobj = pyf.PYAPI() 
            res = tp_Pobj.delete_rows_from_read_data_info(ijson)
        elif 42 == cmd_id: # pre_taxo_20.10 cmd_id(5)
            import pre_taxo_pyapi as pyf
            tp_Pobj = pyf.PYAPI() 
            res = tp_Pobj.search_from_google_get_results(ijson)
        elif 43 == cmd_id: # pre_taxo_20.10 cmd_id(6)
            import pre_taxo_pyapi as pyf
            tp_Pobj = pyf.PYAPI() 
            res = tp_Pobj.accept_reject_info_update(ijson)
        elif 44 == cmd_id: # pre_taxo_20.10 cmd_id(8)
            import pre_taxo_pyapi as pyf
            tp_Pobj = pyf.PYAPI() 
            res = tp_Pobj.individual_info_tree(ijson)
        elif 45 == cmd_id: # pre_taxo_20.10 cmd_id(10)
            import pre_taxo_pyapi as pyf
            tp_Pobj = pyf.PYAPI() 
            res = tp_Pobj.read_project_info()
        elif 46 == cmd_id: # pre_taxo_20.10 cmd_id(11)
            import pre_taxo_pyapi as pyf
            tp_Pobj = pyf.PYAPI() 
            res = tp_Pobj.project_wise_company_info(ijson)
        elif 47 == cmd_id: # pre_taxo_20.10 cmd_id(12)
            import pre_taxo_pyapi as pyf
            tp_Pobj = pyf.PYAPI() 
            res = tp_Pobj.company_wise_doc_info(ijson)
        elif 48 == cmd_id: # pre_taxo_20.10 cmd_id(12)
            import pre_taxo_tree_pyapi as pyf
            tp_Pobj = pyf.PYAPI() 
            res = tp_Pobj.data_builder_structure_tree_data_wseq_grid_data(ijson)
        elif 49 == cmd_id: # pre_taxo_20.10 cmd_id(12)
            import pre_taxo_tree_pyapi as pyf
            tp_Pobj = pyf.PYAPI() 
            res = tp_Pobj.data_builder_structure_tree_data_wseq_reference(ijson)
        elif 50 == cmd_id: # pre_taxo_20.10 cmd_id(12)
            import pre_taxo_tree_pyapi as pyf
            tp_Pobj = pyf.PYAPI() 
            res = tp_Pobj.delete_quick_paths(ijson)
        elif 51 == cmd_id:
            res = self.delete_company_from_company_mgmt(ijson)
        elif 52 == cmd_id:
            res = self.read_company_meta_data(ijson)
        elif 53 == cmd_id:
            res = self.read_user_log(ijson)
        elif 54 == cmd_id:
            res = self.read_all_doc_ids(ijson)
        elif 55 == cmd_id:
            res = self.generate_excel_sheet(ijson)
        elif 56 == cmd_id:
            res = self.save_doc_details(ijson)
        elif 57 == cmd_id:
            res = self.read_doc_id_details(ijson)
        elif 58 == cmd_id:
            res = self.delete_selected_doc_id(ijson)
        elif 59 == cmd_id:
            res = self.delete_sel_sub_data(ijson)
        elif 60 == cmd_id:
            res = self.delete_sel_currency(ijson)
        elif 61 == cmd_id:
            res = self.delete_sel_tickeer(ijson)
        elif 62 == cmd_id:
            res = self.default_settings(ijson)
        elif 63 == cmd_id:
            res = self.read_company_info(ijson)
        elif 64 == cmd_id:
            res = self.read_company_individually(ijson)
        elif 9000 == cmd_id:
            import r_pyapi as rp
            robj = rp.PYAPI()
            res = robj.iicp_update_insert_company_mgmt(ijson)
        elif 65 == cmd_id:
            res = self.insert_or_update_to_mgmt(ijson)
        elif 66 == cmd_id:
            res = self.select_company_info_with_respect_to_client_id(ijson)
        elif 67 == cmd_id:
            res = self.read_DB_status(ijson)
        elif 68 == cmd_id:
            res = self.create_database(ijson)
        elif 69 == cmd_id:
            res = self.upload_sel_docs(ijson)
        elif 70 == cmd_id:
            res = self.create_and_download_excel(ijson)
        elif 71 == cmd_id:
            res = self.reporocess_document(ijson)
        elif 72 == cmd_id:
            import meta_data_using_CM as md
            m_Obj = md.PYAPI()
            res = m_Obj.update_company_meta_info_txt(ijson)
        elif 73 == cmd_id:
            #res = self.create_company_details(ijson)
            res = self.create_company_details_current_dir(ijson)
        elif 74 == cmd_id:
            import redis_search_api
            r_obj = redis_search_api.search()
            string = ijson["text"]
            res = r_obj.search_exact_Query(string)
        elif 75 == cmd_id:
            res = self.validate_builder_docs(ijson)
        elif 76 == cmd_id:
            res = self.update_tas_sec_acc_no(ijson)

        elif 77 == cmd_id:
            res = self.download_sec_docs(ijson)

        return json.dumps(res)

if __name__ == '__main__':
    obj = WebAPI()
    try:
        ijson   = json.loads(sys.argv[1])
        cmd_id  = int(ijson['cmd_id'])
    except:
        cmd_id  = int(sys.argv[1])
        ijson   = {}
        if len(sys.argv) > 2:
            tmpjson = json.loads(sys.argv[2])
            ijson.update(tmpjson)
    if ijson.get('PRINT') != 'Y':
        disableprint()
    #print ijson
    res = obj.process(cmd_id, ijson)
    enableprint()
    print res
