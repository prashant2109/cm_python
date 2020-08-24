import os, sys, MySQLdb, datetime
from collections import OrderedDict as OD
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import config
#import copy

class  PC_mgmg(object):
    def __init__(self):
        self.module_key_map  = {
                                0:'rid',
                                1:'n',
                                2:'full_name',
                                3:'k',
                                4:'fa_icon',
                                5:'doc_view',
                                6:'pk',
                                7:'level_id',
                                8:'usf',
                                9:'active'
                                }   

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def read_insert_copy_modified_module_mgmt_db(self):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'CompanyManagement'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT name, m_key, full_name, fa_icon, doc_view, active, insert_user, update_user FROM modified_module_mgmt; """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        insert_rows = []
        for row in t_data:
            insert_rows.append(row)

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        insert_stmt = """ INSERT INTO modified_module_mgmt(name, m_key, full_name, fa_icon, doc_view, active, insert_user, update_user) VALUES(%s, %s, %s, %s, %s, %s, %s, %s) """
        m_cur.executemany(insert_stmt, insert_rows)
        m_conn.commit()
        m_conn.close()
        return 
    
    def insert_project_module_info(self, ijson):
        project_id     = ijson["project_id"]
        module_name    = ijson["module_name"]
        module_key     = ijson["module_key"]
        module_parent  = ijson["module_parent"]
        user_name      = ijson["user_name"]
        d_time         = str(datetime.datetime.now())
        
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 

        read_qry = """ SELECT project_id, module_parent, row_id FROM project_modified_module_mgmt; """
        m_cur.execute(read_qry)
        pm_data = m_cur.fetchall()
        pm_res  = {}
        get_max = 0
        for row in pm_data:
            project_id, m_p = map(str, row[:2])
            row_id = int(row_id)
            pm_res[(project_id, m_p)]  = row_id
            
        if (project_id, module_parent) not in pm_res:
            insert_stmt = """ INSERT INTO project_modified_module_mgmt(project_id, module_parent, user_name, insert_time) VALUES('%s', '%s', '%s', '%s')  """%(project_id, module_parent, user_name, d_time)
            m_cur.execute(insert_stmt)
            m_conn.commit()
            read_last = """ SELECT row_id from project_modified_module_mgmt ORDER BY row_id DESC LIMIT 1; """
            m_cur.execute(read_last)
            last_row = int(m_cur.fetchone()[0])
        else:
            last_row = pm_res[((project_id, m_p))]
            
        read_qry = """ SELECT parent_row_id, module_key, order_id FROM modified_module_mgmt  """%(last_row)
        m_cur.execute(read_qry)
        mm_data = m_cur.fetchall()
        
        mm_dct = {(str(row[0]), str(row[1])):str(row[2])   for row in mm_data}

        if (last_row, module_key) not in mm_dct:
            get_max_order_id = int(max(mm_dct.values()))+1
            ins_stmt = """ INSERT INTO modified_module_mgmt(module_name, module_key, parent_row_id, order_id) """%(module_name, module_key, last_row, get_max_order_id)
            m_cur.execute(ins_stmt)
            m_conn.commit()
        m_conn.close()
        return 'done'
    
    def read_tree_txt_file(self):
        f = open('/root/databuilder_train_ui/tenkTraining/ProjectBuilder_Copy/pysrc/tree.txt')
        data = eval(f.read())
        return data
        
            
    def insert_module_info(self):
        data_lst = self.read_tree_txt_file()
        select_flg = '0'
    
        #print type(data_lst), data_lst;sys.exit('MT')
        def rec_func(sub_lst, pk_info):
            r_lst = []
            for psk, c_dct in enumerate(sub_lst, 1):
                p_info = '.'.join([pk_info, str(psk)]) 
                clevel_id = str(len(p_info.split('.')) - 1)
                #print c_dct
                gc_n       = c_dct['n']
                gc_desc    = c_dct.get('full_name', 'EMPTY')
                gc_uk      = c_dct['k']
                gc_fi      = c_dct.get('fa_icon', 'EMPTY')
                gc_dc      = c_dct.get('doc_view', 'EMPTY')
                gactv       = c_dct['active']
                ci = (gc_n, gc_desc, gc_uk, gc_fi, str(gc_dc), p_info, clevel_id, select_flg, gactv, 'demo')
                ci = map(str, ci)
                r_lst.append(ci)
                s_menu  = c_dct['submenu']
                if s_menu:
                    r_ls = rec_func(s_menu, p_info)
                    r_lst += r_ls
            return r_lst
    
        res_lst = [] 
        for pk, i_dct in enumerate(data_lst, 1):
            #print i_dct
            pk_info   = str(pk)
            level_id  = str(len(pk_info.split('.')) - 1)
            g_n       = i_dct['n']
            g_desc    = i_dct.get('full_name', 'EMPTY')
            g_uk      = i_dct['k']            
            g_fi      = i_dct['fa_icon']  
            g_dc      = i_dct.get('doc_view', 'EMPTY')
            actv      = i_dct['active']
            di = (g_n, g_desc, g_uk, g_fi, str(g_dc), pk_info, level_id, select_flg, actv, 'demo')
            di = map(str, di)
            res_lst.append(di)
            sub_menu  = i_dct['submenu']            
            if sub_menu:
                #rec_func call
                rs_ls = rec_func(sub_menu, pk_info)
                res_lst += rs_ls 
        
        '''
        Write insert stmt 
        '''
        for k in res_lst:
            print k, '\n'
        #sys.exit('MT')
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
            
        if 1:
            drop_stmt = """  DROP TABLE modified_module_mgmt """
            try:
                m_cur.execute(drop_stmt)
            except:ss = ''
        crt_stmt = """  CREATE TABLE IF NOT EXISTS modified_module_mgmt(row_id INTEGER NOT NULL AUTO_INCREMENT, project_id VARCHAR(256) DEFAULT NULL , module_name VARCHAR(256) DEFAULT NULL,  description TEXT DEFAULT NULL, module_key VARCHAR(256) DEFAULT NULL, fa_icon_info VARCHAR(256) DEFAULT NULL, doc_view VARCHAR(10) DEFAULT NULL, parent_key_info VARCHAR(50) DEFAULT NULL, level_id VARCHAR(50) DEFAULT NULL, user_select_flg VARCHAR(2) DEFAULT NULL, active_status VARCHAR(2) DEFAULT 'Y', user_name VARCHAR(256) DEFAULT NULL, insert_time VARCHAR(256) DEFAULT NULL, PRIMARY KEY (row_id))       """
        m_cur.execute(crt_stmt)
        
        if res_lst:
            insert_stmt = """ INSERT INTO modified_module_mgmt(module_name, description, module_key, fa_icon_info, doc_view, parent_key_info, level_id, user_select_flg, active_status, user_name) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)  """
            m_cur.executemany(insert_stmt, res_lst)
            m_conn.commit()
        m_conn.close()
        return
    
    def user_save(self, ijson):
        project_id = ijson["project_id"]
        save_keys_dct  = ijson['data']
        user_name   = str(ijson.get('user', 'demo'))
        if not user_name:
            user_name = 'demo'

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        
        #crt_stmt = """ CREATE TABLE IF NOT EXISTS project_module_key_map(row_id INTEGER NOT NULL AUTO_INCREMENT, project_id VARCHAR(256) DEFAULT NULL, module_key VARCHAR(256) DEFAULT NULL, key_flag VARCHAR(256) DEFAULT NULL, user_name VARCHAR(256) DEFAULT NULL, insert_time VARCHAR(256) DEFAULT NULL, PRIMARY KEY (row_id)) """
        #try:
        #    m_cur.execute(crt_stmt) 
        #except:s = ''
        
    
        read_qry = """ SELECT module_key, key_flag FROM project_module_key_map WHERE project_id='%s'  AND user_name='%s' """%(project_id, user_name) 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        check_dct = {str(r[0]):str(r[1]) for r in t_data}       

        insert_rows = [] 
        update_rows = []
        for key, flg in save_keys_dct.items():
            if key not in check_dct:
                insert_rows.append((project_id, key, flg, user_name))
            elif key in check_dct:
                get_chk_val = check_dct[key]
                if flg != get_chk_val:
                    update_rows.append((flg, key, project_id, user_name))
        if insert_rows:
            insert_stmt = """ INSERT INTO project_module_key_map(project_id, module_key, key_flag, user_name) VALUES(%s, %s, %s, %s)  """
            m_cur.executemany(insert_stmt, insert_rows)
        if update_rows:
            update_stmt = """ UPDATE project_module_key_map SET key_flag=%s WHERE module_key=%s AND project_id=%s AND user_name=%s """ 
            m_cur.executemany(update_stmt, update_rows)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]
    
    def read_saved_info(self, ijson):
        user_name = str(ijson.get('user', 'demo'))
        if not user_name:
            user_name = 'demo'

        project_id = ijson["project_id"]

        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        
        rd_qry = """ SELECT module_key, key_flag FROM project_module_key_map WHERE project_id='%s'  AND user_name='%s' """%(project_id, user_name)
        m_cur.execute(rd_qry)
        t_data = m_cur.fetchall()
        res_dct = {}
        for row in t_data:
            module_key, key_flag = row
            res_dct[module_key] = key_flag
        return [{'message':'done', 'data':res_dct}]

    def read_all_modules(self, ijson):
        user_name = str(ijson.get('user', 'demo'))
        if not user_name:
            user_name = 'demo'
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        read_qry = """ SELECT row_id, module_name, description, module_key, fa_icon_info, doc_view, parent_key_info, level_id, user_select_flg, active_status FROM modified_module_mgmt """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        
        info_dct = {}
        pc_dct  = OD()
        o_parent_lst = []
    
        for row in t_data:
            row_id, module_name, description, module_key, fa_icon_info, doc_view, parent_key_info, level_id, user_select_flg, active = row
            #print row_id, module_name, description, module_key, fa_icon_info, doc_view, parent_key_info, level_id, user_select_flg, active
            if str(doc_view) == 'True':
                doc_view = 1
            if str(doc_view) == 'False':
                doc_view = 0
            #print doc_view, type(doc_view)
            if parent_key_info not in pc_dct:
                pc_dct[parent_key_info] = []
            if len(parent_key_info.split('.')) == 1:
                get_pc =   parent_key_info
                o_parent_lst.append(get_pc)
            elif len(parent_key_info.split('.')) > 1:
                get_pc = parent_key_info[:-2] 
                pc_dct.setdefault(get_pc, []).append(parent_key_info)         
            
            dst = {'submenu':[]}
            #self.module_key_map
            for ix, ks in enumerate(row):
                if (ks=='' or ks == 'EMPTY') and (ks != 0):continue
                if ks == 'True':    
                    ks = True
                if ks == 'False':
                    ks = False
                get_key = self.module_key_map[ix] 
                dst[get_key] = ks
            info_dct[parent_key_info] = dst
        print info_dct 
        def rec_func(child_lst):
            ch_res_lst = []
            for ch_i in child_lst:
                cc_lst  = pc_dct[ch_i]
                if not cc_lst:
                    cc_data_info = info_dct[ch_i]
                elif cc_lst:
                    cc_ch_lst = rec_func(cc_lst)
                    cc_data_info = info_dct[ch_i]
                    cc_data_info['submenu']  = cc_ch_lst
                ch_res_lst.append(cc_data_info) 
            return ch_res_lst              

        res_lst = []
        for dt_i  in o_parent_lst:
            get_ch_lst = pc_dct[dt_i]
            if not get_ch_lst:
                get_data_info = info_dct[dt_i]
            elif get_ch_lst:
                rs_ch_lst  = rec_func(get_ch_lst)
                get_data_info = info_dct[dt_i]
                get_data_info['submenu'] = rs_ch_lst
            res_lst.append(get_data_info)
        return [{'message':'done', 'data':res_lst}]
    
    def update_modified_module_mgmt(self, ijson):
        update_data = ijson['data']
        update_rows = []
        check_rows  = []
        for row in update_data:
            row_id, parent_key, order_id = row['id'], row['p_id'], row['index'] 
            update_rows.append((parent_key, order_id, row_id))
            check_rows.append(row_id)
           
        print update_rows 
        m_key_str = ', '.join({'"'+str(e)+'"' for e in check_rows})    
        # (name, m_key, full_name, fa_icon, doc_view, active, insert_user, update_user)
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        u_stmt = """ UPDATE modified_module_mgmt SET parent_key=0, order_id=0, alive_status='N'  """
        m_cur.execute(u_stmt)
        update_stmt = """ UPDATE modified_module_mgmt SET parent_key=%s, order_id=%s, alive_status='Y'  WHERE row_id=%s """
        m_cur.executemany(update_stmt, update_rows)
        #y_update = """ UPDATE modified_module_mgmt SET alive_status='N' WHERE row_id not in (%s) """%(m_key_str)
        #m_cur.execute(y_update) 
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]
        

    def read_updated_modules(self):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        read_qry = """ SELECT row_id, name, m_key, full_name, fa_icon, doc_view, active, parent_key, order_id FROM modified_module_mgmt WHERE alive_status='Y' """ 
        m_cur.execute(read_qry)
        t_data= m_cur.fetchall()
        m_conn.close()

        sort_info_cdict = {}
        data = {}
        map_dct = {}
        p_dict = {}
        row_id_map = {}
        for row in t_data:  
            row_id, name, m_key, full_name, fa_icon, doc_view, active, parent_key, order_id = map(str, row)
            if doc_view == 'Y':
                doc_view = 1
            elif doc_view == 'N':
                doc_view = 0
            if parent_key == '-1':
                parent_key = 'root'
            row_id_map[row_id] = m_key
            data[m_key]  = {'rid':row_id, 'fa_icon':fa_icon, 'full_name':full_name, 'doc_view':doc_view, 'k':m_key, 'n':name, 'pk':parent_key, 'order_id':order_id, 'active':active,'submenu':[]}
            map_dct.setdefault(parent_key, []).append(row_id) 
            sort_info_cdict[m_key] = int(order_id)
        
        print map_dct
        def rec_func(c_ids, cl_id):    
            cl_id += 1
            cres_lst = []
            for c_rid in c_ids: 
                cmky = row_id_map[c_rid]
                cinf_data_dct = data[cmky]
                cinf_data_dct['level_id'] = cl_id
                gt_ky_lst = map_dct.get(c_rid, [])
                #if not gt_ky_lst:continue
                r_ls = []
                if gt_ky_lst:
                    r_ls = rec_func(gt_ky_lst, cl_id)
                cinf_data_dct['submenu'] = r_ls
                cres_lst.append(cinf_data_dct)
            cres_lst.sort(key=lambda x:sort_info_cdict[x['k']])
            return cres_lst

        res_lst = []
        get_all_roots = map_dct['root']
        for r_prnt_id in get_all_roots:
            l_id = 0
            pmky = row_id_map[r_prnt_id]
            pdata = data[pmky]
            pdata['level_id'] = 0
            chld_ids = map_dct.get(r_prnt_id, [])
            #if not chld_ids:continue
            cd_lst = []
            if chld_ids:
                cd_lst = rec_func(chld_ids, l_id)
            pdata['submenu'] = cd_lst
            res_lst.append(pdata)
        res_lst.sort(key=lambda x:sort_info_cdict[x['k']])
        return [{'message':'done', 'data':res_lst}]

    def rpurpose_modules(self):
        m_conn, m_cur   = conn_obj.MySQLdb_connection(config.Config.s_dbinfo) 
        read_qry = """ SELECT row_id, name, m_key, full_name, fa_icon, doc_view, active, parent_key, order_id FROM modified_module_mgmt WHERE alive_status='Y' and parent_key !=0 """ 
        m_cur.execute(read_qry)
        
        t_data= m_cur.fetchall()
        m_conn.close()

        sort_info_cdict = {}
        data = {}
        map_dct = {}
        row_id_map = {}
        child_parent = {}
        for row in t_data:  
            row_id, name, m_key, full_name, fa_icon, doc_view, active, parent_key, order_id = map(str, row)
            if parent_key == '-1':
                parent_key = 'root'
            row_id_map[row_id] = m_key
            data[m_key]  = {'fa_icon':fa_icon, 'full_name':full_name, 'k':m_key, 'n':name, 'parent_key':parent_key, 'order_id':order_id, 'rid':row_id}
            map_dct.setdefault(parent_key, []).append(row_id) 
            sort_info_cdict[m_key] = int(order_id)
        
        
        c_dict = {}
        p_dict = {}
        r_data = {}
        for pkey, dt_lst in map_dct.iteritems():
            if pkey == 'root':
                gt_k = pkey 
            else:
                gt_k = row_id_map.get(pkey)
                if not gt_k:continue
            c_lst = []  
            for rds in dt_lst:
                gt_vl = row_id_map[rds]
                get_data_dct = data[gt_vl]
                r_data[gt_vl] = get_data_dct
                c_lst.append(gt_vl)
                p_dict[gt_vl] = gt_k
            c_lst.sort(key=lambda x:sort_info_cdict[x])
            c_dict[gt_k] = c_lst
        return [{'message':'done', 'data': r_data, 'p_dict': p_dict, 'c_dict': c_dict}]
    
if __name__  ==  '__main__':
    p_Obj = PC_mgmg()
    #print p_Obj.rpurpose_modules()



