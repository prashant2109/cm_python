import os, sys
import shelve

import common.baseobj as bobj
import common.getconfig as getconfig
import common.datastore as datastore
import common.get_doc_data as get_doc_data
import common.filePathAdj as fileabspath
import common.decfile as decfile

#cfgfname = fileabspath.filePathAdj().get_file_path('config.ini')
#cfgObj = getconfig.getconfig(cfgfname)
#idatapath = cfgObj.get_config('PageAnalysis', 'input')
#ipath, opath, isdb, isenc = bobj.BaseObj().get_project_info()
#print cfgfname
#print cfgObj
#print ipath, opath, isdb, isenc
#sys.exit()
#isdb = int(isdb) 
#isenc = int(isenc) 

def return_data(data_path):
    dd = datastore.read_data_fname(data_path, isdb, isenc)
    data_dict = dd.get('data', {})
    return data_dict


def read_idata(fname):
        decObj = decfile.decfile()
        decObj.open(fname, 'r')
        lines = map(lambda x:x.strip("\n"), decObj.readlines())
        decObj.close()
        return lines


def get_visual_group_proj_dict(doc_id, page_no): 
    ci_odir = cfgObj.get_config('MOD_DIRNAME', 'visprojdict')
    fname = '%s.sh' %(page_no)
    shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    #print shname
    if not os.path.exists(shname): 
        #print >> sys.stderr, 'Visual Group projected dict not found! ' 
        return {}

    shv = datastore.read_data_fname(shname, isdb, isenc)
    res_dict = shv.get('vis_proj_dict', {})
    return res_dict

def get_semantic_ph(doc_id): 
    ci_odir = cfgObj.get_config('SemanticModule', 'oldPH_odir')
    fname = '%s.sh' %(doc_id)
    shname = os.path.join(opath, str(doc_id), ci_odir, fname)
    #print shname
    if not os.path.exists(shname): 
        print >> sys.stderr, 'Visual Group projected dict not found! ' 
        return {}

    shv = datastore.read_data_fname(shname, isdb, isenc)
    res_dict = shv.get('data', {})
    return res_dict




def get_media_box(doc_id, pno):
    #rm_path = os.path.join(opath, str(doc_id), "db", str(pno), 'pdfdata.db')
    rm_path = os.path.join(ipath, str(doc_id), "db", str(pno), 'pdfdata.db')
    #print rm_path 
    if not os.path.exists(rm_path):
        print >> sys.stderr, ' pdfdata not found'
        return []
            
    dd = datastore.read_data_fname(rm_path, isdb, isenc, {}, 'pdfdata')
    data_dict = dd.get('page_master', [])
    if data_dict:  
       bbox_dict = data_dict[0].get('bbox', {})
       if bbox_dict:
          return "%s_%s_%s_%s" %(bbox_dict['x0'], bbox_dict['y0'], bbox_dict['w'], bbox_dict['h'])
    return ""

def get_parametric_data_level(doc_id, page_no, level):
    ph_odir = cfgObj.get_config('PageAnalysis', 'ParametricResults_odir')
    fname = '%s_%s.sh'%(page_no, level) 
    #ph_data_path = os.path.join(ipath, str(doc_id), ph_odir, fname)
    ph_data_path = os.path.join(opath, str(doc_id), ph_odir, fname)
    if not os.path.exists(ph_data_path): 
        print >> sys.stderr, 'PH DATA not found! '
        return {}
    return return_data(ph_data_path)


def get_parametric_data(doc_id, page_no):
    ph_odir = cfgObj.get_config('PageAnalysis', 'ParametricResults_odir')
    fname = '%s.sh'%(page_no) 
    #ph_data_path = os.path.join(ipath, str(doc_id), ph_odir, fname)
    ph_data_path = os.path.join(opath, str(doc_id), ph_odir, fname)
    if not os.path.exists(ph_data_path): 
        print >> sys.stderr, 'PH DATA not found! '
        return {}
    return return_data(ph_data_path)

def get_triplets_shelve(doc_id, page_no, level):
    ct_odir = cfgObj.get_config('PageAnalysis', 'CoverTriplets_odir')
    fname = '%s_%s.sh'%(str(page_no), level)  
    #rm_path = os.path.join(ipath, str(doc_id), ct_odir, fname)
    rm_path = os.path.join(opath, str(doc_id), ct_odir, fname)
    if not os.path.exists(rm_path):
        print >> sys.stderr, 'Triplet Shelve not found! ' 
        return []
    return return_data(rm_path)

def get_num_behave_shelve(doc_id, page_no):
    drm_odir = cfgObj.get_config('PageAnalysis', 'num_behave_odir')
    fname = '%s.sh'%(str(page_no))
    #rm_path = os.path.join(ipath, str(doc_id), drm_odir, fname)
    rm_path = os.path.join(opath, str(doc_id), drm_odir, fname)
    if not os.path.exists(rm_path):
        print >> sys.stderr, 'Number Behavior Shelve not found! ' 
        return []

    dd = datastore.read_data_fname(rm_path, isdb, isenc)
    data_dict = dd.get('data', {})
    return data_dict


def get_nonG_shelve(doc_id):
    drm_odir = cfgObj.get_config('applicator', 'TAS_Topic_Mapped_NonG')
    fname = '%s.sh'%(str(doc_id))
    #rm_path = os.path.join(ipath, str(doc_id), drm_odir, fname)
    rm_path = os.path.join(opath, str(doc_id), drm_odir, fname)
    if not os.path.exists(rm_path):
        print >> sys.stderr, 'Number Behavior Shelve not found! ' 
        return []

    dd = datastore.read_data_fname(rm_path, isdb, isenc)
    data_dict = dd.get('nong_data', {})
    return data_dict



def get_proj_rm(doc_id, page_no, level):
    drm_odir = cfgObj.get_config('PageAnalysis', 'projectedrm_odir')
    fname = '%s_%s.sh'%(str(page_no), level)
    #rm_path = os.path.join(ipath, str(doc_id), drm_odir, fname)
    rm_path = os.path.join(opath, str(doc_id), drm_odir, fname)
    print 'ppp',rm_path
    if not os.path.exists(rm_path):
        print >> sys.stderr, 'Projected RM not found! ' 
        return []

    dd = datastore.read_data_fname(rm_path, isdb, 0)
    data_dict = dd.get('data', {})
    return data_dict
    #return return_data(rm_path)

def get_synthe_dict(doc_id, page_no, level): 

    slt_odir = cfgObj.get_config('PageAnalysis', 'coverpagesynthesizer_odir')
    fname = '%s_%s.sh' %(page_no, level)
    #CellDictpath = os.path.join(ipath, str(doc_id), slt_odir, fname)
    CellDictpath = os.path.join(opath, str(doc_id), slt_odir, fname)
    #print CellDictpath
    if not os.path.exists(CellDictpath): 
        print >> sys.stderr, 'SLT not found! ' 
        return {}

    dd = datastore.read_data_fname(CellDictpath, isdb, 0)
    data_dict = dd.get('data', {})
    return data_dict



def get_slt_dict(doc_id, page_no, level): 

    slt_odir = cfgObj.get_config('PageAnalysis', 'slt_data_odir')
    fname = '%s_%s.sh' %(page_no, level)
    #CellDictpath = os.path.join(ipath, str(doc_id), slt_odir, fname)
    CellDictpath = os.path.join(opath, str(doc_id), slt_odir, fname)
    #print CellDictpath
    if not os.path.exists(CellDictpath): 
        print >> sys.stderr, 'SLT not found! ' 
        return {}

    #return return_data(CellDictpath)
    dd = datastore.read_data_fname(CellDictpath, isdb, isenc)
    return dd


def get_cell_dict(doc_id, page_no): 
    cpath = cfgObj.get_config('MOD_DIRNAME', 'celldict')
    fname = '%s.sh' %(page_no)
    CellDictpath = os.path.join(ipath, str(doc_id), cpath, fname)
    #print 'CELL DICT PATH : ', CellDictpath
    #sys.exit()
    if not os.path.exists(CellDictpath): 
        print >> sys.stderr, 'Cell dict not found! ', CellDictpath 
        return {}
    #sys.exit()
    #print CellDictpath
    shv = datastore.read_data_fname(CellDictpath, isdb, isenc)
    cell_dict = shv.get('cell_dict', {})
    #print cell_dict
    return cell_dict

def get_font_dict(doc_id, page_no): 
    cpath = cfgObj.get_config('MOD_DIRNAME', 'fontdict')
    fname = '%s.sh' %(page_no)
    #FontDictpath = os.path.join(ipath, str(doc_id), cpath, fname)
    FontDictpath = os.path.join(opath, str(doc_id), cpath, fname)
    if not os.path.exists(FontDictpath): 
        print >> sys.stderr, 'Font dict not found! ', FontDictpath 
        return {}
    #shv = datastore.read_data_fname(FontDictpath, isdb, isenc)
    shv = datastore.read_data_fname(FontDictpath, isdb, 0)
    font_dict = shv.get('font_dict', {})
    return font_dict



def get_num_grid(doc_id, page_no):
    cpath = cfgObj.get_config('MOD_DIRNAME', 'numgrid')
    fname = '%s.sh' %(page_no)
    #numpath = os.path.join(ipath, str(doc_id), cpath, fname)
    numpath = os.path.join(opath, str(doc_id), cpath, fname)
    if not os.path.exists(numpath): 
        print >> sys.stderr, 'NUM GRID dict not found! ', numpath 
        return {}
    shv = datastore.read_data_fname(numpath, isdb, isenc)
    num_dict = shv.get('data', {})
    return num_dict

def get_visual_group_dict(doc_id, page_no): 
    ci_odir = cfgObj.get_config('MOD_DIRNAME', 'visdict')
    fname = '%s.sh' %(page_no)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), ci_odir, fname)
    if not os.path.exists(shname): 
        print >> sys.stderr, 'Visual Group dict not found! ' 
        return {}

    shv = datastore.read_data_fname(shname, isdb, isenc)
    cell_info_dict = shv.get('vis_dict', {})
    return cell_info_dict


def write_cagr_shelve(doc_id, page_no, CellInfoDict, level): 
    ci_odir = cfgObj.get_config('PageAnalysis', 'cagr_result_odir')
    fname = '%s_%s.sh' %(page_no, level)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), ci_odir, fname)
    datastore.make_dirs(os.path.join(opath, str(doc_id), ci_odir))

    d = {}
    d['data'] = CellInfoDict
    datastore.write_data_fname(shname, isdb, isenc, d)
    return

def get_tok_indexing(doc_id, page_no, inkey, level): 
    fname = '%s_%s_%s.sh' %(page_no, inkey, level)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), "MDD", fname)
    print shname
    #print isdb, isenc
    #d = datastore.read_data_fname(shname, isdb, isenc)
    d = datastore.read_data_fname(shname, isdb, 0)
    if type(d) == type({}):  
       cellinfodict = d.get('data', {})
       return cellinfodict
    return {} 

def get_font_rm(doc_id, page_no): 
    fname = '%s.sh' %(page_no)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), "MDB", fname)

    #d = datastore.read_data_fname(shname, isdb, isenc)
    d = datastore.read_data_fname(shname, isdb, 0)
    cellinfodict = d.get('data', {})
    return cellinfodict

def get_base_igs(doc_id, page_no): 
    fname = '%s_TOK_HLPN.sh' %(page_no)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), "MDD", fname)
    #print shname
    #d = datastore.read_data_fname(shname, isdb, isenc)
    d = datastore.read_data_fname(shname, isdb, 0)
    cellinfodict = d.get('data', {})
    return cellinfodict





def save_font_rm(doc_id, page_no,  CellInfoDict): 
    fname = '%s.sh' %(page_no)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), "MDB", fname)

    d = {}
    d['data'] = CellInfoDict
    #datastore.write_data_fname(shname, isdb, isenc, d)
    datastore.write_data_fname(shname, isdb, 0, d)
    return

def save_hrvr_grps(doc_id, page_no, bbox_list, cell_list, font_dict, b_fc_grps_bbox, b_fc_grps_cells, font_chg_grp_cells, new_cells, sfc_signature_list, fc_signature_list, f_res_bbox_n, f_res_cells_n): 
    fname = '%s_HRVR.sh' %(page_no)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), "MDB", fname)

    d = {}
    d['bbox'] = bbox_list
    d['cell'] = cell_list
    d['font'] = font_dict

    d['bbox_unordered'] = b_fc_grps_bbox
    d['cell_unordered'] = b_fc_grps_cells


    d['fc_cells'] = new_cells
    d['fc_sig_dict_list'] = fc_signature_list
    d['sfc_cells'] = font_chg_grp_cells
    d['sfc_sig_dict_list'] = sfc_signature_list

    d['hrvr_cells']  = f_res_cells_n
    d['hrvr_bboxs']  = f_res_bbox_n

    #datastore.write_data_fname(shname, isdb, isenc, d)
    datastore.write_data_fname(shname, isdb, 0, d)
    return

def get_basic_hrvr_grps(doc_id, page_no): 
    fname = '%s_HRVR.sh' %(page_no)
    shname = os.path.join(opath, str(doc_id), "MDB", fname)
    #d = datastore.read_data_fname(shname, isdb, isenc)
    d = datastore.read_data_fname(shname, isdb, 0)
    bbox_list = d.get('hrvr_bboxs', {})
    cell_list = d.get('hrvr_cells', {})
    font_dict = d.get('font', {})
    return bbox_list, cell_list, font_dict

def get_hrvr_grps(doc_id, page_no): 
    fname = '%s_HRVR.sh' %(page_no)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), "MDB", fname)

    #d = datastore.read_data_fname(shname, isdb, isenc)
    d = datastore.read_data_fname(shname, isdb, 0)
    bbox_list = d.get('bbox', {})
    cell_list = d.get('cell', {})
    font_dict = d.get('font', {})
    return bbox_list, cell_list, font_dict

def get_hrvr_grps2(doc_id, page_no): 
    fname = '%s_HRVR.sh' %(page_no)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), "MDB", fname)

    #d = datastore.read_data_fname(shname, isdb, isenc)
    d = datastore.read_data_fname(shname, isdb, 0)
    bbox_list = d.get('bbox_unordered', {})
    cell_list = d.get('cell_unordered', {})
    font_dict = d.get('font', {})
    return bbox_list, cell_list, font_dict

def get_fc_grps(doc_id, page_no): 
    fname = '%s_HRVR.sh' %(page_no)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), "MDB", fname)

    #d = datastore.read_data_fname(shname, isdb, isenc)
    d = datastore.read_data_fname(shname, isdb, 0)

    fc_cells = d.get('fc_cells', [])
    fc_signature_list = d.get('fc_sig_dict_list', [])
    sfc_cells = d.get('sfc_cells', [])
    sfc_signature_list = d.get('sfc_sig_dict_list', [])

    return fc_cells, fc_signature_list, sfc_cells, sfc_signature_list





def save_indexing(doc_id, page_no,  CellInfoDict, inkey, level): 
    fname = '%s_%s_%s.sh' %(page_no, inkey, level)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), "MDD", fname)

    d = {}
    d['data'] = CellInfoDict
    #datastore.write_data_fname(shname, isdb, isenc, d)
    datastore.write_data_fname(shname, isdb, 0, d)
    return




def update_cell_info_dict_level(doc_id, page_no,  CellInfoDict, level): 
    ci_odir = cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
    fname = '%s_%s.sh' %(page_no, level)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), ci_odir, fname)

    d = {}
    d['cell_info_dict'] = CellInfoDict
    #datastore.write_data_fname(shname, isdb, isenc, d)
    datastore.write_data_fname(shname, isdb, 0, d)
    return




def update_cell_info_dict(doc_id, page_no,  CellInfoDict): 
    ci_odir = cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
    fname = '%s.sh' %(page_no)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), ci_odir, fname)

    d = {}
    d['cell_info_dict'] = CellInfoDict
    datastore.write_data_fname(shname, isdb, isenc, d)
    return

def is_cell_info_dict_exists(doc_id, page_no):
    ci_odir = cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
    fname = '%s.sh' %(page_no)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), ci_odir, fname)
    #print 'shname: ', shname   
    if os.path.exists(shname):
       d = datastore.read_data_fname(shname, isdb, isenc)
       if d.get('cell_info_dict', {}):
          return 1
    return 0 

def get_cell_info_dict_level(doc_id, page_no, level): 
    ci_odir = cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
    fname = '%s_%s.sh' %(page_no, level)
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), ci_odir, fname)
    if not os.path.exists(shname): 
        print >> sys.stderr, 'Cell INFO dict not found! ', shname
        return {}

    #shv = datastore.read_data_fname(shname, isdb, isenc)
    shv = datastore.read_data_fname(shname, isdb, 0)
    #print >> sys.stderr, "JJJJ", shname
    cell_info_dict = shv.get('cell_info_dict', {})
    return cell_info_dict


def get_cell_info_dict(doc_id, page_no): 
    ci_odir = "CID"#cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
    fname = '%s.sh' %(page_no)
    #print fname
    #shname = os.path.join(ipath, str(doc_id), ci_odir, fname)
    shname = os.path.join(opath, str(doc_id), ci_odir, fname)
    if not os.path.exists(shname): 
        print >> sys.stderr, 'Cell INFO dict not found! ' 
        return {}

    #print shname, isdb, isenc 
    shv = datastore.read_data_fname(shname, isdb, isenc)
    cell_info_dict = shv.get('cell_info_dict', {})
    return cell_info_dict

def get_cell_info_dict_update(doc_id, page_no,doc_page_dict):
    cell_info_dict = get_cell_info_dict(doc_id, page_no) 
    page_cord = ""
    for key, val in cell_info_dict.items():
        page_cord = val.get('page_rect', '')
        if page_cord: break     
    if page_cord:
        doc_page_dict[str(pageno)] = page_cord.split('_')    
    else:
        doc_page_dict[str(pageno)] = [0,0,0,0]


def write_fp_curr_result(doc_id, page_no, cell_info_dict):
    cid_odir = cfgObj.get_config('PageAnalysis', 'fp_curr_result')
    fname = '%s.sh' %(page_no)
    ofname = os.path.join(opath, str(doc_id), cid_odir, fname)
    d = {}
    d['data'] = cell_info_dict
    datastore.write_data_fname(ofname, isdb, isenc, d)
    return 

def get_fp_curr_result(doc_id, page_no):
    cid_odir = cfgObj.get_config('PageAnalysis', 'fp_curr_result')
    fname = '%s.sh' %(page_no)
    ofname = os.path.join(opath, str(doc_id), cid_odir, fname)
    d = datastore.read_data_fname(ofname, isdb, isenc)
    data = d.get('data', {})
    return data

def read_number_curr_result(doc_id, page_no):
    cid_odir = cfgObj.get_config('PageAnalysis', 'number_curr_result')
    fname = '%s.sh' %(page_no)
    ofname = os.path.join(opath, str(doc_id), cid_odir, fname)
    d = datastore.read_data_fname(ofname, isdb, isenc)
    cell_info_dict = d.get('data', {})
    return cell_info_dict


def write_number_curr_result(doc_id, page_no, cell_info_dict):
    cid_odir = cfgObj.get_config('PageAnalysis', 'number_curr_result')
    fname = '%s.sh' %(page_no)
    ofname = os.path.join(opath, str(doc_id), cid_odir, fname)
    d = {}
    d['data'] = cell_info_dict
    datastore.write_data_fname(ofname, isdb, isenc, d)
    return 

def get_number_curr_result(doc_id, page_no):
    cid_odir = cfgObj.get_config('PageAnalysis', 'number_curr_result')
    #print "number_curr_result path:", cid_odir
    fname = '%s.sh' %(page_no)
    ofname = os.path.join(opath, str(doc_id), cid_odir, fname)
    d = datastore.read_data_fname(ofname, isdb, isenc)
    data = d.get('data', {})
    return data


def write_cell_info_dict(doc_id, page_no, cell_info_dict):
    cid_odir = cfgObj.get_config('PageAnalysis', 'cell_info_dict_odir')
    fname = '%s.sh' %(page_no)
    ofname = os.path.join(opath, str(doc_id), cid_odir, fname)
    d = {}
    d['cell_info_dict'] = cell_info_dict
    datastore.write_data_fname(ofname, isdb, isenc, d)
    return 


def get_metadata_dict(docid):
    cid_odir = cfgObj.get_config('ExtractEntity', 'EntityOutput_odir')
    fname = '%s.sh' %(docid)
    ofname = os.path.join(opath, str(docid), cid_odir, fname)
    shv = datastore.read_data_fname(ofname, isdb, isenc)
    metadat_dict = shv.get('data', {})
    return metadat_dict

def write_ph_info_dict_level(doc_id, pno, ph_info_dict, level):
    ph_odir = cfgObj.get_config('PageAnalysis', 'ParametricResults_odir')
    fname = '%s_%s.sh' %(pno, level)

    #ofname = os.path.join(ipath, str(doc_id), ph_odir, fname)
    ofname = os.path.join(opath, str(doc_id), ph_odir, fname)
    datastore.rmfile(ofname)
    d = {}
    d['data'] = ph_info_dict
    datastore.write_data_fname(ofname, isdb, isenc, d)
    return


 
    
def write_ph_info_dict(doc_id, pno, ph_info_dict):
    ph_odir = cfgObj.get_config('PageAnalysis', 'ParametricResults_odir')
    fname = '%s.sh' %(pno)

    #ofname = os.path.join(ipath, str(doc_id), ph_odir, fname)
    ofname = os.path.join(opath, str(doc_id), ph_odir, fname)
    datastore.rmfile(ofname)
    d = {}
    d['data'] = ph_info_dict
    datastore.write_data_fname(ofname, isdb, isenc, d)
    return

def write_projected_rm_linll(doc_id, page_no, level, rm_lines):
    prm_odir = cfgObj.get_config('PageAnalysis', 'projectedrmlnill_odir')
    fpath = os.path.join(opath, str(doc_id), prm_odir, '')
    #os.system('mkdir -p %s' %fpath)
    datastore.make_dirs(fpath)
    filename = '%s_%s.sh' %(str(page_no), level)
    fname = os.path.join(fpath, filename)
    d = {}
    d['data'] = rm_lines[:]
    datastore.write_data_fname(fname, isdb, isenc, d)
    return

def read_projected_rm_linll(doc_id, page_no, level):
    prm_odir = cfgObj.get_config('PageAnalysis', 'projectedrmlnill_odir')
    fpath = os.path.join(opath, str(doc_id), prm_odir, '')
    #os.system('mkdir -p %s' %fpath)
    datastore.make_dirs(fpath)
    filename = '%s_%s.sh' %(str(page_no), level)
    fname = os.path.join(fpath, filename)
    data = datastore.read_data_fname(fname, isdb, isenc, d)
    return data.get('data', {})



def write_projected_rm(doc_id, page_no, level, rm_lines):
    prm_odir = cfgObj.get_config('PageAnalysis', 'projectedrm_odir')
    fpath = os.path.join(opath, str(doc_id), prm_odir, '')
    #os.system('mkdir -p %s' %fpath)
    datastore.make_dirs(fpath)
    filename = '%s_%s.sh' %(str(page_no), level)
    fname = os.path.join(fpath, filename)
    d = {}
    d['data'] = rm_lines[:]
    #datastore.write_data_fname(fname, isdb, isenc, d)
    datastore.write_data_fname(fname, isdb, 0, d)

    return

def write_projected_rm_file(doc_id, page_no, level, rm_lines):
    prm_odir = cfgObj.get_config('PageAnalysis', 'projectedrm_odir')
    fpath = os.path.join(opath, str(doc_id), prm_odir, '')
    #os.system('mkdir -p %s' %fpath)
    datastore.make_dirs(fpath)
    filename = '%s_%s.txt' %(str(page_no), level)
    fname = os.path.join(fpath, filename)

    f1 = open(fname, "w")
    for e in rm_lines:
        es = '\t'.join(e)
        f1.write('%s\n' %es)
    f1.close()

    return

def write_projected_rm_file_linll(doc_id, page_no, level, rm_lines):
    prm_odir = cfgObj.get_config('PageAnalysis', 'projectedrm_odir')
    fpath = os.path.join(opath, str(doc_id), prm_odir, '')
    #os.system('mkdir -p %s' %fpath)
    datastore.make_dirs(fpath)
    filename = '%s_%s_LinLL.txt' %(str(page_no), level)
    fname = os.path.join(fpath, filename)

    f1 = open(fname, "w")
    for e in rm_lines:
        es = '\t'.join(e)
        f1.write('%s\n' %es)
    f1.close()
    return

def get_relation_dict(doc_id, page_no):
    rel_odir = cfgObj.get_config('PageAnalysis', 'RelationResults_odir')
    fname = '%s.sh'%(str(page_no))
    #sh_path = os.path.join(ipath, str(doc_id), rel_odir, fname)
    sh_path = os.path.join(opath, str(doc_id), rel_odir, fname)
    if not os.path.exists(sh_path): 
        print >> sys.stderr, 'relation dict not found! ' 
        return {}

    dd = datastore.read_data_fname(sh_path, isdb, isenc)
    rd = dd.get('data', {})
    return rd

##################################################################
def read_rule_conf_file(plevel):
    fname = os.path.join(idatapath, '%s_rule_conf.txt' %plevel)
    lines = read_idata(fname)
    mydict = {}   
    for line in lines:
        line_sp = line.strip('\n').split('\t')
        if not mydict.get(line_sp[1], {}):
           mydict[line_sp[1]] = {} 
        mydict[line_sp[1]][line_sp[0]] = 1
    return mydict 
 


def get_projected_columns(level, r_type):
    fname = os.path.join(idatapath, 'projected_columns.txt')
    #lines = open(fname, 'r').readlines()
    lines = read_idata(fname)
    for line in lines:
        line_sp = line.strip('\n').split('\t')
        if line_sp[0] == level+":"+r_type:
           return map(lambda x:x.strip(), line_sp[1:])
    return []  
 
def read_token_simplify_file(level):
    fname = os.path.join(idatapath, 'token_simplify_%s.txt' %level)
    fname = fileabspath.filePathAdj().get_file_path(fname)
    #lines = open(fname, 'r').readlines()
    lines = read_idata(fname)
    
    d_dict = {} 
    for line in lines:
        line_sp = line.split('\t')
        d_dict[line_sp[0].strip()] = line_sp[1].strip()
    return d_dict     


def read_rule_selection_file(level):
    fname = os.path.join(idatapath, 'rule_igs_%s.txt' %level)
    fname = fileabspath.filePathAdj().get_file_path(fname)
    #lines = open(fname, 'r').readlines()
    lines = read_idata(fname)

    d_dict = {} 
    for line in lines:
        if not line.strip(): continue
        lsp = line.strip('\n').split('\t')
        d_dict[lsp[0].strip()] = []
        for l in lsp[1:]:
            if not l.strip(): continue
            d_dict[lsp[0].strip()].append(l.strip())

    return d_dict


def get_level_igs_projrm(level):
    level_igs = []
    fname = os.path.join(idatapath, 'token_controller_%s.txt' %level)
    fname = fileabspath.filePathAdj().get_file_path(fname)
    #lines = open(fname, 'r').readlines()
    lines = read_idata(fname)
    if not lines: return level_igs
    for line in lines[1:]:
        line = line.strip('\n')
        ls = line.split('\t')
        if ls[3].strip() == 'None': continue
        if str(level) == ls[0].strip(): 
            level_igs.append(ls[1:])
    return level_igs


def get_level_igs(level):
    level_igs = []
    fname = os.path.join(idatapath, 'token_controller_%s.txt' %level)
    fname = fileabspath.filePathAdj().get_file_path(fname)
    #lines = open(fname, 'r').readlines()
    lines = read_idata(fname)
    if not lines: return level_igs
    for line in lines[1:]:
        line = line.strip('\n')
        ls = line.split('\t')
        if ls[3].strip() == 'None': continue
        if 1:#str(level) == ls[0].strip(): 
            level_igs.append(ls[1:])
    return level_igs

def get_level_igs_user(level, user):
    level_igs = []
    conf_path = cfgObj.get_config('PageAnalysis', 'rule_file_path')
    fname = os.path.join(conf_path, user, 'token_controller_%s.txt' %level)
    #fname = os.path.join(idatapath, 'token_controller_%s.txt' %level)
    fname = fileabspath.filePathAdj().get_file_path(fname)
    #lines = open(fname, 'r').readlines()
    lines = read_idata(fname)
    if not lines: return level_igs
    for line in lines[1:]:
        line = line.strip('\n')
        ls = line.split('\t')
        if ls[3].strip() == 'None': continue
        if 1:#str(level) == ls[0].strip(): 
            level_igs.append(ls[1:])
    return level_igs

