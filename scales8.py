import spss
import spssaux
import os
import pandas as pd
import numpy as np
import sys
import timeit

########GLOBAL VARS##########
error_search = 0
possible_paths = [R'C:\Users\stupg\Desktop', R'C:\Users\Jon\Desktop', R'C:\Users\jonaur\Desktop']
for path in possible_paths:
    if os.path.exists(path):
        folder = path


os.chdir(folder+'\Jon\LUST\out')
data = folder+'\Jon\LUST\COMPLETE_recoded.sav'
data2 = folder+'\Jon\LUST\COMPLETE_RECODED_BINARY.sav'
input_scale_csv = folder+'\Jon\Lust\input_scale.csv'
#save_data = '01_middle_cut_mod.sav'

prefix, start, stop = 'x6_', 2, 5
version = 'suggestion_03'
single_items = 'single_item_02'
cut_mod = 'cut_mod_05'
single_items_cut_mod = 'cutmod_single_item_02'
db_glob = pd.DataFrame.from_csv(input_scale_csv)
db_glob = db_glob.loc[db_glob[version].notnull() | db_glob[single_items].notnull()]

save_data = version + "_" + cut_mod + "_imputed" + ".sav"

columns_to_use = [version,single_items]
cut_mods = [cut_mod]

########GLOBAL VARS##########


def main():
    spssaux.OpenDataFile(data)
    spss.SetOutput("off")
    spss.Submit('SET TNumbers=Values ONumbers=Labels OVars=Labels.')
    spss.Submit('SORT CASES  BY Imputation_.\nSPLIT FILE LAYERED BY Imputation_.')
    db_prefixed = mod_database(db_glob,prefix,start,stop)
    get_median_cut()
    input_indep = db_prefixed.query('dependent != 1')
    spss.Submit(dicotomize(input_indep))
    #spss.SetOutput('on')
    spss.Submit(make_analysis_exposure(input_indep))
    spss.Submit("SAVE OUTFILE='COMPLETE_RECODED_BINARY.sav'\n/COMPRESSED.")
    outdb,outdb_err = calculate_odds(db_prefixed,start,stop,prefix)
    outdb.to_csv('{version}_{cutmod}.csv'.format(version=version,cutmod=cut_mod))
    outdb_err.to_csv('error_{version}_{cutmod}.csv'.format(version=version, cutmod=cut_mod))
    #save_output(outdb)
    spss.Submit("SAVE OUTFILE='%s'\n/COMPRESSED." % save_data)


def save_output(db):

    for time in range(start,stop):
        tid = prefix + str(time)
        print(tid)
        db_out = db.query('time == "{tid}"'.format(tid=tid))
        db_out.to_csv('{tid}.csv'.format(tid=tid))


def dicotomize(db):
    sdict = spssaux.VariableDict()
    cmd = ''
    for typ in columns_to_use:
        if typ == version:
            mod_cut = cut_mod
            suffix = '_middle'
        if typ == single_items:
            mod_cut = single_items_cut_mod
            suffix = '_single'
        for var in db['items']:
            mod_val = db.loc[db['items'] == var, mod_cut ]
            mod = 0
            if pd.notnull(mod_val.iloc[0]):
                mod = int(mod_val.iloc[0])
            key_list = []
            labels = sdict[var].ValueLabels
            for key in labels:
                if 0 < int(key) < 77:
                    key_list.append(int(key))
            min_val,max_val = min(key_list),max(key_list)
            if max_val - min_val % 2 == 1:  # if even
                limit = max_val / 2
            else:                           # if odd
                limit = (max_val + 1 )/2
            if limit:
                limit = limit + mod
                cmd += 'RECODE {old_var} (1 thru {low_lim}=0) ({high_lim} thru 76=1) INTO {new_var}.\n'.format(low_lim=limit,high_lim=limit+1,old_var=var,new_var=var+suffix)
                if 'x6_2occdev1' == var:
                    print(var)
    cmd += 'EXECUTE.\n'
    print(cmd)
    return cmd


def make_analysis_exposure(db):
    cmd = ''
    for typ in columns_to_use:
        if typ == version:
            suffix = '_middle'
        if typ == single_items:
            suffix = '_single'
        for typ in columns_to_use:
            db2 = db.loc[db[typ].notnull()]
            for cell in db2[typ].unique():
                db2 = db.query('%s == "%s"' % (typ,cell))
                lista = [x + suffix for x in db2['items'].values.tolist()]
                cmd += 'COMPUTE %s=%s.\n' % (cell+suffix,'+'.join(lista))
        cmd += 'EXECUTE.\n'
    return cmd


def calculate_odds(db_prefixed,start,stop,prefix):
    cmd_recode = ''
    cmd_log_reg = ''
    db_list = []
    db_err_list = []
    #input_indep = db_glob.query('dependent != 1')
    input_dep = db_glob.query('dependent == 1')
    dep_list = get_median_cut()
    n = 0
    for time in range(start,stop):
        dep_list_time = [prefix+str(time)+item[4:] for item in dep_list]
        myset = set(dep_list_time)
        dep_list_time = list(myset)
        input_indep = db_prefixed.query('dependent != 1 and time == "%s%s"' % (prefix,str(time)))
        for typ in columns_to_use:
            if typ == version:
                suffix = '_middle'
            if typ == single_items:
                suffix = '_single'
            for indep in input_indep[typ].unique():
                if str(indep) != 'nan':
                    indep = indep + suffix
                    cutoffs = list_of_cut(indep)
                    for dep in dep_list_time:
                        for x in range(len(cutoffs) - 1):
                            if n >= error_search:

                                #cmd_log_reg += recode_cut(indep,cutoffs,x)
                                #cmd_log_reg += log_reg_cmd(dep,indep)

                                spss.Submit(recode_cut(indep,cutoffs, x))
                                db, db_err = log_reg(dep,indep,cutoffs[x+1],n)
                                db_list.append(db)
                                db_err_list.append(db_err)

                                # spss.Submit(odds(dep, indep))
                                #db_list.append(log_reg(dep,indep,cutoffs[x+1],n))
                            n += 2
                        #db_list.append(find_value(indep,dep,prefix+str(time)))

    db = pd.concat(db_list)
    db_err = pd.concat(db_err_list)
    #print(cmd_log_reg)
    return db, db_err

def log_reg_cmd(dep,indep):
    cmd = ("""
    CROSSTABS
      /TABLES={dep}_cut BY {indep}_cut
      /FORMAT=AVALUE TABLES
      /CELLS=COUNT
      /COUNT ROUND CELL.
    LOGISTIC REGRESSION VARIABLES {dep}_cut
      /METHOD=ENTER {indep}_cut
      /PRINT=CI(95)
      /CRITERIA=PIN(.05) POUT(.10) ITERATE(20) CUT(.5).
""".format(dep=dep, indep=indep))
    return cmd



def log_reg(dep,indep,cutoff,n):
    columns = ['dep','indep','cutoff','RR','RR_low','RR_high','SE','_00','_01','_10','_11','pool','%dep','%indep']
    db = pd.DataFrame(columns=columns)
    db_err = pd.DataFrame(columns=columns)
    for x in range(0, 2):
        db.loc[n + x] = np.nan
        db.loc[n + x]['pool'] = 0 + x
        for key, val in {'dep': dep, 'indep': indep[:-7], 'cutoff': cutoff}.items():
            db.loc[n + x][key] = val
        #########for exceptions
        db_err.loc[n + x] = np.nan
        db_err.loc[n + x]['pool'] = 0 + x
        for key, val in {'dep': dep, 'indep': indep[:-7], 'cutoff': cutoff}.items():
            db_err.loc[n + x][key] = val

    cmd = """OMS SELECT TABLES
/DESTINATION FORMAT=OXML XMLWORKSPACE='log_table'.
CROSSTABS
  /TABLES={dep}_cut BY {indep}_cut
  /FORMAT=AVALUE TABLES
  /CELLS=COUNT
  /COUNT ROUND CELL.
LOGISTIC REGRESSION VARIABLES {dep}_cut
  /METHOD=ENTER {indep}_cut
  /PRINT=CI(95)
  /CRITERIA=PIN(.05) POUT(.10) ITERATE(20) CUT(.5).
OMSEND.""".format(dep=dep,indep=indep)
    #print(cmd)
    spss.Submit(cmd)
    context = "/outputTree"
    xpath_0_cells = "//pivotTable[@subType='Crosstabulation']//category[@varName='Imputation_' and @text='0']" \
            "//group[@text='{dep}']//category[@varName='{indep}']//cell/@number".format(dep=dep+'_cut',indep=indep+'_cut')
    xpath_pool_cells = "//pivotTable[@subType='Crosstabulation']//category[@text='Pooled']" \
            "//group[@text='{dep}']//category[@varName='{indep}']//cell/@number".format(dep=dep + '_cut',
                                                                                        indep=indep + '_cut')
    xpath_0_log_reg = "//command[@command='Logistic Regression']//pivotTable[@subType='Variables in the Equation']" \
                      "//category[@label='Original data']//category[@text='{indep}']" \
                      "//category[@text='S.E.' or @text='Exp(B)' or @text='Lower' or @text='Upper']//cell/@number".format(indep=indep+'_cut')
    xpath_pool_log_reg = "//command[@command='Logistic Regression']//pivotTable[@subType='Variables in the Equation']" \
                      "//category[@text='Pooled']//category[@text='{indep}']" \
                      "//category[@text='S.E.' or @text='Exp(B)' or @text='Lower' or @text='Upper']//cell/@number".format(indep=indep + '_cut')

    try:
        _0_cells = [float(x) for x in spss.EvaluateXPath('log_table', context, xpath_0_cells)]
        pooled_cells = [float(x) for x in spss.EvaluateXPath('log_table', context, xpath_pool_cells)]
    except Exception as ex:
        print(n)
        print(type(ex).__name__)
        print(ex.args)
        db_err.loc[n]['_00'] = spss.EvaluateXPath('log_table', context, xpath_0_cells)
        db_err.loc[n+1]['_00'] = spss.EvaluateXPath('log_table', context, xpath_pool_cells)
    try:
        _0_log_reg = [float(x) for x in spss.EvaluateXPath('log_table', context, xpath_0_log_reg)]
        pool_log_reg = [float(x) for x in spss.EvaluateXPath('log_table', context, xpath_pool_log_reg)]
    except Exception as ex:
        print(n)
        print(type(ex).__name__)
        print(ex.args)
        try:
            _0_log_reg = spss.EvaluateXPath('log_table', context, xpath_0_log_reg)
            pool_log_reg = spss.EvaluateXPath('log_table', context, xpath_pool_log_reg)
            db_err.loc[n]['RR'] = _0_log_reg
            db_err.loc[n + 1]['RR'] = pool_log_reg
        except Exception as ex:
            print(n)
            _0_log_reg = []
            pool_log_reg = []
    try:
        db.loc[n]['_00','_01','_10','_11'] = _0_cells[0],_0_cells[1],_0_cells[2],_0_cells[3]
        db.loc[n]['SE','RR','RR_low','RR_high'] = _0_log_reg[0],_0_log_reg[1],_0_log_reg[2],_0_log_reg[3]
    except Exception as ex:
        print(n)
        print(type(ex).__name__)
        print(ex.args)
        db_err.loc[n]['_01'] = _0_cells
        db_err.loc[n]['SE'] = _0_log_reg
    try:
        db.loc[n+1]['_00', '_01', '_10', '_11'] =  pooled_cells[0], pooled_cells[1], pooled_cells[2], pooled_cells[3]
        db.loc[n+1]['SE','RR','RR_low','RR_high'] = pool_log_reg[0],pool_log_reg[1],pool_log_reg[2],pool_log_reg[3]
    except Exception as ex:
        print(n)
        print(type(ex).__name__)
        print(ex.args)
        db_err.loc[n+1]['01'] = pooled_cells
        db_err.loc[n+1]['SE'] = pool_log_reg

    db['%dep'] = db[['_10','_11']].sum(axis=1)/db[['_00', '_01', '_10', '_11']].sum(axis=1)
    db['%indep'] = db[['_01','_11']].sum(axis=1) / db[['_00', '_01', '_10', '_11']].sum(axis=1)
    if n % 100 == 0 or (n + 1) % 100 == 0:
        print(n)
    return db,db_err


def log_reg_text(dep,indep,cutoff,n):

    columns = ['dep','indep','cutoff','RR','RR_low','RR_high','"00"','"01"','"10"','"11"','pool','%dep','%indep']
    db = pd.DataFrame(columns=columns)

    spss.SetOutput("ON")
    temp = sys.stdout
    sys.stdout = open('log.txt', 'w')
    spss.Submit("""
CROSSTABS
  /TABLES={dep} BY {indep}
  /FORMAT=AVALUE TABLES
  /CELLS=COUNT
  /COUNT ROUND CELL.

LOGISTIC REGRESSION VARIABLES {dep}_cut
  /METHOD=ENTER {indep}_cut
  /PRINT=CI(95)
  /CRITERIA=PIN(0.05) POUT(0.10) ITERATE(20) CUT(0.5).
""".format(dep=dep,indep=indep))

    sys.stdout.close()
    sys.stdout = temp
    spss.SetOutput("off")

    with open('log.txt','r') as indata:
        allrows = [r for r in indata]
        linenr = -1
        class_flagg = 0
        for x in range(0,2):
            db.loc[n+x] = np.nan
            db.loc[n+x] = np.nan
            db.loc[n+x]['pool'] = 0+x
            for key,val in {'dep':dep,'indep':indep[:7],'cutoff':cutoff}.items():
                db.loc[n+x][key] = val

        for row in allrows:
            linenr += 1
            if 'Classification Table(a)' in row:
                class_flagg = 1
            if class_flagg == 1:
                if '|0         |Step|' in row:
                    db.loc[n]['"00"'] = extract(0, 5, allrows, linenr, num=1)
                    db.loc[n]['"01"'] = extract(0, 6, allrows, linenr, num=1)
                    db.loc[n]['"10"'] = extract(2, 5, allrows, linenr, num=1)
                    db.loc[n]['"11"'] = extract(2, 6, allrows, linenr, num=1)
                    class_flagg += 1
            if class_flagg == 2:
                if 'Variables in the Equation' in row:
                    class_flagg += 1
            if class_flagg == 3:
                if '|0         |Step|' in row:
                    db.loc[n]['SE'] = extract(0, 5, allrows, linenr, num=1)
                    db.loc[n]['RR'] = extract(0, 9, allrows, linenr, num=1)
                    db.loc[n]['RR_low'] = extract(0, 10, allrows, linenr, num=1)
                    db.loc[n]['RR_high'] = extract(0, 11, allrows, linenr, num=1)
                if '|Pooled    |Step|' in row:
                    db.loc[n + 1]['SE'] = extract(0, 5, allrows, linenr, num=1)
                    db.loc[n + 1]['RR'] = extract(0, 9, allrows, linenr, num=1)
                    db.loc[n + 1]['RR_low'] = extract(0, 10, allrows, linenr, num=1)
                    db.loc[n + 1]['RR_high'] = extract(0, 11, allrows, linenr, num=1)
                    class_flagg += 1
    for x in range(0,2):
        db.loc[n+x]['%dep'] = db.loc[n+x][sum["00","01","10","11"]]
    print(db)
    return db


def get_median_cut():
    cmd = ''
    cmd2 = ''

    input_dep = db_glob[db_glob['dependent'] == 1 & db_glob[version].notnull()]
    for dep in input_dep[version].unique():
        db = input_dep['items'][input_dep[version] == dep]
        for time in range(start,stop):
            list_of_dep_items = [prefix + str(time) + var for var in db]
            cmd += 'COMPUTE {var}=mean.1({var_list}).\n'.format(var=prefix+str(time)+dep,var_list=','.join(list_of_dep_items))
    cmd += 'EXECUTE.\n'
    #print(cmd)
    spss.Submit(cmd)
    list_of_dep = [prefix + str(start) + dep for dep in input_dep[version].unique()]
    tag, err = spssaux.CreateXMLOutput("""FREQUENCIES VARIABLES={vars}
        /STATISTICS=MEDIAN.\n""".format(vars=' '.join(list_of_dep)))
    spss.GetXmlUtf16(tag,folder+'/fest.xml')
    context = "/outputTree"
    xpath = "//category[@text='Median']/dimension[@text='Variables']/category[@variable='true']//@*[name()='number' or name()='varName']"
    median_list = spss.EvaluateXPath(tag, context, xpath)
    median_list = [[prefix + str(i) + median_list[n][4:],median_list[n+1]] for i in range(start,stop) for n in range(0,len(median_list),2)]
    for sublist in median_list:
        cmd2 += 'RECODE {var} (Lowest thru {mean}=0) (sysmis,77,88,99,0=sysmis) (else=1) into {var}_cut.\n'.format(var=sublist[0],mean=sublist[1])
    cmd2 += 'EXECUTE.\n'
    #print(cmd2)
    spss.Submit(cmd2)
    return list_of_dep


def recode_dep_cut(dictionary,prefix):
    cmd = 'RECODE '
    cmd = 'RECODE {var} (Lowest thru {low}=0) ({high} thru Highest=1) INTO {var}_cut.\n'.format(var=prefix+dictionary['scale'],low=dictionary['low_cut'],high=dictionary['high_cut'])
    return cmd


def mod_database(input_scale,prefix,start,stop):
    vars_in_file = spssaux.VariableDict().Variables
    df_dict = {}
    for i in range(start,stop):
        df = input_scale.reset_index()
        df['time'] = prefix+str(i)
        for col in columns_to_use +['items']:
            df[col] = prefix + str(i) + df[col]
        df_dict[i] = df
    db = pd.concat(df_dict,ignore_index=True)
    db = db[db['items'].isin((vars_in_file))]
    return db


def char_limit(scale):
    cmd = 'TITLE'
    length = 'TITLE'
    for var in scale:
        length += var
        if len(length) < 60:
            cmd += ' ' + var
        else:
            length = 'TITLE'
            cmd += '.\nTITLE'
    return cmd


def factor_analysis(cell,db):
    cmd = ''
    lista = db['items'].values.tolist()
    scale_vars = char_limit(lista)
    db = db.dropna(subset=['subscale'])
    n_factor = len(db['subscale'].unique())
    if n_factor == 0:
        n_factor = 1
    cmd += """TITLE {title_name}.
    {title_vars}.
    FACTOR
        /VARIABLES {vars}  /MISSING LISTWISE
        /ANALYSIS {vars}  /PRINT INITIAL EXTRACTION KMO REPR AIC ROTATION
        /PLOT EIGEN
        /CRITERIA FACTORS({factor_n}) ITERATE(25)
        /EXTRACTION PAF
        /ROTATION OBLIMIN
        /METHOD=CORRELATION.\n""".format(title_name=cell,vars=' '.join(lista),factor_n=n_factor,title_vars=scale_vars)
    return cmd


def reliability_check(cell,db):
    cmd = ''
    lista = db['items'].values.tolist()
    scale_vars = char_limit(lista)
    cmd += """TITLE {title_name}.
            {title_vars}.
            RELIABILITY
            /VARIABLES={vars}
            /SCALE('ALL VARIABLES')  ALL/MODEL=ALPHA
            /STATISTICS=CORR
            /SUMMARY=TOTAL CORR . \n""".format(title_name=cell,title_vars=scale_vars,vars=' '.join(lista))
    return cmd


def odds(dep,indep):
    cmd = """CROSSTABS
  /TABLES=%s_cut BY %s_cut
  /FORMAT=AVALUE TABLES
  /STATISTICS=RISK
  /CELLS=COUNT ROW
  /COUNT ROUND CELL.""" % (indep,dep)
    return cmd


def recode_cut(indep,cutoff,x):
    #print('cutoff=%s' % cutoff[x + 1])
    cmd = 'RECODE %s (Lowest thru %s=0) (%s thru Highest=1) INTO %s_cut.\nEXECUTE.\n' % (indep,cutoff[x],cutoff[x+1],indep)
    return cmd


def extract(row, column, allrows, linenr, clean='', separator='|', num=0):
    theline = allrows[linenr + row]
    info = theline.split(separator)
    info = info[column].replace(',','.')
    info = info.replace(clean,'')
    if num == 1:
        try:
            info = float(info)
        except:
            info = np.nan
    return info


def find_value(ind,dep,time):
    n = 0
    columns = ['dep','indep','cutoff','value','lower','upper','00','01','10','11','time']
    db_dict = {}
    db = pd.DataFrame(columns=columns)
    with open("log.txt", "rb") as indata:
        allrows = []
        for line in indata:
            allrows.append(line)
        linenr = -1
        for row in allrows:
            linenr = linenr + 1
            row_db = pd.DataFrame
            if 'cutoff=' in row:
                text,cutoff = row.split('=')
            if 'Crosstabulation' in row:
                cell00=extract(7,4,allrows,linenr,' ','|',1)
                cell01 = extract(7, 5, allrows, linenr, ' ','|',1)
                cell10=extract(11,4,allrows,linenr,' ','|',1)
                cell11 = extract(11, 5, allrows, linenr, ' ','|',1)
            if '|Odds Ratio for ' in row:
                inlist = []
                try:
                    empty,text,value,lower,upper,empty = row.split('|')
                    for val in [cutoff,value,lower,upper]:
                        val = val.replace(' ','')
                        val = val.replace(',','.')
                        val = val.strip('\r\n')
                        val = float(val)
                        inlist.append(val)
                    db.loc[n] = dep,ind,inlist[0],inlist[1],inlist[2],inlist[3],cell00,cell01,cell10,cell11,time

                    n += 1
                except:
                    pass
    db['n_proc_dep'] = (db['01']+db['11'])/(db['00']+db['01']+db['10']+db['11'])
    db['n_proc_indep'] = (db['10']+db['11'])/(db['00']+db['01']+db['10']+db['11'])
    #db['n_proc_indep'] = db['11']/(db['10']+db['11'])
    return db

####################################################################################
def list_of_cut(indep):
    context = '/outputTree'
    xpath = '//pivotTable[@subType="Frequencies"]//group[@text="Valid"]//category/@number'
    spss.Submit('SET TNumbers=Values ONumbers=Labels OVars=Labels.')
    tag, err = spssaux.CreateXMLOutput('FREQUENCIES VARIABLES={}.'.format(indep))
    cut_list = spss.EvaluateXPath(tag, context, xpath)
    spss.DeleteXPathHandle(tag)
    return sorted(list((set(cut_list))))


def list_of_cut_old(indep):
    temp = sys.stdout
    sys.stdout = open('descript.txt', 'w')
    spss.Submit("""FREQUENCIES VARIABLES=%s
    /STATISTICS=MINIMUM MAXIMUM""" % indep)
    sys.stdout.close()
    sys.stdout = temp
    with open('descript.txt','r') as indata:
        valid_flag = 0
        listaallarader = []
        cut_vals = []
        for line in indata:
            listaallarader.append(line)
        for row in listaallarader:
            if 'Valid' in row:
                valid_flag = valid_flag +1
            if '__' not in row and valid_flag == 3:
                if 'Total' in row:
                    valid_flag = valid_flag+1
                    break
                e,e,value,e2,e,e,e,e = row.split('|')
                value = value.replace(',','.')
                value = value.replace(' ','')
                cut_vals.append(value)
        return cut_vals


if __name__ == '__main__':
    main()