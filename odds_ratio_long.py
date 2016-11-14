import spss
import spssaux
import os
import pandas as pd
import numpy as np
import sys
import timeit

########GLOBAL VARS##########

possible_paths = [R'C:\Users\stupg\Desktop', R'C:\Users\Jon\Desktop']
for path in possible_paths:
    if os.path.exists(path):
        folder = path


os.chdir(folder+'\Jon\LUST\out')
data = folder+'\Jon\LUST\long_vars.sav'
input_exposure_vars_csv = folder+'\Jon\Lust\input_exposure_vars.csv'
save_data = 'long_vars.sav'

prefix, start, stop = 'x6_', 2, 5
db_glob = pd.DataFrame.from_csv(input_exposure_vars_csv).reset_index()
db_glob = db_glob.loc[db_glob['indep'].notnull()]


########GLOBAL VARS##########

def main():
    spssaux.OpenDataFile(data)
    #spss.SetOutput("off")
    spss.Submit('SET TNumbers=Values ONumbers=Labels OVars=Labels.')
    outdb = calculate_odds(start,stop,prefix)
    outdb.to_csv('long_vars_ut.csv')


def calculate_odds(start,stop,prefix):
    db_list = []
    dep_list = ["x6_4jobsat_cut","x6_4orgcom_cut","x6_4work_priv_cut","x6_4intent_to_leave_yrke_cut","x6_4intent_to_leave_arbetsplats_cut"]
    for var in db_glob['indep']:
        for t in ["at_least_once","first","last","all","not_last","not_first"]:
            indep = var[:-7]+"_"+t
            for dep in dep_list:
                temp = sys.stdout
                sys.stdout = open('log.txt', 'w')
                #print(odds(dep,indep))
                spss.Submit(odds(dep, indep))
                sys.stdout.close()
                sys.stdout = temp
                db_list.append(find_value(indep,dep))
    db = pd.concat(db_list)
    return db


def find_value(ind,dep):
    n = 0
    columns = ['dep','indep','value','lower','upper','00','01','10','11']
    db = pd.DataFrame(columns=columns)
    with open("log.txt", "rb") as indata:
        allrows = []
        for line in indata:
            allrows.append(line)
        linenr = -1
        for row in allrows:
            linenr = linenr + 1
            if 'Crosstabulation' in row:
                cell00=extract(7,4,allrows,linenr,' ','|',1)
                cell01 = extract(7, 5, allrows, linenr, ' ','|',1)
                cell10=extract(11,4,allrows,linenr,' ','|',1)
                cell11 = extract(11, 5, allrows, linenr, ' ','|',1)
            if 'Odds Ratio for ' in row:
                inlist = []
                try:
                    empty,text,value,lower,upper,empty = row.split('|')
                    for val in [value,lower,upper]:
                        val = val.replace(' ','')
                        val = val.replace(',','.')
                        val = val.strip('\r\n')
                        val = float(val)
                        inlist.append(val)
                    db.loc[n] = dep,ind,inlist[0],inlist[1],inlist[2],cell00,cell01,cell10,cell11
                    n += 1
                except:
                    pass
    db['n_proc_dep'] = (db['01']+db['11'])/(db['00']+db['01']+db['10']+db['11'])
    db['n_proc_indep'] = (db['10']+db['11'])/(db['00']+db['01']+db['10']+db['11'])
    return db


def extract(row, column, allrows, linenr, clean, separator, num):
    theline = allrows[linenr + row]
    info = theline.split(separator)
    info = info[column].replace(',','.')
    info = info.replace(clean,'')
    if num == 1:
        info = float(info)
    return info



def odds(dep,indep):
    cmd = """CROSSTABS
  /TABLES=%s BY %s
  /FORMAT=AVALUE TABLES
  /STATISTICS=RISK
  /CELLS=COUNT ROW
  /COUNT ROUND CELL.""" % (indep,dep)
    return cmd

if __name__ == '__main__':
    main()