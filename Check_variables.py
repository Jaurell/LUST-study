__author__ = 'Jon'
"""
Checks availability of variables in dataset and makes new dataset from selection variable and list of variables.
"""
import  os
import spss
import spssaux
import itertools
import csv
import pandas as pd
import numpy as np

### GLOBAL VARS ###
possible_paths = [R'C:\Users\stupg\Desktop', R'C:\Users\Jon\Desktop', R'C:\Users\jonaur\Desktop']
for path in possible_paths:
    if os.path.exists(path):
        folder = path
os.chdir(folder + '\Jon\LUST\out')
#data = folder + '\Jon\LUST\EX2006_long_v3EF_ID.sav' #X6
data = R'C:\Users\jonaur\Desktop\Jon\LUST\data\EX2004_long_v10EF.sav' #X4
input_csv = folder + '\Jon\LUST\input_scale.csv'
save_data = 'EX2004_select_rev.sav'
missing = ['0','77','88','99']
prefix, start, stop = 'x4_', 4, 9
urvalsinfo = 'svar_1', '1'
version = 'suggestion_03'
single_items = 'single_item_02'
columns_to_use = [version,'rename','items']
#keep_vars = ['ID','x6_1kon'] #X6
keep_vars = ['Kon','EX2004_id'] #X4


### GLOBAL VARS ###


def main():
    spssaux.OpenDataFile(data)
    spss.SetOutput("off")
    db = pd.DataFrame.from_csv(input_csv)
    db2 = db.loc[db[version].notnull() | db[single_items].notnull() | db['rename'].notnull() | db['recode'].notnull()]
    #print(db2['items'].values.tolist())
    vars = mod_database(db2,prefix,start,stop)
    spss.Submit(prepare_data(vars))
    list_of_all_vars = vars['items'].values.tolist()
    vars_in_file = spssaux.VariableDict().Variables
    print(str(len(list_of_all_vars))+' variables extracted.')
    how_often(vars,vars_in_file,prefix,start,stop)
    reverse_items(vars)

    spss.Submit(extract_vars(list_of_all_vars,urvalsinfo,save_data,keep_vars))


def recode(db):
    cmd = ''
    db2 = db.loc[db['recode'].notnull()]
    cmd += '\n'.join(['RECODE {var} {recode}.'.format(var=pos[0],recode=pos[1]) for pos in db2[['items','recode']].values.tolist()])
    cmd += '\nEXECUTE.\n'
    #print(cmd)
    return(cmd)


def prepare_data(db):
    cmd = ''
    list_of_vars = db['items'].values.tolist()
    for var in list_of_vars:
        cmd += "MISSING VALUES %s (0,77 thru HI).\n" % var
        cmd += "VARIABLE ALIGNMENT %s (right).\n" % var
        cmd += "VARIABLE LEVEL %s (scale).\n" % var
    return(cmd)


def mod_database(input_scale,prefix,start,stop):
    vars_in_file = spssaux.VariableDict().Variables
    df_dict = {}
    for i in range(start,stop):
        df = input_scale.reset_index()
        df['time'] = prefix+str(i)
        for col in columns_to_use:
            df[col] = prefix + str(i) + df[col]
        df_dict[i] = df
    db = pd.concat(df_dict,ignore_index=True)
    diff_vars = db['items'][~db['items'].isin(vars_in_file)].values.tolist()
    print('Variables not in file\n'+'\n'.join(diff_vars)+'\n')

    db2 = db[db['items'].isin((vars_in_file))]
    spss.Submit(recode(db2))
    rename_db = db2.loc[db['rename'].notnull(),['items','rename']]
    for i,row in rename_db.iterrows():
        orgname = row['items']
        new_name = row['rename']
        spss.Submit('RENAME VARIABLES {orgname}={new_name}.\nEXECUTE.'.format(orgname=orgname,new_name=new_name))
    vars_in_file = spssaux.VariableDict().Variables
    diff_vars = db['items'][~db['items'].isin(vars_in_file)].values.tolist()
    print('Variables not in file\n'+'\n'.join(diff_vars)+'\n')
    db = db[db['items'].isin((vars_in_file))]
    return db


def reverse_items(db):
    cmd = ''
    reverse_list = db.loc[db['reverse'] == 1,['items']].values.tolist()
    n = 0
    sdict = spssaux.VariableDict()
    for item in reverse_list:
        n += 1
        cmd += get_reverse(item[0],sdict)
    spss.Submit(cmd)
    print(str(n)+' items have been reversed.')


def get_reverse(var,sdict):
    cmd = ''
    reversed_lables = {}
    try:
        labels = sdict[var].ValueLabels
    except:
        print(var)
    key_list = []
    for key in labels:
        if 0 < int(key) < 77:
            key_list.append(int(key))
    try:
        sub_val = max(key_list) + 1
    except:
        print(var)
        print(sub_val,max(key_list))

    for label in labels:
        if label not in missing:
            rev_val = sub_val - int(label)
            reversed_lables[str(rev_val)] = labels[label]
        else:
            reversed_lables[label] = labels[label]
    cmd += get_recode_rev(reversed_lables, var, sub_val)
    return cmd


def get_recode_rev(dictionary,var,sub_val):
    cmd = 'RECODE %s' % var
    for i in dictionary:
        if i not in missing:
            rev_val = sub_val - int(i)
            cmd += ' (%s=%s)' % (i,str(rev_val))
    cmd += '.\nEXECUTE.\n'
    cmd += get_value_label(dictionary,var)
    return cmd


def get_value_label(dictonary,var):
    cmd = 'VALUE LABELS\n%s\n' % var
    for i in dictonary:
        cmd += i + ' ' + "'%s'" % dictonary[i]
        cmd += ' '
    cmd += '.\n'
    return cmd


def extract_vars(vars,urvalsinfo,save,keep):
    cmd ="""FILTER OFF.
    USE ALL.

    *SELECT IF ({urvals_var} = {urvals_val}).

    EXECUTE.
    SAVE OUTFILE='{save}'
    /KEEP={keep} {all_vars}
    /COMPRESSED.""".format(save=save,urvals_var=urvalsinfo[0],urvals_val=urvalsinfo[1],all_vars=' '.join(vars), keep=' '.join(keep))
    return(cmd)


def how_often(var_list,vars_in_file,prefix,start,stop):
    sdict = spssaux.VariableDict()
    nn = 0
    for var in var_list:
        n = 0

        list_of_dict = []
        for x in range(start,stop):
            variable = prefix+str(x)+var
            if variable in vars_in_file:
                var_label = sdict[variable].ValueLabels
                for key in ['77','88','99']:
                    if key in var_label:
                        del var_label[key]
                list_of_dict.append(var_label)
                n += 1

        if not checkEqual1(list_of_dict):
            nn += 1
            print(var)
    print(str(nn)+' variables have different number of response categories.')
        #print(var +' ' +str(n))


def checkEqual1(iterator):
  try:
     iterator = iter(iterator)
     first = next(iterator)
     return all(first == rest for rest in iterator)
  except StopIteration:
     return True


if __name__ == '__main__':
    main()