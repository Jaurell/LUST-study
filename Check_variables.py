__author__ = 'Jon Aurell'

import time
start_time = time.time()


"""
Checks availability of variables in dataset and makes new dataset from selection variable and list of variables.
"""
from vars_and_functions import global_vars as gb
from vars_and_functions import reverse_items
import spss
import spssaux
import pandas as pd
import time

log = gb.folder + 'out/check_variables_log.txt'
syntax_save = gb.folder + 'out/check_variables.sps'

def main():
    gb.to_log += """data in = {data_in}
data out = {data_out}\n\n""".format(data_in=gb.org_data,data_out=gb.select_rev)
    with open(log, 'w') as out:
        out.write(gb.to_log)
    spssaux.OpenDataFile(gb.org_data)
    spss.SetOutput("off")
    db = pd.DataFrame.from_csv(gb.input_csv)
    db2 = db.loc[db[gb.version].notnull() | db[gb.single_items].notnull() | db['rename'].notnull() | db['recode'].notnull()]
    vars, to_log2 = mod_database(db2,gb.prefix,gb.start,gb.stop)
    cmd = prepare_data(vars)
    with open(syntax_save, 'w') as out:
        out.write(cmd)
    list_of_all_vars = vars['items'].values.tolist()
    vars_in_file = spssaux.VariableDict().Variables
    to_log1 = str(len(list_of_all_vars))+' variables extracted.\n'
    print(to_log1)
    with open(log,'a') as out:
        out.write(to_log1)
    how_often(vars,vars_in_file,gb.prefix,gb.start,gb.stop)
    reverse_items.main(vars,log,syntax_save)
    cmd = extract_vars(list_of_all_vars,gb.urvalsinfo,gb.select_rev,gb.keep_vars)
    with open(syntax_save, 'a') as out:
        out.write(cmd + '\n')
    #spss.Submit(cmd)
    with open(log,'a') as out:
        out.write(to_log2)
    print("--- %s seconds ---" % (time.time() - start_time))


def recode(db):
    cmd = ''
    db2 = db.loc[db['recode'].notnull()]
    cmd += '\n'.join(['RECODE {var} {recode}.'.format(var=pos[0],recode=pos[1]) for pos in db2[['items','recode']].values.tolist()])
    cmd += '\nEXECUTE.\n'
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
        for col in gb.columns_to_use:
            df[col] = prefix + str(i) + df[col]
        df_dict[i] = df
    db = pd.concat(df_dict,ignore_index=True)
    diff_vars = db['items'][~db['items'].isin(vars_in_file)].values.tolist()
    db2 = db[db['items'].isin((vars_in_file))]
    spss.Submit(recode(db2))
    rename_db = db2.loc[db['rename'].notnull(),['items','rename']]
    for i,row in rename_db.iterrows():
        orgname = row['items']
        new_name = row['rename']
        spss.Submit('RENAME VARIABLES {orgname}={new_name}.\nEXECUTE.'.format(orgname=orgname,new_name=new_name))
    vars_in_file = spssaux.VariableDict().Variables
    diff_vars = db['items'][~db['items'].isin(vars_in_file)].values.tolist()
    vars_not_in_file = 'Variables not in file\n'+'\n'.join(diff_vars)+'\n'
    print(str(len(diff_vars)) + ' variables not in file. Check log.txt for specification\n')
    db = db[db['items'].isin((vars_in_file))]
    return db, vars_not_in_file


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
    to_log = str(nn)+' variables have different number of response categories.\n'
    print(to_log)
    with open(log,'a') as out:
        out.write(to_log)


def checkEqual1(iterator):
  try:
     iterator = iter(iterator)
     first = next(iterator)
     return all(first == rest for rest in iterator)
  except StopIteration:
     return True


if __name__ == '__main__':
    main()