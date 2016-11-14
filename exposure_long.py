import spss
import spssaux
import os
import pandas as pd
import numpy as np
import sys
import timeit
from vars_and_functions import global_vars as gb

########GLOBAL VARS##########

data = gb.folder + 'suggestion_03_cut_mod_05_imputed.sav'
input_exposure_vars_csv = folder+'\Jon\Lust\input_exposure_vars.csv'
save_data = 'imputed_long_vars_prime.sav'

prefix, start, stop = 'x6_', 2, 5
db_glob = pd.DataFrame.from_csv(input_exposure_vars_csv).reset_index()
db_glob = db_glob.loc[db_glob['indep'].notnull()]
exposed_primes = [2, 7, 17]
unexposed_primes = [3, 11, 19]
missing_primes = [5, 13, 23]


########GLOBAL VARS##########


def main():
    spssaux.OpenDataFile(data)
    spss.SetOutput("off")
    spss.Submit('SET TNumbers=Values ONumbers=Labels OVars=Labels.')
    db_prefixed = mod_database(db_glob,prefix,start,stop)
    #print(recode_cut(db_prefixed))
    spss.Submit(recode_cut(db_prefixed))
    #print(make_long_exp_vars())
    spss.Submit(make_long_exp_vars())

    spss.Submit("SAVE OUTFILE='%s'\n/COMPRESSED." % save_data)




def recode_primes(var_list):
    cmd1 = ''
    cmd2 = ''
    n = -1
    dict_primes = prime_generator()
    for var in var_list:
        n += 1
        cmd1 += 'RECODE {var} (1={exposed}) (sysmis={sysmis}) (0={unexposed}) into {var}_prime.\n'.format(var=var,
        exposed=exposed_primes[n],unexposed=unexposed_primes[n],sysmis=missing_primes[n])
        for suffix, value in sorted(dict_primes.items()):
            cmd2 += 'RECODE {var} ({value}=1) (1=0) into {var}__{suffix}.\n'.format(var=var,value=value,suffix=suffix)
    return cmd1 , cmd2
    #for var in var_list:


def make_long_exp_vars():
    cmd1 = ''
    cmd2 = ''
    cmd3 = ''
    cmd4 = ''
    cmd5 = ''
    list_of_new_var = []
    dict_primes = prime_generator()
    for var in db_glob["indep"]:
        n = 0
        new_var = var[:-7]
        list_of_new_var.append(new_var)
        list_of_vars = []
        list_of_vars_not_last = []
        for i in range(start,stop):
            prefixed_var = prefix+str(i)+var+"_cut"
            list_of_vars.append(prefixed_var)
            if i != stop-1:
                list_of_vars_not_last.append(prefix+str(i)+var+"_cut")
        n = -1
        dict_primes = prime_generator()
        for var in list_of_vars:
            n += 1
            cmd1 += 'RECODE {var} (1={exposed}) (sysmis={sysmis}) (0={unexposed}) into {var}_prime.\n'.format(var=var,
                                        exposed=exposed_primes[n],unexposed=unexposed_primes[n],sysmis=missing_primes[n])

        cmd2 += 'COMPUTE {new_var}={list_of_vars}.\n'.format(new_var=new_var,list_of_vars='*'.join([x+'_prime' for x in list_of_vars]))
        cmd3 += 'VALUE LABELS\n{var}\n{prime_dict}.\n'.format(var=new_var, prime_dict=' '.join([str(val) + ' \''+key+'\'' for key,val in dict_primes.items()]))
        cmd4 += 'RECODE {new_var} '.format(new_var=new_var)
        for n in range(0, int(len(dict_primes) ** (1. / 3)) + 1):
            cmd4 += '(' + ','.join([str(val) for key,val in dict_primes.items() if key.count('1') == n and key.count('999') == 0]) + '=%s)' % n
        cmd4 += ' into {new_var}_combined.\n'.format(new_var=new_var)
        missing_recode = ','.join([str(val) for key,val in dict_primes.items() if key.count('999') >= 1])
        for key,val in dict_primes.items():
            if key.count('999') == 0 and key.count('1') > 0:
                cmd5 += 'RECODE {new_var} ({exp}=1) ({miss}=sysmis) (else=0) into {new_var}__{suffix}.\n'.format(new_var=new_var,exp=str(val),miss=missing_recode,suffix=key)
    cmd1 += 'EXECUTE.\n'
    cmd2 += 'EXECUTE.\n'
    cmd3 += 'EXECUTE.\n'
    cmd4 += 'EXECUTE.\n'
    cmd5 += 'EXECUTE.\n'
    cmd = cmd1 +cmd2 + cmd3 + cmd4 + cmd5

    return cmd






def mod_database(input_scale,prefix,start,stop):
    vars_in_file = spssaux.VariableDict().Variables
    df_dict = {}
    for i in range(start,stop):
        df = input_scale.reset_index()
        df['time'] = prefix+str(i)
        df["indep"] = prefix + str(i) + df["indep"]
        df_dict[i] = df
    db = pd.concat(df_dict,ignore_index=True)
    db = db[db['indep'].isin((vars_in_file))]
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


def recode_cut(db):
    cmd = ''
    for var in db["indep"]:
        indep = var
        cutoff = db.loc[db['indep'] == var, "cutoff"].iloc[0]
        cmd += 'RECODE %s (Lowest thru %s=0) (%s thru Highest=1) INTO %s_cut.\n' % (indep,cutoff-1,cutoff,indep)
    cmd += 'EXECUTE.\n'
    return cmd


def prime_generator():
    import numpy as np
    prime_dict = {}
    exposed_primes = [2, 7, 17]
    unexposed_primes = [3, 11, 19]
    missing_primes = [5, 13, 23]
    all_primes = [exposed_primes, unexposed_primes, missing_primes]
    prime_products = [[x1, x2, x3] for x1 in [x[0] for x in all_primes] for x2 in [x[1] for x in all_primes] for x3 in
                      [x[2] for x in all_primes]]
    for pp in prime_products:
        key = '_'.join(
            ['0' if x in unexposed_primes else '999' if x in missing_primes else '1' if x in exposed_primes else 'error'
             for x in pp])
        prod = np.prod(pp)
        prime_dict[key] = prod
    return prime_dict


if __name__ == '__main__':
    main()