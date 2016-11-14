import spss
import spssaux
import pandas as pd
import numpy as np
import os
from vars_and_functions import global_vars as gb
from vars_and_functions import funcs

log = gb.folder + 'out\imputations_log.txt'
imputation_syntax = gb.folder + 'out\imputation.sps'

with open(log, 'w') as out:
    out.write(gb.to_log)
    out.write("""data in = {data_in}
    data out = data_out\n""".format(data_in = gb.select_rev, data_out = gb.imputed_data))


#############RECODE DOES NOT WORK ###############

########GLOBAL VARS##########
imputations = gb.folder +'imputations'

db_glob = pd.DataFrame.from_csv(gb.input_csv)
db_glob = db_glob.loc[db_glob[gb.version].notnull() | db_glob[gb.single_items].notnull()]
#db_glob = db_glob.loc[db_glob['items'].isin(['yeffg1','yeffg2','yeffg3','occrisk1res','occrisk5res','occrisk6res'])]
save_data = gb.version + "_" + gb.cut_mod + ".sav"

#columns_to_use = [gb.version,gb.single_items]
#cut_mods = [gb.cut_mod]

########GLOBAL VARS##########


def main():
    os.chdir(imputations)
    spssaux.OpenDataFile(gb.select_rev)
    cmd = 'DATASET NAME orginal.\n'
    cmd += 'cd "{cwd}".\n'.format(cwd=imputations)
    spss.SetOutput("off")
    cmd += fix_value_lables()
    with open(imputation_syntax, 'w') as out:
        out.write(cmd)
    spss.Submit(cmd)
    run_cmd = impute_item_for_item()
    with open(imputation_syntax, 'a') as out:
        out.write('\n'+run_cmd)
    spss.Submit(run_cmd)
    match_files()
    cmd = recode()
    with open(imputation_syntax, 'a') as out:
        out.write('\n'+cmd)
    spss.Submit("""SAVE OUTFILE = '{imputed_data}'
    /COMPRESSED.\n""".format(imputed_data=gb.imputed_data))


def fix_value_lables():
    cmd = ''
    val_dict = db_glob.loc[db_glob['val_dict'].notnull()]
    for var in val_dict[['items', 'val_dict']].values.tolist():
        for i in range(gb.start,gb.stop):
            prefix_var = gb.prefix + str(i) +var[0]
            dictionary = eval(var[1])
            cmd += get_value_label(dictionary,prefix_var)
    return cmd


def impute_item_for_item():
    cmd = ''
    sdict = spssaux.VariableDict()
    for value,gender in sdict[gb.gender].ValueLabels.items():
        cmd += ("""DATASET ACTIVATE orginal.
                    DATASET COPY  {gender}.
                    DATASET ACTIVATE  {gender}.
                    FILTER OFF.
                    USE ALL.
                    SELECT IF ({gender_var} = {val}).
                    EXECUTE.\n""".format(val=value,gender=gender, gender_var=gb.gender))
        for var in db_glob['items'].loc[db_glob['items'].notnull()]:
            try:
                var_list = [gb.prefix + str(i) + var for i in range(gb.start,gb.stop)]
                var_list = [var for var in var_list if var in sdict]
            except:
                print(var)
                print(type(var))
                print(var)
            if len(var_list) > 0:
                cmd += 'DATASET DECLARE {var}.\n'.format(var=var+'_'+gender)
                cmd += 'MULTIPLE IMPUTATION {all_vars} \n'.format(all_vars=' '.join(var_list))
                cmd += '  /IMPUTE METHOD=FCS MAXITER= 10 NIMPUTATIONS=25 SCALEMODEL=LINEAR INTERACTIONS=NONE \n  SINGULAR=1E-012 MAXPCTMISSING=NONE \n'
                for prefixed_var in var_list:
                    vals = [val for val in sdict[prefixed_var].ValueLabels if val not in gb.missing]
                    if min(vals) < 1 or max(vals) > 10:
                        print('warning: \nmin = {min} \nmax = {max}'.format(min=min(vals),max=max(vals)))
                    cmd +=' /CONSTRAINTS {prefixed_var}( MIN={min} MAX={max} RND=1)\n'.format(prefixed_var=prefixed_var,min=min(vals),max=max(vals))
                cmd += '  /MISSINGSUMMARIES NONE\n'
                cmd += '  /IMPUTATIONSUMMARIES MODELS\n'
                cmd += '  /OUTFILE IMPUTATIONS={var} .\n'.format(var=var + '_' + gender)
                cmd += 'DATASET ACTIVATE {var}.\n'.format(var=var + '_' + gender)
                cmd += 'SAVE OUTFILE = "{var}.sav"\n'.format(var=var+'_'+gender)
                cmd += '  /KEEP Imputation_ {ID} {gender_var} {vars}\n'.format(vars=' '.join(var_list), gender_var=gb.gender, ID=gb.id)
                cmd += '  /COMPRESSED.\n'
                cmd += 'DATASET ACTIVATE {gender}.\n'.format(gender=gender)
    return cmd


def match_files():
    cmd1 = ''
    cmd2 = ''
    A = funcs.get_filelist(imputations,'sav')
    f = lambda A, n=30: [A[i:i + n] for i in range(0, len(A), n)]
    cmd1 += '\n'.join(['MATCH FILES\n' + '\n'.join([' /FILE="%s"' % fil for fil in dl]) +
                     '\n /BY Imputation_ %s.\n' % gb.id  + 'SAVE OUTFILE="part_%s.sav"\n/COMPRESSED.\n' % f(A).index(dl) for dl in f(A)])
    with open(imputation_syntax, 'a') as out:
        out.write('\n'+cmd1)
    spss.Submit(cmd1)
    B = [x for x in funcs.get_filelist(imputations,'sav') if 'part_' in x]
    cmd2 += '\n'.join(['MATCH FILES\n' + '\n'.join([' /FILE="%s"' % fil for fil in B]) +
            '\n /BY Imputation_ %s.\n' % gb.id + 'SAVE OUTFILE="COMPLETE.sav"\n/COMPRESSED.\n'])
    cmd2 += 'DATASET CLOSE ALL.\n'
    with open(imputation_syntax, 'a') as out:
        out.write('\n'+cmd2)
    spss.Submit(cmd2)


def recode():
    cmd = ''
    spssaux.OpenDataFile(imputations+'/COMPLETE.SAV')
    #cmd += ("""SORT CASES  BY Imputation_.
    #SPLIT FILE LAYERED BY Imputation_.\n""")
    recode_db = db_glob.loc[db_glob['recode2'].notnull()]
    cmd += '\n'.join(['RECODE {prefix}{var} {recode}.'.format(var=pos[0], recode=pos[1],prefix=gb.prefix+str(i)) for pos in
                          recode_db[['items', 'recode2']].values.tolist() for i in range(gb.start,gb.stop)])
    cmd += '\nEXECUTE.\n'
    print(cmd)
    return (cmd)


def get_value_label(dictonary,var):
    cmd = 'VALUE LABELS\n%s\n' % var
    for i in dictonary:
        #print(i)
        cmd += i + ' ' + "'%s'" % dictonary[i]
        cmd += ' '
    cmd += '.\n'
    return cmd


if __name__ == '__main__':
    main()