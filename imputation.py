import spss
import spssaux
import pandas as pd
import numpy as np
import os

#############RECODE DOES NOT WORK ###############

########GLOBAL VARS##########

possible_paths = [R'C:\Users\stupg\Desktop', R'C:\Users\Jon\Desktop', R'C:\Users\jonaur\Desktop']
for path in possible_paths:
    if os.path.exists(path):
        folder = path

outfolder = folder+'\Jon\LUST\out'

data = folder+'\Jon\LUST\EX2006_select_rev.sav'

input_scale_csv = folder+'\Jon\Lust\input_scale.csv'
imputations = folder +'\Jon\LUST\imputations'
#save_data = '01_middle_cut_mod.sav'

missing = ['0','77','88','99']
prefix, start, stop = 'x6_', 2, 5
version = 'suggestion_03'
single_items = 'single_item_02'
cut_mod = 'cut_mod_05'
single_items_cut_mod = 'cutmod_single_item_02'
db_glob = pd.DataFrame.from_csv(input_scale_csv)
db_glob = db_glob.loc[db_glob[version].notnull() | db_glob[single_items].notnull()]
#items_list = [prefix + str(i) + var for var in db_glob['items'] for i in range(start,stop)]
save_data = version + "_" + cut_mod + ".sav"

columns_to_use = [version,single_items]
cut_mods = [cut_mod]

########GLOBAL VARS##########


def main():
    os.chdir(imputations)
    spssaux.OpenDataFile(data)
    spss.Submit('DATASET NAME orginal.')
    spss.Submit('cd "{cwd}".'.format(cwd=imputations))
    spss.SetOutput("off")
    spss.Submit(fix_value_lables())
    run_cmd = impute_item_for_item()
    #print(run_cmd)
    spss.Submit(run_cmd)
    match_files()
    spss.Submit(recode())
    spss.Submit("""SAVE OUTFILE = '{folder}\COMPLETE_recoded.sav'
  /COMPRESSED.\n""".format(folder=outfolder))


def fix_value_lables():
    cmd = ''
    val_dict = db_glob.loc[db_glob['val_dict'].notnull()]
    for var in val_dict[['items', 'val_dict']].values.tolist():
        for i in range(start,stop):
            prefix_var = prefix + str(i) +var[0]
            dictionary = eval(var[1])
            cmd += get_value_label(dictionary,prefix_var)
    return cmd

def impute_item_for_item():
    cmd = ''
    sdict = spssaux.VariableDict()
    for value,gender in sdict['x6_1kon'].ValueLabels.items():

        cmd += ("""DATASET ACTIVATE orginal.
                    DATASET COPY  {gender}.
                    DATASET ACTIVATE  {gender}.
                    FILTER OFF.
                    USE ALL.
                    SELECT IF (x6_1kon = {val}).
                    EXECUTE.\n""".format(val=value,gender=gender))

        for var in db_glob['items'].loc[db_glob['items'].notnull()]:
            try:
                var_list = [prefix + str(i) + var for i in range(start,stop)]
            except:
                print(var)
                print(type(var))
                print(var)
            cmd += 'DATASET DECLARE {var}.\n'.format(var=var+'_'+gender)
            cmd += 'MULTIPLE IMPUTATION {all_vars} \n'.format(all_vars=' '.join(var_list))
            cmd += '  /IMPUTE METHOD=FCS MAXITER= 10 NIMPUTATIONS=25 SCALEMODEL=LINEAR INTERACTIONS=NONE \n  SINGULAR=1E-012 MAXPCTMISSING=NONE \n'
            for prefixed_var in var_list:
                vals = [val for val in sdict[prefixed_var].ValueLabels if val not in missing]
                cmd +=' /CONSTRAINTS {prefixed_var}( MIN={min} MAX={max} RND=1)\n'.format(prefixed_var=prefixed_var,min=min(vals),max=max(vals))
            cmd += '  /MISSINGSUMMARIES NONE\n'
            cmd += '  /IMPUTATIONSUMMARIES MODELS\n'
            cmd += '  /OUTFILE IMPUTATIONS={var} .\n'.format(var=var + '_' + gender)
            cmd += 'DATASET ACTIVATE {var}.\n'.format(var=var + '_' + gender)
            cmd += 'SAVE OUTFILE = "{var}.sav"\n'.format(var=var+'_'+gender)
            cmd += '  /KEEP Imputation_ ID x6_1kon {vars}\n'.format(vars=' '.join(var_list))
            cmd += '  /COMPRESSED.\n'
            cmd += 'DATASET ACTIVATE {gender}.\n'.format(gender=gender)
    return cmd


def match_files():
    cmd1 = ''
    cmd2 = ''
    A = get_filelist(imputations,'sav')
    f = lambda A, n=30: [A[i:i + n] for i in range(0, len(A), n)]
    cmd1 += '\n'.join(['MATCH FILES\n' + '\n'.join([' /FILE="%s"' % fil for fil in dl]) +
                     '\n /BY Imputation_ ID.\n' + 'SAVE OUTFILE="part_%s.sav"\n/COMPRESSED.\n' % f(A).index(dl) for dl in f(A)])
    spss.Submit(cmd1)
    B = [x for x in get_filelist(imputations,'sav') if 'part_' in x]
    cmd2 += '\n'.join(['MATCH FILES\n' + '\n'.join([' /FILE="%s"' % fil for fil in B]) +
            '\n /BY Imputation_ ID.\n' + 'SAVE OUTFILE="COMPLETE.sav"\n/COMPRESSED.\n'])
    cmd2 += 'DATASET CLOSE ALL.\n'
    #print(cmd2)
    spss.Submit(cmd2)


def recode():
    cmd = ''

    spssaux.OpenDataFile(imputations+'/COMPLETE.SAV')
    #cmd += ("""SORT CASES  BY Imputation_.
#SPLIT FILE LAYERED BY Imputation_.\n""")
    recode_db = db_glob.loc[db_glob['recode2'].notnull()]
    cmd += '\n'.join(['RECODE {prefix}{var} {recode}.'.format(var=pos[0], recode=pos[1],prefix=prefix+str(i)) for pos in
                          recode_db[['items', 'recode2']].values.tolist() for i in range(start,stop)])
    cmd += '\nEXECUTE.\n'
    return (cmd)


def get_value_label(dictonary,var):
    cmd = 'VALUE LABELS\n%s\n' % var
    for i in dictonary:
        #print(i)
        cmd += i + ' ' + "'%s'" % dictonary[i]
        cmd += ' '
    cmd += '.\n'
    return cmd


def get_filelist(folder, extension):
    list_of_files = []
    for fil in os.listdir(folder):
        try:
            base, ext, = fil.split('.')
            if ext == extension:
                list_of_files.append(os.path.join(folder, fil))
        except:
            pass
    list_of_files.sort()
    return list_of_files



if __name__ == '__main__':
    main()