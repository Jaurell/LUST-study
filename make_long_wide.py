
                                             #Don't forget to change the date

import spss
import spssaux
import os
import pandas as pd


########## global vars #########
folder = r"C:\Users\jonaur\Desktop\make data"
#spss.SetOutput("off")
datevars = ["AnswerDate", "datum_anställning", "examen_date", "anställningsstart"]
starting_date = "01,28,2016"  # MM/DD/YYYY intensivstudien
list_of_folders = ["strings", "no_strings", "long_and_wide", "no_strings/suffix", "strings/suffix"]
id_variable = "kod_id"
input_csv = folder + '/scales.csv'
input_rename = folder + '/rename.csv'


rename = pd.DataFrame.from_csv(input_rename, index_col=False)




def main():
    create_folders(folder,list_of_folders)
    fix_files(folder,datevars,id_variable,starting_date)
    add_suffix(folder)
    for item in ["strings","no_strings"]:
        make_wide(folder+"/"+item+"/suffix",item,folder+"/long_and_wide")
        make_long(folder+"/"+item,item,folder+"/long_and_wide")


def rename_vars(vars_in_file):
    for i, row in rename.iterrows():
        orgname = row['old']
        new_name = row['new']
        if orgname in vars_in_file:
            spss.Submit('RENAME VARIABLES {orgname}={new_name}.\nEXECUTE.'.format(orgname=orgname, new_name=new_name))


def get_stringvars():
    varcount=spss.GetVariableCount()
    stringvars = []
    for i in range(varcount):
        if spss.GetVariableType(i) > 0:
            stringvars.append(spss.GetVariableName(i))
    return stringvars


def fix_files(folder,datevars,id_variable,date):
    datafiles = get_filelist(folder, "sav")
    for fil in datafiles:
        exclude = []
        spssaux.OpenDataFile(fil)
        vars_in_file = spssaux.VariableDict().variables
        rename_vars(vars_in_file)
        spss.Submit("ALTER TYPE %s (f8)." % id_variable)
        stringvars = get_stringvars()
        print(stringvars)
        vars_in_file = spssaux.VariableDict().variables
        base, ext = os.path.basename(fil).split('.')
        for var in stringvars:
            if var in vars_in_file:
                exclude.append(var)
        if "AnswerDate" in vars_in_file:
            spss.Submit("""
            COMPUTE time_days = DATEDIFF(AnswerDate, DATE.MDY(%s), "day").
            EXECUTE.
            """ % date)
        for var in vars_in_file:
            if 'mean' not in var and var not in exclude + datevars:
                spss.Submit("ALTER TYPE %s (f8)." % var)
                spss.Submit("VARIABLE ALIGNMENT %s(right)." % var)
                spss.Submit("VARIABLE LEVEL %s(scale)." % var)
                if var != id_variable:
                    spss.Submit("RECODE %s (SYSMIS=999)." % var)
                    spss.Submit("MISSING VALUES %s (999)." % var)
        spss.Submit("SORT CASES BY %s (A)." % id_variable)
        spss.Submit("""COMPUTE data_from_week=%s.
        EXECUTE.
        ALTER TYPE data_from_week (f8).
        ALTER TYPE time_days (f8).
        """ % base)
        spss.Submit(save_and_close(base,exclude,folder))
    spss.Submit(("NEW FILE."))


def save_and_close(base,exclude,folder):
    print(exclude)
    cmd = "SAVE OUTFILE = '%s%s.sav'.\n" %(folder+'/strings/',base)
    cmd += "SAVE OUTFILE='%s%s.sav'\n" % (folder+'/no_strings/',base)
    if exclude:
        cmd += '/DROP=%s\n' % " ".join(exclude)
    cmd += """
        /COMPRESSED.
        DATASET CLOSE ALL.
        """
    return cmd


def add_suffix(folder):
    for item in ["/no_strings","/strings"]:
        datafiles = get_filelist(folder+item, 'sav')
        for file in datafiles:
            print(file)
            exclude = ['kod_id'] #Ange namnet på id_variablen
            spssaux.OpenDataFile(file)
            basename = os.path.basename(file).strip('.sav')
            suffix = basename #önskat suffix
            print(basename)
            vars = spssaux.VariableDict().variables
            for i in exclude:
                if i in vars:
                    vars.remove(i)
            oldnames = spssaux.VariableDict().expand(vars)
            newnames = [varnam + "_" + suffix for varnam in oldnames]
            spss.Submit('rename variables (%s=%s).'%('\n'.join(oldnames),'\n'.join(newnames)))
            spss.Submit("""
            SAVE OUTFILE = "%s%s".
            DATASET CLOSE ALL.
            NEW FILE.
            """ %(folder+item+'/suffix/',basename + '.sav'))


def get_cmd(datafiles,datatype,file_suffix):
    if datatype == "suffix":
        command = 'MATCH FILES\n'
        save = "wide"
    if datatype == "fixed":
        command = 'ADD FILES\n'
        save = "long"
    cmd = command
    for datafile in datafiles:
        base, ext, =  os.path.basename(datafile).split('.')
        if datatype == "suffix":
            cmd += '  /FILE="'
            cmd += datafile
            cmd += '" /IN=%s\n' %("Week_"+base)
        if datatype == "fixed":
            cmd += '  /FILE="'
            cmd += datafile
            cmd += '\n'

    cmd += '  /BY kod_id.\n'
    cmd += 'LIST CASES.\n'
    cmd += 'SAVE OUTFILE = %s_%s.sav.\n' %(save,file_suffix)
    cmd += 'DATASET CLOSE ALL.\n'
    return cmd


def make_wide(folder,file_suffix,save_folder): #wide
    spss.Submit("CD '%s'." % save_folder)
    datafiles = get_filelist(folder, 'sav')
    prefix = "suffix"
    spss.Submit(get_cmd(datafiles,prefix,file_suffix))


def make_long(folder,file_suffix,save_folder): #long
    spss.Submit("CD '%s'." % save_folder)
    datafiles = get_filelist(folder, 'sav')
    prefix = "fixed"
    spss.Submit(get_cmd(datafiles, prefix,file_suffix))


def create_folders(folder,list_of_folders):
    for item in list_of_folders:
        if not os.path.exists(folder+"/"+item):
            os.makedirs(folder+"/"+item)


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