import pandas as pd
import numpy as np
import spss
import spssaux
import os

folder = r'C:\Users\jonaur\Desktop\make data\long_and_wide'
input_csv = R'C:\Users\jonaur\Desktop\make data\scales.csv'
df = pd.DataFrame.from_csv(input_csv, index_col=False, encoding = 'iso-8859-1')
exclude = ['AE_all','AE_5_6','exp_dev','sss8','top','tow'] #2015
#exclude = ['AE_all'] # 2016
df = df[df['scale'].isin(exclude) == False]


missing = '999'




def main():
    spss.SetOutput("off")
    datafile = folder + "/long_strings.sav"
    cmd = ''
    spssaux.openDataFile(datafile)
    cmd += reverse_items()
    cmd += compute_means()
    print(cmd)


def compute_means():
    cmd = ''
    rev = list()
    sdict = spssaux.VariableDict()

    for row in df.values.tolist():
        rev = list()
        #items = [x for x in row[5:] if pd.notnull(x)]
        items = [x for x in row[5:] if pd.notnull(x) and x in sdict]
        print(items)
        if len(items) > 1:
            if pd.notnull(row[2]):
                rev = [int(x) for x in row[2].split(",")]
            for n in rev:
                items[n-1] = items[n-1]+'_rev'
            cmd += 'COMPUTE {var}_mean=mean.{min_mean}({items}).\n'.format(var=row[0],min_mean=row[1],items=','.join(items))
            cmd += 'Execute.\n'
            cmd += "VARIABLE LABELS {var}_mean '{lable}'.\n".format(var=row[0], lable=row[4])
    return cmd


def reverse_items():
    cmd = ''
    sdict = spssaux.VariableDict()
    for row in df.values.tolist():
        if pd.notnull(row[2]):
            rev = [int(x) for x in row[2].split(",")]
            rev_items = [row[5:][x-1] for x in rev]
            for item in rev_items:

                cmd += get_reverse(item,sdict)
    return cmd


def get_reverse(var,sdict):
    cmd = ''
    reversed_lables = {}
    try:
        labels = sdict[var].ValueLabels
    except:
        print(var)
    key_list = []
    for key in labels:
        if int(key) != 999:
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
    cmd += get_recode_rev(reversed_lables, var, sub_val,sdict)
    return cmd


def get_recode_rev(dictionary,var,sub_val,sdict):
    cmd = 'RECODE %s' % var
    lab = sdict[var].VariableLabel
    for i in dictionary:
        if i not in missing:
            rev_val = sub_val - int(i)
            cmd += ' (%s=%s)' % (i,str(rev_val))
    cmd += 'INTO {var}_rev .\nEXECUTE.\n'.format(var=var)
    cmd += "VARIABLE LABELS {var}_rev '{lab}'.\n".format(var=var,lab=lab)
    cmd += get_value_label(dictionary,var)
    return cmd


def get_value_label(dictonary,var):
    cmd = 'VALUE LABELS\n%s_rev\n' % var
    for i in dictonary:
        cmd += i + ' ' + "'%s'" % dictonary[i]
        cmd += ' '
    cmd += '.\n'
    return cmd


if __name__ == '__main__':
    main()