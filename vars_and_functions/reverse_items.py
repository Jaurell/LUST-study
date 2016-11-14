import spss
import spssaux
from vars_and_functions import global_vars as gb


def main(db,log,syntax_save = 'rev.sps'):
    cmd, n = reverse_items(db,log,syntax_save)
    with open(syntax_save, 'a') as out:
        out.write('\n'+cmd+'\n')
    rev_log = str(n)+' items have been reversed.\n'
    print(rev_log)
    with open(log, 'a') as out:
        out.write(rev_log)


def reverse_items(db,log,syntax_save):
    cmd = ''
    reverse_list = db.loc[db['reverse'] == 1,['items']].values.tolist()
    n = 0
    sdict = spssaux.VariableDict()
    for item in reverse_list:
        n += 1
        cmd += get_reverse(item[0],sdict,log)
    return cmd, n

def get_reverse(var,sdict,log):
    cmd = ''
    reversed_lables = {}
    try:
        labels = sdict[var].ValueLabels
    except:
        print(var)
    key_list = []
    for key in labels:
        if int(key) not in gb.missing_int:
            key_list.append(int(key))
    try:
        sub_val = max(key_list) + 1
    except:
        print(var)
        print(sub_val,max(key_list))

    for label in labels:
        if label not in gb.missing:
            rev_val = sub_val - int(label)
            reversed_lables[str(rev_val)] = labels[label]
        else:
            reversed_lables[label] = labels[label]
    cmd += get_recode_rev(reversed_lables, var, sdict,log)
    return cmd


def get_value_label(dictonary,var):
    cmd = 'VALUE LABELS\n%s\n' % var
    for i in dictonary:
        cmd += i + ' ' + "'%s'" % dictonary[i]
        cmd += ' '
    cmd += '.\n'
    return cmd

def get_recode_rev(reversed_lables,var,sdict,log):
    cmd = 'RECODE %s' % var
    i = sdict[var].index
    dataCursor = spss.Cursor([i])
    oneVar = dataCursor.fetchall()
    # extending the example to get the actual list of values.
    uniqueList = list((set(oneVar)))
    uniq_vals = sorted([int(x[0]) for x in uniqueList if x[0] != None and x[0] not in gb.missing_int])
    uniq_vals_rev = reversed(uniq_vals)
    dataCursor.close()
    check_list = [int(x) for x in reversed_lables if int(x) not in gb.missing_int]
    if min(check_list) != min(uniq_vals) or max(check_list) != max(uniq_vals):
        print('%s values not aligned with value labels.\n check %s for details.' % (var, log))
        rev_log = """
        {var} actual min {min_uniq} = {min_lable} in value labels
        actual max {max_uniq} = {max_lable} in value labels
        """.format(var=var,min_uniq=min(uniq_vals),min_lable=min(check_list),max_uniq=max(uniq_vals),max_lable=max(check_list))
        with open(log, 'a') as out:
            out.write(rev_log)
    recode_list = [[x1, x2] for x1, x2 in zip(uniq_vals, uniq_vals_rev)]
    for rec in recode_list:
        cmd += ' (%s=%s)' % (rec[0],rec[1])
    cmd += '.\n'
    cmd += get_value_label(reversed_lables, var)
    return cmd
