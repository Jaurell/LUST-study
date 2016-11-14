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
spssaux.OpenDataFile(gb.org_data)
test = spssaux.VariableDict()
print(test)
vars = ['x4_6aevyt01','x4_6aevyt02']

for var in reversed(vars):
    i = test[var].index
    dataCursor=spss.Cursor([i])
    oneVar=dataCursor.fetchall()
    #extending the example to get the actual list of values.
    uniqueList=list((set(oneVar)))
    uniq_vals = [int(x[0]) for x in uniqueList if x[0] != None and x[0] not in gb.missing_int]
    uniq_vals_rev = reversed(uniq_vals)
    dataCursor.close()
    tests = [[x1,x2] for x1, x2 in zip(uniq_vals, uniq_vals_rev)]

    print(tests)