import os
import time
now = time.ctime()

folder = R'C:/Users/jonaur/Desktop/Jon/LUST/'
os.chdir(folder)
input_csv = folder + 'input_scale.csv'


####2004####
org_data = folder + 'data\EX2004_long_v10EF.sav' #X4

select_rev = folder + 'out/EX2004_select_rev.sav'
imputed_data = folder + 'out/Imputed.sav'
urvalsinfo = 'svar_1', '1'
id = 'EX2004_id'
gender = 'Kon'
prefix, start, stop = 'x4_', 4, 9

####2006####
#data = folder + '\Jon\LUST\EX2006_long_v3EF_ID.sav' #X6




###for all datasets###
version = 'suggestion_03'
single_items = 'single_item_02'
cut_mod = 'cut_mod_05'
single_items_cut_mod = 'cutmod_single_item_02'
columns_to_use = [version,'rename','items']
missing_int = [0,77,88,99]
missing = [str(x) for x in missing_int]


keep_vars = [id,gender]

to_log ="""{time}

missing = {missing}
version = {version}
single item = {single_items}\n\n""".format(missing=missing, version=version,single_items=single_items, time=now)



