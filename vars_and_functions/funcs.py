def get_filelist(folder, extension):
    import os
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