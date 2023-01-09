def merge_dictionaries_with_sum(dict_1, dict_2):
    dict_3 = {**dict_1, **dict_2}
    for key in dict_3.keys():
        if key in dict_1 and key in dict_2:
                dict_3[key] = dict_1[key] + dict_2[key]
    return dict_3

def write_file(file_path,separator,items):
    with open(file_path, 'w') as fp:
        fp.write(f'{separator}'.join(list(items)))
        
        
        
if __name__ == '__main__':
    pass