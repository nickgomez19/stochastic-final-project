import pandas as pd
import numpy as np
import re
from string import digits
from itertools import groupby
import dask.dataframe as dd

def load_reference_ds():
    reference = pd.read_csv('data/reference.csv')
    reference = reference.rename(columns={'city mpg': 'City MPG', 'highway MPG': 'Highway MPG'})

    styles = ["Hatchback", "SUV", "Coupe", "Sedan"]
    for style in styles:
        reference.loc[reference['Vehicle Style'].str.contains(style), 'Vehicle Style'] = style
    
    print("REFERENCE HEAD")
    print(reference.head())
    # reference.info(memory_usage='deep')

    return reference

def load_sales_ds():
    sales1 = pd.read_csv('data/carsales1.csv', error_bad_lines=False)
    # sales1['Model'] = sales1['Model'].str.replace("\\","\\")
    # sales1.drop([1084357])
    print("SALES HEAD")
    print(sales1.head())

    sales1.info(memory_usage='deep')
    return sales1

def camel_case_split(identifier):
    matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    return [m.group(0) for m in matches]

def removeDigits(str):
    remove_digits = str.maketrans('', '', digits)
    res = str.translate(remove_digits)
    return res

def splitByInts(s):
    return [''.join(g) for _, g in groupby(s, str.isalpha)]

def pickModel(str):
    first_camel = camel_case_split(str)[0]
    wo_digits = splitByInts(first_camel)

    if len(wo_digits) > 1 and wo_digits[0].isupper() and wo_digits[1].isdigit():
        model = wo_digits[0] + wo_digits[1]
    else:
        model = wo_digits[0]

    return model

def pickHighest(df, col, model):
    counts = df[col].value_counts()
    value = counts.sort_values(ascending=True).index[0]
    # if len(counts) > 1:
    #     print("-------------------------------------------------------------------------MULTIPLE OF THESE VALUES")
    #     print("Model: "  + model)
    #     print("Col: " + col)
    #     print("Counts: ")
    #     print(counts)
    return value

def pickAvg(df, col, model):
    return sum(df[col]) / len(df[col])

def iterate(dic, df, labels, picking_function, model):
    for label in labels:
        dic[label] = picking_function(df, label, model)
    return dic

def generate_bool(rf, row, model):
    bool_filter = (rf['Model'].str.contains(model)) & (rf['Make'] == row['Make']) & (
        rf['Year'] == np.int64(row['Year']))
    return bool_filter

def findAttr(attrMap, model):
    res = False 
    for attr in attrMap.keys():
        if attr in model:
            res = attrMap[attr]
    return res

model2style = {'2dr':'Coupe', '4dr':'Sedan', 'Coupe':'Coupe', 'Sedan':'Sedan', 'Hatch': 'Hatchback'}
model2size = {} # no good, common way to pan model names to a specific size
meta = {}
multi_val_cols = ['Vehicle Style', 'Vehicle Size']
avg_cols = ['MSRP', 'City MPG', 'Engine HP', 'Highway MPG', 'Popularity']
for label in ["Matched"] + multi_val_cols:
    meta[label] = "str"
for label in avg_cols+['Total Depreciation', 'Age', 'Avg MPG', 'Avg Depreciation Per Year']:
    meta[label] = "float"
# print("META INFORMATION FOR ROW")
# dtypes=[''string', 'string', 'float', 'float','float','float','float']
def applicble(row):
    # print("---------------")
    index= row.name
    model = pickModel(row['Model'])
    filtr = reference[generate_bool(reference, row, model)]

    size, style = findAttr(model2size, row['Model']), findAttr(model2style, row['Model'])
    if(size):
        size_cond = filtr['Vehicle Size'].str.contains(str(size))
    else:
        size_cond = np.ones((len(filtr.index)), dtype=bool)
    if(style):
        style_cond = filtr['Vehicle Style'].str.contains(str(style))
    else:
        style_cond = np.ones((len(filtr.index)), dtype=bool)
    
    filtr = filtr[size_cond & style_cond]

    # print("*****matched style: " + str(style))
    # print("before--")
    # print(filtr[['Vehicle Style']])


    # print("after--")
    # print(filtr[['Vehicle Style']])

    # print("style boolean--")
    # print(style_cond)

    # print("boolean")
    # print(str(not style))
    res = {}

    res['Matched'] = (len(filtr.index) != 0)
    if not res['Matched']:
        for label in multi_val_cols:
            res[label] = "str"
        for label in avg_cols+['Total Depreciation', 'Age', 'Avg MPG', 'Avg Depreciation Per Year']:
            res[label] = 1.01
        
        res = pd.Series(res)
        # print("fake result")
        # print(res.dtypes)
        return res

    # print("filtr")
    # print(filtr.head())
    # print(row)

    

    iterate(res, filtr, multi_val_cols, pickHighest, row['Model'])
    iterate(res, filtr, avg_cols, pickAvg, row['Model'])


    res['Total Depreciation'] = res['MSRP'] - row['Price']
    res['Age'] = (2017 - (row['Year']-1))
    res['Avg MPG'] = (res['City MPG'] + res['Highway MPG']) / 2

    if res['Age'] != 0:
        res['Avg Depreciation Per Year'] = res['Total Depreciation'] / res['Age']
    else:
        res['Avg Depreciation Per Year'] = res['Total Depreciation']

    res = pd.Series(res)
    # print("actual result")
    # print(res.dtypes)
    return res

        # new_s = pd.Series(new_dict)
        # combined_s = new_s.append(row)
        # result_df = result_df.append(combined_s, ignore_index=True)

def merge(reference, target):
    iterable = target[0:1084000]
    no_match = 0
    result__added_fields_df = pd.DataFrame()
    dict_added_fileds = {}
    print("merge")

    # matched = iterable.apply(lambda rw: applicble(rw), axis=1)

    dd_iterable = dd.from_pandas(iterable, npartitions=7)
    matched = dd_iterable.map_partitions(lambda df: df.apply(lambda rw: applicble(rw), axis=1), meta=meta).compute(scheduler='processes')  
    # matched.compute()

    # added_fields_df = pd.DataFrame.from_dict(dict_added_fileds, "index")
    # print(added_fields_df.head())

    trimmed_df = iterable[matched['Matched']]
    matched = matched[matched['Matched']]
    matched = matched.drop('Matched', 1)

    # trimmed_df = iterable[matched]

    # print(added_fields_df.head())
    # print(trimmed_df.head())

    result_df = trimmed_df.join(matched)
    print("NUMBER OF MATCHES")
    print(len(result_df.index))

    print("NUMBER WITHOUT MATCHES")
    print(len(iterable.index) - len(result_df.index))

    print("Resulting Dataframe")
    print(matched.head())

    result_df.to_csv("merged_car_sales_data.csv", index=False)

if __name__ == "__main__":
    sales = load_sales_ds()
    reference = load_reference_ds()
    merge(reference, sales)

#df frame operations
#