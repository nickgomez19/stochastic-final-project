import pandas as pd
import numpy as np
import math
import random
import statsmodels.formula.api as smf

data = pd.read_csv("merged_car_sales_data.csv")
data = data[data['Price'] < data['MSRP']/.8]
data.dropna()

smp_size = math.floor(.7 * len(data.index))
random.seed(123)

train_ind =

