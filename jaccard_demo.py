import pandas as pd
from pytokenjoin.jaccard.join_delta import JaccardTokenJoin
from pytokenjoin.edit.join_delta import EditTokenJoin

## Load data
file = '/home/alex/Desktop/datasets/yelp/yelp_clean.csv'

delta = 0.7

df = pd.read_csv(file, header=None, nrows=1000)
df = df.reset_index(drop=False)
df.columns = ['id', 'text']
df.text = df.text.apply(lambda x: x.split(';'))
df.text = df.text.apply(lambda x: list(set(x)))
print(df.head(10))

output_df = EditTokenJoin().tokenjoin_self(df, id='id', join='text', posFilter=True, jointFilter=True)
# print(output_df.shape)

#pyTokenJoin