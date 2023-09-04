import pandas as pd
from jaccard.jaccard_delta import JaccardTokenJoin

## Load data
file = '/home/alex/Desktop/datasets/yelp/yelp_clean.csv'

delta = 0.7

df = pd.read_csv(file, header=None, nrows=1000)
df = df.reset_index(drop=False)
df.columns = ['id', 'text']
df.text = df.text.apply(lambda x: x.split(';'))
df.text = df.text.apply(lambda x: list(set(x)))
print(df.head(10))

output_df = JaccardTokenJoin().tokenjoin_self(df, id='id', join='text', posFilter=True, jointFilter=True)
# print(output_df.shape)
