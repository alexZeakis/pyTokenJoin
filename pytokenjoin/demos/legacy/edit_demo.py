import pandas as pd
from edit_utils import tokenjoin_self, tokenjoin_foreign

## Load data
file = '/home/alex/Desktop/datasets/yelp/yelp_clean.csv'

delta = 0.7

df = pd.read_csv(file, header=None, nrows=1000)
df = df.reset_index(drop=False)
df.columns = ['id', 'text']
df.text = df.text.apply(lambda x: x.split(';'))
df.text = df.text.apply(lambda x: list(set(x)))
# print(df.head(10))

# output_df = tokenjoin_self(df, id='id', join='text', posFilter=True, jointFilter=True)
# print(output_df.shape)

df2 = df.sample(1000, random_state=1924).reset_index(drop=True)
df1 = df2.sample(100, random_state=1924).reset_index(drop=True)
output_df = tokenjoin_foreign(df1, df2, 'id', 'id', 'text', 'text',
                              posFilter=True, jointFilter=True)
print(output_df.shape)