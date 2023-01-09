from task2_1 import get_airbnb_df
from utilities import write_file

#Here the get_airbnb_df is implemented to get spark session and the airbnb dataframe
spark, df = get_airbnb_df()

#min_price, max_price and row_count calculated
row_count = df.count()
max_price = df.agg({'price': 'max'}).collect()[0][0]
min_price = df.agg({'price':'min'}).collect()[0][0]


write_file = write_file('../out/out_2_2.txt',', ', [str(min_price),str(max_price),str(row_count)])
