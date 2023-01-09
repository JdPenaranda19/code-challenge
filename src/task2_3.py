from task2_1 import get_airbnb_df
from utilities import write_file

#Here the get_airbnb_df is implemented to get spark session and the airbnb dataframe
spark, df = get_airbnb_df()

#Assumption: the review score is given by the review_scores_value field
avg_results = df.filter("price > 5000 and review_scores_value = 10").agg({'bathrooms':'avg','bedrooms':'avg'}).collect()

write_file('../out/out_2_3.txt', ', ', [str(avg_results[0]["avg(bathrooms)"]), str(avg_results[0]["avg(bedrooms)"])])
