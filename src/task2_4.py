from task2_1 import get_airbnb_df
from utilities import write_file

#Here the get_airbnb_df is implemented to get spark session and the airbnb dataframe
spark, df = get_airbnb_df()


result_task_4 = df.orderBy(df.price,df.review_scores_rating.desc()).select('accommodates').take(1)[0][0]

write_file('../out/out_2_4.txt','',str(result_task_4))
