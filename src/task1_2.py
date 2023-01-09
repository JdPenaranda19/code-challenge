from task1_1 import get_groceries_rdd
from utilities import write_file

#Here the get_groceries_rdd is implemented to get spark session and groceries_rdd
spark, groceries_rdd = get_groceries_rdd()

# Part 1 - Task 2-a
distinct_elements = groceries_rdd.reduce(lambda a,b: {*a,*b})

write_file('../out/out_1_2a.txt','\n',distinct_elements)

# Part 1 - Task 2-b
products_per_transaction = groceries_rdd.map(lambda x: len(x))
total_products = products_per_transaction.reduce(lambda a,b: a+b)

write_file('../out/out_1_2b.txt','Count:\n',['',str(total_products)])