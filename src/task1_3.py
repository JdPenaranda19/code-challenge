from task1_1 import get_groceries_rdd
from utilities import merge_dictionaries_with_sum, write_file

#Here the get_groceries_rdd is implemented to get spark session and groceries_rdd
spark, groceries_rdd = get_groceries_rdd()

#utilities.py is added in context to be used as a module by PySpark
sc=spark.sparkContext
sc.addPyFile("utilities.py") 

#For the following calculation we are assuming that we are counting only appearances in shopping lists
# not the number of appearances, i.e. [apple, apple, milk] will return a count of {apple:1,milk:1}
count_map = groceries_rdd.map(lambda x: {item:1 for item in list(set(x))})
count_reduced = count_map.reduce(lambda a,b: merge_dictionaries_with_sum(a,b))
sorted_count = map(lambda x: str(x), sorted(list(count_reduced.items()), reverse = True, key = lambda x: x[1]))

write_file('../out/out_1_3.txt','\n',sorted_count)
