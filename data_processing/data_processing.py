from pyspark import SparkConf, SparkContext
conf = SparkConf.setAppName("Data_processor").setMaster("local")
sc = SparkContext(conf=conf)

distfile = sc.textFile("data.txt")

