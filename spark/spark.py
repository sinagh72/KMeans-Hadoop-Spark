import sys
from operator import add
import random
from pyspark import SparkContext

def create_vector(line):
    vector= []
    vector= line.split(",")
    return vector

def select_random_centroid(point, indexes):
    if point[0] in indexes:
        return point

def assign_to_cluster(point, centroid): 
    #compute the distance from each centroid and output the key corrisponing to the nearest 
    print("tets")

if __name__ == "__main__":
    if len(sys.argv) !=6 :
        print("Usage: K-means <k> <rows> <columns> <threshold> <input file> <output file>", file=sys.stderr)
        sys.exit(-1)
#parameter from the command line 
    k = sys.argv[1]
    rows= sys.argv[2]
    columns = sys.argv[3]
    threshold =sys.argv[4]
    input_file= sys.argv[5]
    output_file= sys.argv[6]

    print("Number of cluster: "+ k)
    print("Number of vector: " + rows)
    print("Dimension of vectors: "+ columns)
    print("Threshold: "+  threshold)
    print("Input file: "+input_file)
    print("Output file: "+ output_file)
    

#initialize spark context    
    master = "local"
    sc = SparkContext(master, "K-means")

    #set a maximum number of iteration
    iteration= 100

    lines = sc.textFile(sys.argv[5])

    datapoints=lines.flatMap(lambda point : create_vector(point))

    #select random indexes
    indexes= random.sample(range(rows), k)

    centroid = lines.flatMap(lambda point : select_random_centroid(point, indexes))
    finish= False

    while iteration>0 or not finish:
        data_assigned = datapoints.map(lambda point : assign_to_cluster(point, centroid) ) #this should return a (assigned_cluster, datapoint) pair 

        data_assigned.reduceByKey(...) #calculate the new centroid summing each component and dividing the number

        #compare between two iteration and set condition "finish"

    #save the centroid (how to consider the centroid that are not point?)





    

