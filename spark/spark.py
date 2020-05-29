import sys
from operator import add
import random
from pyspark import SparkContext

def create_vector(line):
    vector= []
    vector= line.split(",")
    del vector[0]
    #now we should obtain the vector component only
    return vector

#def select_random_centroid(line, k, columns):
    


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

    lines = sc.textFile(sys.argv[5])

    datapoints=lines.flatMap(create_vector)

    #select random indexes
    indexes= random.sample(range(rows), k)

    #first_centroid = 

    

