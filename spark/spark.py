import sys
from operator import add
import random
from pyspark import SparkContext
import time

def create_vector(line):
    vector= []
    vector= line.split(",")
    for i in range(1,len(vector)):
        vector[i] = float(vector[i])
    return vector[1:]


def assign_to_cluster(point, centroids):
    #compute the distance from each centroid and output the key corrisponing to the nearest
    idx=0
    min_dist=float("inf")
    for centroid in centroids:
        dist=distance(point, centroid) 
        if dist<min_dist:
            min_dist= dist
            idx=centroids.index(centroid)
    return (idx, point)


def distance (p1, p2):
    sum=0
    if len(p1)!=len(p2):
        print("Error computing the distance: different dimension")

    for iteration in range(len(p1)):
        dist= p1[iteration]-p2[iteration]
        sum += (dist**2)
    return (sum**(1/2)) #return the index of the centroid and the distance from the point


def vector_sum (key_value): #here is (k, list of points in key_value form)
    points= key_value[1]
    count= len(points)
    new_centroid=[sum(x)/count for x in zip(*points)]
    return new_centroid



if __name__ == "__main__":
    if len(sys.argv) !=8 :
        print("Usage: K-means <k> <rows> <columns> <threshold> <master> <input file> <output file>", file=sys.stderr)
        sys.exit(-1)

#parameter from the command line
    k = int(sys.argv[1])
    rows= int(sys.argv[2])
    columns = int(sys.argv[3])
    threshold =float(sys.argv[4])
    mst= sys.argv[5]
    input_file= sys.argv[6]
    output_file= sys.argv[7]

    print("Number of cluster: "+ str(k))
    print("Number of vector: " + str(rows))
    print("Dimension of vectors: "+ str(columns))
    print("Threshold: "+  str(threshold))
    print("Input file: "+input_file)
    print("Output file: "+ output_file)


#initialize spark context
    sc = SparkContext(appName="K-means", master=mst)
    sc.setLogLevel("WARN")
    
    iteration= 1

    lines = sc.textFile(input_file)

    start_time = time.time()
    datapoints=lines.map(lambda point : create_vector(point))
    datapoints.cache()

    #select random centroids
    withReplacement = False
    numberToTake = k
    randomSeed =int(time.time())
    centroids=datapoints.takeSample(withReplacement, numberToTake, randomSeed)

    finish= False

    while not finish:
        data_assigned = datapoints.map(lambda point : assign_to_cluster(point, centroids)) #this should return a (assigned_cluster, datapoint) pair
        grouped= data_assigned.groupByKey().mapValues(list)

        new_centroids_rdd= grouped.map(lambda rdd: vector_sum(rdd))
        new_centroids= new_centroids_rdd.collect()
        finish=True

        #antiloop fix
        new_centroids.sort(key= lambda x: x[0])
        print("ITERAZIONE ",iteration,": ",  new_centroids)

        #stop condition
        for i in range(k):
            if distance(new_centroids[i], centroids[i])>threshold:
                finish=False

        centroids=new_centroids

        iteration+=1

    #output step
    data_assigned.saveAsTextFile(output_file+"-1")
    new_centroids_rdd.saveAsTextFile(output_file)

    #write test, iteration , time
    stop_time= time.time()
    time_eff=stop_time-start_time
    r=open("risultati.txt", "a")
    r.write(str(rows)+ ", "+str(columns)+ ", "+str(k)+", "+str(iteration)+", "+str(time_eff)+"\n")
    r.close()