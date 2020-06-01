import sys
from operator import add
import random
from pyspark import SparkContext

def create_vector(line):
    vector= []
    vector= line.split(",")
    for i in range(len(vector)):
        vector[i] = float(vector[i])
    return (int(vector[0]), vector[1:])

def select_random_centroid(point, indexes):
    if point[0] in indexes:
        return [point] #key, value
    return []

def assign_to_cluster(point, centroids):
    #compute the distance from each centroid and output the key corrisponing to the nearest
    idx=0
    min_dist=float("inf")
    for centroid in centroids:
        dist=distance(point[1], centroid[1]) #they are key-value element
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
    key=key_value[0]
    points= key_value[1]
    count= len(points)
    new_centroid=[sum(x)/count for x in zip(*[y[1] for y in points])]
    return (key, new_centroid)



if __name__ == "__main__":
    if len(sys.argv) !=7 :
        print("Usage: K-means <k> <rows> <columns> <threshold> <input file> <output file>", file=sys.stderr)
        sys.exit(-1)

#parameter from the command line
    k = int(sys.argv[1])
    rows= int(sys.argv[2])
    columns = int(sys.argv[3])
    threshold =float(sys.argv[4])
    input_file= sys.argv[5]
    output_file= sys.argv[6]

    print("Number of cluster: "+ str(k))
    print("Number of vector: " + str(rows))
    print("Dimension of vectors: "+ str(columns))
    print("Threshold: "+  str(threshold))
    print("Input file: "+input_file)
    print("Output file: "+ output_file)


#initialize spark context
    master = "local"
    sc = SparkContext(master, "K-means")
    sc.setLogLevel("WARN")
    #set a maximum number of iteration
    iteration= 10

    lines = sc.textFile(sys.argv[5])

    datapoints=lines.map(lambda point : create_vector(point))
    #select random indexes
    indexes= random.sample(range(rows), k)

    #extract centroid from the datapoint
    centroids = datapoints.flatMap(lambda point : select_random_centroid(point, indexes)).collect()
    finish= False

    #print("CENTROID_RANDOM: ", centroids)
    #print("INDICI: ",indexes)

    while iteration>0 and not finish:
        data_assigned = datapoints.map(lambda point : assign_to_cluster(point, centroids)) #this should return a (assigned_cluster, datapoint) pair
        groupped= data_assigned.groupByKey().mapValues(list)
        #print("GROUPPED: ",groupped.collect())
        new_centroids= groupped.map(lambda rdd: vector_sum(rdd)).collect()

        finish=True

        #print("ITERAZIONE ",iteration,": ",  new_centroids)

        #stop condition
        for i in range(k):
            if distance(new_centroids[i][1], centroids[i][1])>threshold:
                finish=False

        centroids=new_centroids

        iteration-=1


    print(centroids)