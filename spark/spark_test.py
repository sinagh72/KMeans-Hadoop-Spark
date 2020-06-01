from pyspark import SparkContext
import random
import numpy as np
import time
import argparse
parser = argparse.ArgumentParser()

parser.add_argument("-f","--filename",help="Path to the inputs file", default="")
parser.add_argument("-m","--master",help="Configure parameter 'master' to be passed to SparkContext", default="local[*]")
parser.add_argument("-t", "--test",
                    help="Generate test data", action="store_true", default=False)
parser.add_argument("-d","--dims", type=int,help="Specify the amount of dimensions for each input element of synthetic data", default=3)
parser.add_argument("-l","--limit", type=int,help="Specify the upper limit of each feature (dimension) of synthetic data", default=20)
parser.add_argument("-q","--quantity", type=int,help="Specify the amount of synthetic data", default=1000)
parser.add_argument("-k", type=int,help="Specify the amount of k means", default=7)
parser.add_argument("-g", "--graph",
                    help="Show matplotlib graph", action="store_true", default=False)
parser.add_argument("-v", "--verbosity",
                    help="increase output verbosity", action="store_true", default=False)
args = parser.parse_args()

if(args.graph):
    import matplotlib.pyplot as plt

def get_inputs(filename="",test=True, quantity=20, dims=2, limit=20):

    if(test):
        return [[1,5],[1.2,4.4],[1.8,4],[2,5],[2,1],[3,2],[2.5,2.5],[3.6,1.6],[4,2],[6.5,4.5],[7,4],[7,5],[7.5,3.5],[7.5,4.5]]

    inputs=[]
    if(filename==""):
        for i in range(0,quantity):
            element=[]
            for j in range(0, dims):
                feature=random.randint(0,limit)
                element.append(feature)
            inputs.append(element)
        return inputs
    else:
        # load from file
        with open(filename, "r") as f:
            lines=f.readlines()

            for line in lines:
                line=line[:-1].split(",")[1:]
                features=[]
                for feature in line:
                    features.append(int(feature))
                inputs.append(features)
        return inputs

def distance(x_i,mu):
    total=0
    # print("mu",mu)
    # print("x_i", x_i)
    for d in range(0,len(x_i)):
        diff=x_i[d]-mu[d]
        total+=diff*diff
    return total

def get_break_loop(mus, new_clusters):
    mus.sort()
    new_clusters.sort()
    break_loop=True

    for idx,mu in enumerate(mus):
        if(mu != new_clusters[idx]):
            break_loop=False
            return break_loop
    
    return break_loop

k=args.k

sc=SparkContext(appName="test", master=args.master)


quantity=args.quantity
dims=args.dims
limit=args.limit

# Params of get_inputs: filename,test, quantity, dims, limit
inputs=get_inputs(args.filename,args.test,quantity,dims, limit)
# print(inputs)
# quit()

sp_inputs=sc.parallelize(inputs)
# Start measuring time
start = time.time()

withReplacement = False
numberToTake = k
randomSeed = 123456
mus=sp_inputs.takeSample(withReplacement, numberToTake, randomSeed)
# print(mus)

iteration=0
break_loop=False
while(not break_loop):
    print("iteration: " + str(iteration))
    # diff=sp_inputs.map(lambda x: (np.array([distance(x,mu) for mu in mus]).argmin(),x)).reduceByKey(lambda aggregated_dp,dp: list(map(lambda x,y: x+y, aggregated_dp, dp)))
    diff=sp_inputs.map(lambda x: (np.array([distance(x,mu) for mu in mus]).argmin(),(x,1)))
    # print(diff.collect())

    def aggregate(aggregated_dp,dp):
        total=aggregated_dp[1]+dp[1]

        return (list(map(lambda x,y: x+y, aggregated_dp[0], dp[0])),total)

    totals=diff.reduceByKey(lambda aggregated_dp,dp:  aggregate(aggregated_dp,dp)   )
    # print("totals",totals.collect())

    new_clusters=totals.map( lambda element: list(map(lambda x: x/element[1][1] ,element[1][0]) ) )

    # print("new_clusters",new_clusters.collect())

    # Convert to Numpy for plotting
    if(args.graph):
        x_array=np.array(inputs)
        np_mus=np.array(mus)
        np_new_mus=np.array(new_clusters.collect())

        plt.scatter(x_array[:,0],x_array[:,1])
        plt.scatter(np_mus[:,0],np_mus[:,1], c="green")
        plt.scatter(np_new_mus[:,0],np_new_mus[:,1], c="red")

        plt.show()

    break_loop=get_break_loop(mus, new_clusters.collect())

    mus=new_clusters.collect()
    iteration+=1

end = time.time()

print("centroids: ", mus)

print("Time ellapsed: " + str(end - start) + " seconds")
