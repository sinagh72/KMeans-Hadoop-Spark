from pyspark import SparkContext
import numpy as np

def distance(x_i,mu):
    total=0
    # print("mu",mu)
    # print("x_i", x_i)
    for d in range(0,len(x_i)):
        diff=x_i[d]-mu[d]
        total+=diff*diff
    return total

k=3

sc=SparkContext(appName="test", master="local[*]")

inputs=[[1,5],[1.2,4.4],[1.8,4],[2,5],[2,1],[3,2],[2.5,2.5],[3.6,1.6],[4,2],[6.5,4.5],[7,4],[7,5],[7.5,3.5],[7.5,4.5]]

sp_inputs=sc.parallelize(inputs)

withReplacement = False
numberToTake = k
randomSeed = 123456
mus=sp_inputs.takeSample(withReplacement, numberToTake, randomSeed)
print(mus)

iteration=0
while(iteration<5):
    # diff=sp_inputs.map(lambda x: (np.array([distance(x,mu) for mu in mus]).argmin(),x)).reduceByKey(lambda aggregated_dp,dp: list(map(lambda x,y: x+y, aggregated_dp, dp)))
    diff=sp_inputs.map(lambda x: (np.array([distance(x,mu) for mu in mus]).argmin(),(x,1)))
    print(diff.collect())

    def aggregate(aggregated_dp,dp):
        total=aggregated_dp[1]+dp[1]

        return (list(map(lambda x,y: x+y, aggregated_dp[0], dp[0])),total)

    totals=diff.reduceByKey(lambda aggregated_dp,dp:  aggregate(aggregated_dp,dp)   )
    print("totals",totals.collect())

    new_clusters=totals.map( lambda element: list(map(lambda x: x/element[1][1] ,element[1][0]) ) )

    print("new_clusters",new_clusters.collect())

    mus=new_clusters.collect()
    iteration+=1

print("final mus", mus)
