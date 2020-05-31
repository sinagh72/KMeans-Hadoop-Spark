import random
import math
import numpy as np
import matplotlib.pyplot as plt

def distance(x_i,mu):
    total=0
    print("mu",mu)
    print("x_i", x_i)
    for d in range(0,len(x_i)):
        diff=x_i[d]-mu[d]
        total+=diff*diff
    return total

def min_distance(x_i, mus):
    print("mus",mus)
    min_dist=2000000
    c=-1
    k=0
    for mu in mus:
        dist=distance(x_i,mu)
        print("dist",dist)
        if(dist< min_dist):
            min_dist=dist
            c=k
        k+=1

    return c

# Performs sampling of means
def mus_sampling(inputs):
    mus=[]
    for i in range(0,k):
        tmp_index=random.randint(0,len(inputs))
        mus.append(inputs[tmp_index].copy())

    return mus

# Returns a list of k empty lists
def get_new_clusters(k):
    clusters=[]
    for i in range(0,k):
        clusters.append([])

    return clusters

# Creates and returns a clusters list, and populates it with the nearest datapoints to each centroid mus
def clusterize(x, mus, k):
    clusters=get_new_clusters(k)
    for datapoint in x:
        c=min_distance(datapoint,mus)
        print("c",c)
        clusters[c].append(datapoint.copy())
        print("clusters", clusters)
    return clusters

# Computes new clusters by averaging the datapoints in the list: clusters; Compares new centroids with old ones to decide whether to stop the training process
def compute_new_clusters(clusters, total_features, mus):
    break_loop=True

    new_mus=[]
    idx=0
    for cluster in clusters:
        new_mu=[]
        for d in range(total_features):
            tmp_total=0
            for cluster_datapoint in cluster:
                tmp_total+=cluster_datapoint[d]
            tmp_avg=tmp_total/len(cluster)
            new_mu.append(tmp_avg)
        
        if(new_mu != mus[idx]):
            break_loop=False

        new_mus.append(new_mu)
        idx+=1

    return new_mus, break_loop

# Performs K-Means algorithm
def kmeans(x, mus, k, total_features):

    break_loop=False
    iteration=0
    
    while (not break_loop):

        # Creation of clusters set
        clusters=clusterize(x,mus, k)

        # Compute new clusters
        new_mus, break_loop=compute_new_clusters(clusters, total_features, mus)

        # Convert to Numpy for plotting
        x_array=np.array(x)
        np_mus=np.array(mus)
        print(np_mus[:,0])
        np_new_mus=np.array(new_mus)

        print("new_mus",new_mus)
        plt.scatter(x_array[:,0],x_array[:,1])
        plt.scatter(np_mus[:,0],np_mus[:,1], c="green")
        plt.scatter(np_new_mus[:,0],np_new_mus[:,1], c="red")

        plt.show()

        if(iteration>5):
            break

        mus=new_mus
        iteration+=1
    return mus


x=[[1,5],[1.2,4.4],[1.8,4],[2,5],[2,1],[3,2],[2.5,2.5],[3.6,1.6],[4,2],[6.5,4.5],[7,4],[7,5],[7.5,3.5],[7.5,4.5]]

k=3
total_features=2

# Sample k means (mus)
mus=mus_sampling(x)

print("mus",mus)

centroids=kmeans(x,mus, k, total_features)

print("centroids",centroids)
    




