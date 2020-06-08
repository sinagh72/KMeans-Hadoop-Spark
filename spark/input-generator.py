import numpy as np 
val = 1000000 

for k in [7,13]:
    for m in [3,7]:
        for n in [1000,10000,100000, 1000000]:            
            A = np.random.randint(val/2, size=(n, m))  

            B = np.zeros((n,m+1))  

            B[:,1:] = A  

            B[:,0:1] = np.arange(0, n).reshape((n,1))  

            np.savetxt('k-'+str(k)+'-'+str(n)+'-'+str(m)+'.txt',B,fmt='%.0f',delimiter=",")










