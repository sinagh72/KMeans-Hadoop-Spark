#!/bin/bash

for n in 1000 10000 100000
do
    for d in  3 7
    do
        for k in 7 13
        do
            for i in 1 2 3 4 5
            do
            start-yarn.sh
            start-dfs.sh
            hdfs dfsadmin -safemode leave
            sleep 30
            spark-submit k-means.py ${k} ${n} ${d} 0 yarn hdfs://hadoop-namenode:9820/user/hadoop/input/k-7-${n}-${d}.txt hdfs://hadoop-namenode:9820/user/hadoop/output/output-${k}-${n}-${d}-${i}
            hadoop fs -rm -r .sparkStaging/*
            hadoop fs -rm -r output/*
            stop-yarn.sh
            stop-dfs.sh
            sleep 2
            rm -rf /opt/yarn/local/usercache/*
            rm -r /opt/hadoop/logs/*
            sleep 2
            done
        done
    done
done






