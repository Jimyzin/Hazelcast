# Hazelcast

## Coding Challenge:
Imagine an environment that consists of multiple nodes. Each node is a separate JVM process and could potentially be running on a distinct physical machine. Your task is to write an application that will run on all nodes. The application should coordinate between the nodes so that as they are started System.out.println("We are started!") is only called exactly once across the whole cluster, whether 1 node or 10 are running.
As we are discussing a distributed environment, your solution should take into consideration the variables that such an environment presents, such as:
Some nodes may start at the exact same time as others.
Some nodes may start seconds or minutes later than others.
Some nodes may restart before others start a first time.
Some nodes may never be started at all.
There is no need to build a distributed system from scratch. You can use an already existing library for the solution. We highly value simplicity.