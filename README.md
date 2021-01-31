## Description 

Distributed application managing a DHT (Distributed Hash Table) with global table divided into partitions, storing each partition in different servers of the distributed application.
Each server manages a partition of the global table, which holds a range of values in hash function space (the size of the range is the same for each node).
The servers have information that relates the partition of the data to the IP address of the server that manages the corresponding table.

## Features
* Availability, consistency and fault tolerance properties are based on [Zookeeper](https://zookeeper.apache.org/).
* When a server is down, a new server is created to replace it and allow the application to continue to function. Updated tables are sent to the new server.

## Instructions
1. Clone the project
2. ```cd DHT_Zookeeper```
3. Include Zookeeper libraries in the Eclipse project.
4. Generate jar file in target folder
5. Run ```java -jar target/<filename>.jar``` from three different terminals (quorum)

## Known issues
* Systems blocks when trying to perform get or containsKey operation on a key that doesn't exist
* There are cases where the tables are not correctly transfered to a new server.
