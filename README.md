# MapR Docker Sandbox Demo
Sample applications that can run against MapR Sandbox to help one get started on MapR.

There are essentially 2 modules here -
1. **[drill-db-demo](https://github.com/aravi5/docker-sandbox-demo/tree/master/drill-db-demo)** - This module contains sample applications that demonstrate how Ojai and Drill JDBC based applications can interact with MapR and query data that resides in MapR-DB JSON.
2. **[spark-db-demo](https://github.com/aravi5/docker-sandbox-demo/tree/master/spark-db-demo)** - This module contains sample applications that demonstrate how Spark based applications can interact with MapR-DB JSON using Spark-DB JSON connector.


## Steps to run

### 1. Spin up MapR Developer Sandbox Container
Steps to run MapR Developer Sandbox in container is available here. This is a pre-requisite before running any of these applications. The developer sandbox comes with a MapRDB-JSON table, pre-loaded with **Yelp Data**. The applications in this project will query against this table `/demoVol/business_table`.

### 2. Clone current project

### 3. Import project into IDE
Import project into your favorite IDE (Eclipse, IntelliJ etc.) 

### 4. Build and Run any of the classes
