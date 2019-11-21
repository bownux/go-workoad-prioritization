 
Workload Prioritization helps with the orchestration of multiple workloads or sources accessing the database so that during critical peaks or even everyday usage of the database. The idea is that you may have certain business requirements for different applications or users accessing your database, a common case being an analytics use-case versus a more real-time transaction. 

Scylladb Enterprise allows you to create something called a SERVICE_LEVEL which represents certain number shares of resources granted to that SERVICE_LEVEL ranging from 1 to 1000 (where 1000 is 100% of the max resources it can access at any point in time).

When creating the users for each of your sources accessing your database, ROLES can be created and GRANTED to these users (or users themselves can be a ROLE). Each ROLE can represent your different access needs. For this to work, the ROLES will need to be ATTACH to a SERVICE_LEVEL so that now each ROLE respectively represents different levels of resources granted to them.

As an example:

1. Create SERVICE_LEVEL
2. Create USERS
3. Create ROLES
    - 
4. Attach SERVICE_LEVEL to ROLE

Benefits of Workload Prioritization:
- Noisy Neighbors Problem
- Cost Optimization

Sudden Reduction of Resources:
...

Name	Zone	Recommendation	In use by	Internal IP	External IP	Connect	
 luis-workload-1	us-west1-b			10.240.0.166 (nic0)	
35.230.115.159
 	
 luis-workload-2	us-west1-b			10.240.0.167 (nic0)	
35.233.239.4
 	
 luis-workload-3	us-west1-b			10.240.0.168 (nic0)	
35.233.231.102
 	
 luis-workload-4	us-west1-b			10.240.0.18 (nic0)	
35.247.98.217
 	
 luis-workload-5	us-west1-b			10.240.0.173 (nic0)	
34.82.38.151
 	
 luis-workload-6	us-west1-b			10.240.0.174 (nic0)	
34.82.177.160
 	
 luis-workload-loader-1	us-west1-b			10.240.0.177 (nic0)	
34.82.128.76
 	
 luis-workload-loader-2	us-west1-b			10.240.0.178 (nic0)	
34.82.200.144
 	
 luis-workload-loader-3	us-west1-b			10.240.0.179 (nic0)	
35.247.52.234
 	
 luis-workload-loader-4	us-west1-b			10.240.0.44 (nic0)	
34.82.50.171
 	
 luis-workload-monitor-1	us-west1-b			10.240.0.176 (nic0)	
35.227.150.171

 	
Service Levels:
cqlsh -uscylla -p123456 10.240.0.18

LIST ALL SERVICE_LEVELS;
 service_level | shares
---------------+--------
          olap |   1000
      pipeline |    100
       service |    500

ALTER SERVICE_LEVEL OLAP WITH SHARES = 1000;
ALTER SERVICE_LEVEL PIPELINE WITH SHARES = 1000;
ALTER SERVICE_LEVEL SERVICE WITH SHARES = 1000;

Grafana:

worker_benchmarks_total_get_iterations_count

sum(
  rate(worker_benchmarks_total_get_time_sum[5m])
  /
  rate(worker_benchmarks_total_get_time_count[5m])
)

avg by (benchmark_id) (rate(worker_benchmarks_total_insert_time_sum[5m])/rate(worker_benchmarks_total_insert_time_count[5m]))


Workloads:

Loader (Inserts):
GOMAXPROCS=8 ./go-workoad-prioritization --workers 36 --iterations 1000 --benchmarks insert --cluster "10.240.0.166,10.240.0.168,10.240.0.173" --connections 2 --username loader1 --password 123456 -rf 3 

Analytics (Scans):
GOMAXPROCS=8 ./go-workoad-prioritization --workers 8 --iterations 200 --benchmarks select --cluster "10.240.0.166,10.240.0.167,10.240.0.168,10.240.0.18,10.240.0.173,10.240.0.174" --connections 2 --username analytics1 --password 123456 -rf 3

Service (Gets):
GOMAXPROCS=8 ./go-workoad-prioritization --workers 36 --iterations 1000 --benchmarks get --cluster "10.240.0.166,10.240.0.168,10.240.0.173" --connections 2 --username web1 --password 123456 -rf 3