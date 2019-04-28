# C4

## Prep phase

Additionally the following environment variables can be set:

```bash
export INSERT_COMMITS_AFTER=1000
export PREP_PHASE_CHUNK_SIZE=5000
export TEMP_TABLE_SIZE_RATIO=0.66
```

```bash
#!/bin/bash
./bombard \
    -v=2 \
    -stderrthreshold=INFO \
    --host localhost \
    --experiment-host localhost \
    --username root \
    --password <password> \
    --port 3306 \
    --database bigbasket \
    --tablename membercommunication_membercommunicationlog \
    --prepare


```

## Run phase

The run phase will essentially select rows from the original table and perform CRUD operations on the temporary table. The `MasterPublishController` and the `MasterSubscriberController` controls the number of instances of publisher and consumers spun.
The data resides in the `bus`.
note: the `bus` is simply a channel of type

```go
*sql.Rows
```

### Publisher

The publisher is responsible for selecting rows from the original table and publishing to the bus. The chunk size of rows selected from the original table is controlled by `readChunkSize`. The `readChunkSize` is supplied as a pointer to the publisher instances by the `MasterPublishController`. The sleep time after each select query is also supplied as a pointer to the publisher instances by the `MasterPublishController`. The `MasterPublishController` will maintain a channel of boolean type for signalling any publisher instance to stop.

The `MasterPublishController` can control the rate of publishing data to the bus in 2 ways:

- Increasing/Decreasing the number of publisher instances
- Increasing/Decreasing the sleep time for every publisher
- Increasing/Decreasing the readChunkSize of each publisher instance

The crux of the problem is really deciding when to do either of the above 2 operations. The following parameters will be available at the disposal of the `MasterPublishController` to decide the same:

- Number of publisher instances running
- readChunkSize for each publish instance
- sleepTime for each publish instance
- the calls per minute value for each type of operation
- the average wait time of the each type of operation
- number of consumers instances running

Using these 6 parameters it is possible to make an approximate assumption of when to increase/decrease the number of running publish instances or the sleep time for each publisher.

**Algorithm**
Lets say there are 3 publisher instances running with a readChunkSize of 1000 and a sleepTime of 500 milliseconds. Our goal is to decide whether the publisher is being a bottleneck in the throughput the queries on the temporary table(i.e. whether the consumers are waiting frequently on the bus for more data).

For each query type, we fetch the average wait time(converted to minutes) and divide 1 minute by this average wait time. Let this be X. X is now the approximate total number of queries hit by one consumer in 1 minute. We multiply this value with the total number of consumers and this gives us the expected calls per minute value. If the actual calls per minute value for that type of query is less than this expected number, we can decide to do either of the 3 things mentioned a while back
