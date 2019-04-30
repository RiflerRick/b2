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

The publisher is responsible for selecting rows from the original table and publishing to the bus. The chunk size of rows selected from the original table is controlled by `readChunkSize`. The `readChunkSize` is supplied as a pointer to the publisher instances by the `MasterPublishController`. The sleep time after each select query is also supplied as a pointer to the publisher instances by the `MasterPublishController`. The `MasterPublishController` will maintain a channel `stopSignal` of boolean type for signalling any publisher instance to stop.

The `MasterPublishController` can control the rate of publishing data to the bus in 3 ways:

- Increasing/Decreasing the number of publisher instances
- Increasing/Decreasing the sleep time for every publisher
- Increasing/Decreasing the readChunkSize of each publisher instance

In any case, the bus cannot be empty, from the consumer instances, if the bus is empty, it sends the query type as a string to the channel that is received by the `MasterPublishController` and the `MasterSubscribeController`. The following parameters will be available at the disposal of the `MasterPublishController` to decide the what to do in case of such an incident:

- Number of publisher instances running
- readChunkSize for each publish instance
- sleepTime for each publish instance
- the calls per minute value for each type of operation
- the average wait time of the each type of operation
- number of consumers instances running

**Upscaling the publisher instances**
For now the straightforward solution is to simply spawn a new publish routine in case the any of the consumer instances notify that the bus is empty. Although later on more intelligence need to be provided when deciding the to do any of increasing/decreasing the number of publish instances, increasing/decreasing the sleep time for every publisher or increasing/decreasing the readChunkSize of each publisher instance.

**Downscaling the publisher instances**
From the CPM for each query type it is possible to get the average wait time for each query type. For CRUD operations, lets assume, the average wait time for each operation is a,b,c and d respectively. We take the average of a,b,c and d. Let this be `x`.
We also know the actual average wait times for each query type. Let these values be p,q,r and s respectively. We take the average of p,q,r and s. Let this be `y`
If `y` < `x`, the `MasterPublishController` will issue a signal through the `stopSignal` channel. This will stop any one go routine

## Subscriber

The subscriber will be responsible for
