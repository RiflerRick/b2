# B2

## Prep phase

```bash
docker-compose up c4-debug -f prep-docker-compose.yml
```

## Bombard with itself

DB benchmarking tools like sysbench and the like prepare their own tables, generate queries for their tables and hit those tables with it. If you know what your table is, you know what kind of data your table expects. However in this case, we are attempting to bombard a table whose structure we are not aware of beforehand.
The table data itself can help us in bombarding the table with it. The following approach is therefore followed to do the same:

prep-phase: we copy a percentage of data from the original table to a new temporary table preferably in a separate mysql database instance. Lets say this data starts from id x and goes till id y. Now from the original table, we can select rows between id x and id y and hit the temporary table with that data itself. This is important for a couple of reasons. First of all, if there are foreign keys of this table to any other table, randomly hitting such foreign key columns with values may result in an unprecendented number of foreign key constraint fails which may adversely affect the throughput, Moreover generating exactly the kind of data that may be stored in a column is a hard problem.
The fact that we are trying to bombard queries of the same data as the original table may lead to an unprecedented number of unique/candidate key constraint fails as well which may also adversely affect the throughput. To handle this, just before the run phase, the chunk of data to be used for the run phase is taken as a chunk that appears before the prep chunk. This way, we can be sure there will be no clashes

## Run phase

The run phase will essentially select rows from the original table and perform CRUD operations on the temporary table. The `MasterPublishController` and the `MasterSubscriberController` controls the number of instances of publisher and consumers spun.
The data resides in the `bus`.
note: the `bus` is simply a channel of type

```go
*sql.Rows
```

### Publisher

**We would always be having one subscriber**

The publisher is responsible for selecting rows from the original table and publishing to the bus. The chunk size of rows selected from the original table is controlled by `readChunkSize`. The `readChunkSize` is supplied as a pointer to the publisher instances by the `MasterPublishController`. The sleep time after each select query is also supplied as a pointer to the publisher instances by the `MasterPublishController`. The `MasterPublishController` will maintain a channel `stopSignal` of boolean type for signalling any publisher instance to stop.

The `MasterPublishController` can control the rate of publishing data to the bus in 3 ways:

- Increasing/Decreasing the number of publisher instances
- Increasing/Decreasing the sleep time for every publisher
- Increasing/Decreasing the readChunkSize of each publisher instance

In any case, the bus cannot be empty, from the consumer instances, if the bus is empty, it sends the query type as a string to the channel that is received by the `MasterPublishController` and the `MasterSubscribeController`.

#### Upscaling the publisher instances

For now the straightforward solution is to simply spawn a new publish routine in case the any of the consumer instances notify that the bus is empty. Although later on more intelligence need to be provided when deciding the to do any of increasing/decreasing the number of publish instances, increasing/decreasing the sleep time for every publisher or increasing/decreasing the readChunkSize of each publisher instance.

```text
improvements req: control sleep time, control readChunkSize
```

#### Downscaling the publisher instances

downscaling will be based on calls per minute(or unit time in that case), if the calls per unit time is more than required, it will attempt to downscale the publishers

## Metadata(wait time and calls per unit time) time series

A metadata time series is maintained which records average wait time for queries of a particular queryType and average calls per unit time averaged over a `windowSize` time period. The `windowSize` is critical here as it makes sure, the calls per unit time and wait time values for the corresponding queryType does not get affected by momentary blips

### Subscriber

**We would always be having atleast one subscriber**

#### Upscaling and downscaling the subscriber instances

Here we introduce 1 new parameter: `decisionWindow`.
The `MasterSubscribeController` polls the Metadata time series every `decisionWindow` intervals. The `decisionWindow` would typically be a multiple of the metadata time series `windowSize`.
The `MasterSubscriberController` gets the latest available CPM available in the metadata time series, it also gets the max of the wait times for the entire decision window, if the latest wait time in the metadata time series is more than this max wait time over the decision window, a `potentialDownscaleDecision` is taken. After this the latest CPM available is compared with the desired CPM, if the latest CPM is lower than the desiredCPM, a `potentialUpscaleDecision` is taken. A `potentialDownscaleDecision` is always preferred over a `potentialUpscaleDecision`.

TODOs:

- metadata controller locking must be removed instead use channels(message passing always wins over locking)
- bulk ops at run phase
- support of multiple tables for joins. This can be done using go-templates. normalized queries can be provided by the user, getQuery will no longer be used to generate queries, follow (this)[https://golang.org/pkg/text/template/] for reference
