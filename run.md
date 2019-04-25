### Prep phase

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
