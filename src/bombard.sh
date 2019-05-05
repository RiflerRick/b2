#!/bin/bash
../bin/bombard \
    -v=2 \
    -stderrthreshold=INFO \
    --host localhost \
    --experiment-host localhost \
    --username root \
    --password root \
    --port 3306 \
    --database bigbasket \
    --tablename membercommunication_membercommunicationlog \
    --prepare 

