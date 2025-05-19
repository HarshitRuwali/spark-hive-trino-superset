# Hive - Spark - Trino - Superset

This repo contains the setup for the hive-spark-trino-superset. Where hive will spark will read the parquet files stored over S3 and form a tables for the same in Hive and which inturn can be queried by Trino and using superset's connector of trino, analytics can be done over superset.


## Syncing the tables from data saved as parquet in s3 to hive

1. Copy the init_hive_tables.py script to the spark-master container <br> `docker cp ./spark/jobs/init_hive_tables.py spark-hive-trino-superset-spark-master-1:/opt/bitnami/spark`

2. Get the shell in the spark master container <br> `docker exec -it spark-hive-trino-superset-spark-master-1 bash`

3. navigate to the spark folder where the script which we copy resides <br>
    `cd /opt/bitnami/spark`

4. Execute the script using the spark-submit <br> `spark-submit init_hive_tables.py`



## Troubleshooting

### hive-metastore container issue
If getting the error while starting the hive-metastore server, then make sure there are enough permissions for the container to read and write in the mounted folders.


```bash
sudo chown 1001:1001 ./spark/conf/spark-env.sh
sudo chmod 664 ./spark/conf/spark-env.sh
sudo chown -R 1001:1001 ./spark/conf
sudo chmod -R u+rw ./spark/conf
```


### superset-trino connection issue

Natively superset does not have trio's connector's driver installed. Which can be done by getting the shell in the superset app container where the actual backend code lies.

```bash
# get the shell
docker exec -it spark-hive-trino-superset-superset-app-1 bash

# isntaling the driver
pip install --no-cache-dir trino[sqlalchemy]
```
