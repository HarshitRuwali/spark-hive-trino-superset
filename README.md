# Hive - Spark - Trino - Superset

This repo contains the setup for the hive-spark-trino-superset. Where hive will spark will read the parquet files stored over S3 and form a tables for the same in Hive and which inturn can be queried by Trino and using superset's connector of trino, analytics can be done over superset.

## Config

### S3
Create the IAM User for S3 having read write acesss the the bucket you going to store the parquet files to, and update the client-id and client-secret in the config files, 

`spark/conf/core-site.xml` and `trino/etc/catalog/hive.properties`

### postgresql
Update the postgres credentials in `spark/conf/hive-site.xml` for ConnectionURL, ConnectionUserName and ConnectionPassword.

## Syncing the tables from data saved as parquet in s3 to hive

1. Copy the init_hive_tables.py script to the spark-master container <br> `docker cp ./spark/jobs/init_hive_tables.py spark-hive-trino-superset-spark-master-1:/opt/bitnami/spark`

2. Get the shell in the spark master container <br> `docker exec -it spark-hive-trino-superset-spark-master-1 bash`

3. navigate to the spark folder where the script which we copy resides <br>
    `cd /opt/bitnami/spark`

4. Execute the script using the spark-submit <br> `spark-submit init_hive_tables.py`



## TLS cert

To create the ssl cert, we will create the certificate locally by running, 
```bash
mkdir -p certs && cd certs

openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=trino.yourdomain.com"

openssl pkcs12 -export -in cert.pem -inkey key.pem -out keystore.p12 -name trino -passout pass:change

keytool -importkeystore \
  -deststorepass change_it \
  -destkeypass change_it \
  -destkeystore keystore.jks \
  -srckeystore keystore.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass change_it \
  -alias trino
```

And update the password you set it to in the file `trino/etc/config.properties` for the key, `http-server.https.keystore.key`


### Using Nginx as reverse proxy?

If using nginx as the reverse proxy, then have the nginx configuration as below, and issue a self-signed certificate using any provider you like, I presonally use certbot. 
```nignx
server {
        server_name trino.yourdoamin.com;

        location / {
                proxy_ssl_verify off;
                proxy_set_header X-Forwarded-Proto https;
                proxy_set_header Authorization $http_authorization;
                proxy_set_header Host $host;
                proxy_pass http://localhost:8088;
                proxy_http_version 1.1;
                proxy_set_header Upgrade $http_upgrade;
                proxy_set_header Connection "upgrade";
        }

    listen 443 ssl; # managed by Certbot
    ssl_certificate /etc/letsencrypt/live/trino.yourdomain.com/fullchain.pem; # managed by Certbot
    ssl_certificate_key /etc/letsencrypt/live/trino.yourdomain.com/privkey.pem; # managed by Certbot
    include /etc/letsencrypt/options-ssl-nginx.conf; # managed by Certbot
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem; # managed by Certbot

}


server {
    if ($host = trino.yourdomain.com) {
        return 301 https://$host$request_uri;
    } # managed by Certbot

    server_name trino.yourdomain.com;
    listen 80;
    return 404; # managed by Certbot

}
```

### Using cloudfare for domain?

If using cloudfare for domain then turn of the proxied toggle for the subdomain, this way you wont be having infinite redirects.


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
