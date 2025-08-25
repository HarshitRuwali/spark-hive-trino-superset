# Hive - Spark - Trino - Superset

This repo contains the setup for the hive-spark-trino-superset. Where hive will spark will read the parquet files stored over S3 and form a tables for the same in Hive and which inturn can be queried by Trino and using superset's connector of trino, analytics can be done over superset.

## Architecture

### Trino Cluster Configuration
The setup includes a distributed Trino cluster with:
- **1 Trino Coordinator** (port 8088) - handles query planning and client connections
- **2 Trino Workers** (internal only) - handle query execution and data processing

This multi-worker setup enables concurrent query processing and improved performance for large-scale analytics workloads.

### Services Overview
- **Hive Metastore**: Stores metadata for Spark tables
- **Spark Master + 2 Workers**: Processes data from S3 and creates Hive tables
- **Trino Coordinator + 2 Workers**: Distributed query engine for analytics
- **Superset** (optional): Web-based data visualization platform

## Recent Changes (August 2025)

### Trino Multi-Worker Setup
Upgraded from single Trino instance to a distributed coordinator-worker architecture:

#### Changes Made:
1. **Split Trino services in docker-compose.yml:**
   - `trino-coordinator`: Single coordinator instance with external ports (8088, 8443)
   - `trino-worker`: Multiple worker instances (2 replicas) for parallel processing

2. **Configuration Structure:**
   ```
   trino/
   ├── etc/              # Coordinator configuration
   └── etc-worker/       # Worker configuration
   ```

3. **Key Configuration Updates:**
   - **Coordinator** (`trino/etc/config.properties`):
     - `coordinator=true`
     - `discovery-server.enabled=true`
     - Exposed ports for external access
   
   - **Workers** (`trino/etc-worker/config.properties`):
     - `coordinator=false`
     - `discovery.uri=http://trino-coordinator:8080`
     - No exposed ports (internal communication only)
     - Separate data directories to avoid conflicts

4. **Spark Integration:**
   - Updated Dockerfile to install `trino` Python package
   - Enables Spark jobs to connect to Trino cluster via Python

5. **Authentication Setup:**
   - Configured password authentication for Trino coordinator
   - Created password database with bcrypt hashed passwords
   - Workers configured for internal communication without authentication
   - Removed SSL configuration from workers to avoid keystore issues

#### Benefits:
- **Improved Performance**: Parallel query execution across multiple workers
- **Scalability**: Easy to add more workers by increasing replica count
- **High Availability**: Coordinator handles client connections while workers process queries
- **Resource Optimization**: Better utilization of available compute resources

## Current Working Configuration

### Trino Cluster Status
- **Coordinator**: 1 instance with password authentication (port 8088/8443)
- **Workers**: 2 instances for parallel processing (internal communication only)
- **Authentication**: Username `p2p`, Password `password`
- **SSL**: Enabled on coordinator only, workers use HTTP internally

### Key Files:
```
trino/
├── etc/                           # Coordinator configuration
│   ├── config.properties         # Main coordinator config with auth
│   ├── password-authenticator.properties
│   └── password.db               # User passwords (bcrypt hashed)
└── etc-worker/                   # Worker configuration
    ├── config.properties         # Simplified worker config
    └── node.properties           # Worker node settings
```

### Connection Examples:
```bash
# Web UI Access
# Navigate to: http://localhost:8088
# Login: p2p / password

# CLI Access
trino --server https://localhost:8443 --user p2p --password --insecure

# Python Connection
import trino
conn = trino.dbapi.connect(
    host='localhost', port=8443, user='p2p', 
    password='password', http_scheme='https',
    auth=trino.auth.BasicAuthentication('p2p', 'password'),
    verify=False
)
```

## Config

#### Accessing the Trino Cluster:
- **Web UI**: http://localhost:8088 (coordinator interface)
- **HTTPS**: https://localhost:8443 (with SSL certificate)
- **Query Execution**: Connect to coordinator (port 8088), queries distributed to workers automatically

#### Accessing the Trino Cluster:
- **Web UI**: http://localhost:8088 (coordinator interface)
- **HTTPS**: https://localhost:8443 (with SSL certificate)
- **Query Execution**: Connect to coordinator (port 8088), queries distributed to workers automatically

#### Authentication:
The Trino coordinator is configured with password authentication:
- **Username**: `p2p`
- **Password**: `password` (bcrypt hashed in `/trino/etc/password.db`)
- **Configuration**: Password authenticator enabled in coordinator only
- **Workers**: No authentication required (internal communication only)

#### Monitoring:
```bash
# Check all services status
docker compose ps

# View Trino coordinator logs
docker compose logs trino-coordinator

# View Trino worker logs  
docker compose logs trino-worker

# Check active workers via API
curl -k -u p2p:password https://localhost:8443/v1/node

# Scale workers (optional)
docker compose up -d --scale trino-worker=3
```

## Production Deployment Guide

### Prerequisites
- Docker and Docker Compose installed
- Sufficient server resources (minimum 8GB RAM, 4 CPU cores)
- Valid SSL certificates for HTTPS access
- S3 bucket with appropriate IAM permissions
- PostgreSQL database for Hive Metastore

### Step 1: Environment Setup

#### 1.1 Clone Repository
```bash
git clone https://github.com/HarshitRuwali/spark-hive-trino-superset.git
cd spark-hive-trino-superset
```

#### 1.2 Create Production Environment Variables
```bash
# Create .env file for production settings
cat > .env << EOF
# Database Configuration
POSTGRES_DB=metastore
POSTGRES_USER=hive_prod
POSTGRES_PASSWORD=secure_password_here
POSTGRES_HOST=your-postgres-host
POSTGRES_PORT=5432

# S3 Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET=your-production-bucket
S3_REGION=us-east-1

# Trino Security
TRINO_SHARED_SECRET=generate_secure_32_char_secret
TRINO_KEYSTORE_PASSWORD=your_keystore_password

# Domain Configuration
TRINO_DOMAIN=trino.yourdomain.com
EOF
```

### Step 2: Configuration Updates

#### 2.1 Update S3 Credentials
```bash
# Update Spark configuration
vi spark/conf/core-site.xml
# Replace with your production S3 credentials

# Update Trino Hive catalog
vi trino/etc/catalog/hive.properties
# Replace with your production S3 credentials
```

#### 2.2 Update Database Configuration
```bash
# Update Hive configuration
vi spark/conf/hive-site.xml
# Update PostgreSQL connection details
```

#### 2.3 Configure SSL Certificates
```bash
# Option A: Use existing certificates
cp /path/to/your/cert.pem trino/certs/cert.pem
cp /path/to/your/key.pem trino/certs/key.pem

# Option B: Generate self-signed certificates
mkdir -p trino/certs && cd trino/certs
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes \
    -subj "/CN=trino.yourdomain.com"

# Create keystore
openssl pkcs12 -export -in cert.pem -inkey key.pem -out keystore.p12 \
    -name trino -passout pass:${TRINO_KEYSTORE_PASSWORD}

keytool -importkeystore \
    -deststorepass ${TRINO_KEYSTORE_PASSWORD} \
    -destkeypass ${TRINO_KEYSTORE_PASSWORD} \
    -destkeystore keystore.jks \
    -srckeystore keystore.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass ${TRINO_KEYSTORE_PASSWORD} \
    -alias trino

cd ../..
```

### Step 3: Production Docker Compose

#### 3.1 Create Production Compose File
```bash
# Create docker-compose.prod.yml
cat > docker-compose.prod.yml << EOF
version: '3.8'

services:
  hive-metastore:
    build: ./hive-metastore
    environment:
      HIVE_METASTORE_DB_TYPE: postgres
      HIVE_DB_NAME: \${POSTGRES_DB}
      HIVE_DB_USER: \${POSTGRES_USER}
      HIVE_DB_PASSWORD: \${POSTGRES_PASSWORD}
      SERVICE_NAME: metastore
      HDFS_NAMENODE_URI: ""
      CORE_CONF_fs_defaultFS: s3a://\${S3_BUCKET}
    ports:
      - "9083:9083"
    volumes:
      - ./spark/conf/core-site.xml:/opt/hive/conf/core-site.xml
      - ./spark/conf/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml
    networks:
      - prod-network
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9083"]
      interval: 30s
      timeout: 10s
      retries: 3

  spark-master:
    build: ./spark
    environment:
      - SPARK_MODE=master
    ports:
      - "7077:7077"
      - "8082:8080"
    volumes:
      - ./spark/conf:/opt/bitnami/spark/conf
      - ./spark/jobs:/opt/bitnami/spark/jobs
      - spark-warehouse:/opt/bitnami/spark/spark-warehouse
    networks:
      - prod-network
    restart: unless-stopped
    depends_on:
      hive-metastore:
        condition: service_healthy

  spark-worker:
    build: ./spark
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=4g
      - SPARK_WORKER_CORES=2
    volumes:
      - ./spark/conf:/opt/bitnami/spark/conf:rw
    networks:
      - prod-network
    restart: unless-stopped
    deploy:
      mode: replicated
      replicas: 2

  trino-coordinator:
    image: trinodb/trino:440
    ports:
      - "8088:8080"
      - "8443:8443"
    volumes:
      - ./trino/etc:/etc/trino
      - ./trino/data:/var/trino/data
      - ./trino/certs/keystore.jks:/etc/trino/keystore.jks
    networks:
      - prod-network
    environment:
      - TRINO_NODE_ID=coordinator
      - TRINO_COORDINATOR=true
      - TRINO_DISCOVERY_ENABLED=true
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-k", "https://localhost:8443/v1/info"]
      interval: 30s
      timeout: 10s
      retries: 3

  trino-worker:
    image: trinodb/trino:440
    volumes:
      - ./trino/etc-worker:/etc/trino
      - ./trino/certs/keystore.jks:/etc/trino/keystore.jks
    networks:
      - prod-network
    environment:
      - TRINO_NODE_ID=worker
      - TRINO_COORDINATOR=false
      - TRINO_DISCOVERY_ENABLED=true
    depends_on:
      trino-coordinator:
        condition: service_healthy
    restart: unless-stopped
    deploy:
      mode: replicated
      replicas: 3  # Increased for production

volumes:
  spark-warehouse:

networks:
  prod-network:
    driver: bridge
EOF
```

### Step 4: System Optimization

#### 4.1 Update System Limits
```bash
# Add to /etc/security/limits.conf
echo "* soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 65536" | sudo tee -a /etc/security/limits.conf

# Update Docker daemon
sudo tee /etc/docker/daemon.json << EOF
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
EOF

sudo systemctl restart docker
```

#### 4.2 Configure Production Memory Settings
```bash
# Update Trino coordinator memory
sed -i 's/query.max-memory=1GB/query.max-memory=8GB/g' trino/etc/config.properties
sed -i 's/query.max-memory-per-node=512MB/query.max-memory-per-node=2GB/g' trino/etc/config.properties

# Update Trino worker memory
sed -i 's/query.max-memory=1GB/query.max-memory=8GB/g' trino/etc-worker/config.properties
sed -i 's/query.max-memory-per-node=512MB/query.max-memory-per-node=2GB/g' trino/etc-worker/config.properties

# Update JVM settings
echo "-Xmx4G" >> trino/etc/jvm.config
echo "-Xmx4G" >> trino/etc-worker/jvm.config
```

### Step 5: Deployment

#### 5.1 Build and Start Services
```bash
# Build all images
docker compose -f docker-compose.prod.yml build

# Start all services
docker compose -f docker-compose.prod.yml up -d

# Verify all services are running
docker compose -f docker-compose.prod.yml ps
```

#### 5.2 Initialize Data
```bash
# Wait for services to be healthy
sleep 30

# Initialize Hive tables
docker compose -f docker-compose.prod.yml exec spark-master \
    spark-submit /opt/bitnami/spark/jobs/init_hive_tables.py

# Setup Infinity tables (if applicable)
docker compose -f docker-compose.prod.yml exec spark-master \
    spark-submit /opt/bitnami/spark/jobs/setup_infinity_tables.py
```

### Step 6: Load Balancer Setup (Optional)

#### 6.1 Nginx Configuration
```nginx
upstream trino_backend {
    server localhost:8088;
}

server {
    listen 80;
    server_name trino.yourdomain.com;
    return 301 https://\$server_name\$request_uri;
}

server {
    listen 443 ssl http2;
    server_name trino.yourdomain.com;

    ssl_certificate /path/to/your/cert.pem;
    ssl_certificate_key /path/to/your/key.pem;

    location / {
        proxy_pass https://trino_backend;
        proxy_ssl_verify off;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header Authorization \$http_authorization;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

### Step 7: Monitoring and Logging

#### 7.1 Setup Log Rotation
```bash
# Create logrotate configuration
sudo tee /etc/logrotate.d/trino-cluster << EOF
/var/lib/docker/containers/*/*.log {
    daily
    rotate 7
    compress
    missingok
    notifempty
    create 644 root root
}
EOF
```

#### 7.2 Health Check Script
```bash
# Create health check script
cat > health_check.sh << 'EOF'
#!/bin/bash

echo "=== Trino Cluster Health Check ==="
echo "Date: $(date)"
echo

# Check service status
echo "Docker Compose Services:"
docker compose -f docker-compose.prod.yml ps

echo -e "\nTrino Coordinator Health:"
curl -k -s https://localhost:8443/v1/info | jq '.'

echo -e "\nTrino Cluster Nodes:"
curl -k -s https://localhost:8443/v1/node | jq '.'

echo -e "\nSpark Master Status:"
curl -s http://localhost:8082/json/ | jq '.status'

echo "=== End Health Check ==="
EOF

chmod +x health_check.sh

# Setup cron job for regular health checks
(crontab -l 2>/dev/null; echo "*/5 * * * * /path/to/health_check.sh >> /var/log/trino-health.log 2>&1") | crontab -
```

### Step 8: Backup and Recovery

#### 8.1 Backup Script
```bash
cat > backup.sh << 'EOF'
#!/bin/bash

BACKUP_DIR="/backup/trino-cluster/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

# Backup configurations
cp -r trino/ "$BACKUP_DIR/"
cp -r spark/ "$BACKUP_DIR/"
cp docker-compose.prod.yml "$BACKUP_DIR/"
cp .env "$BACKUP_DIR/"

# Backup Hive Metastore (if using local PostgreSQL)
# pg_dump -h localhost -U hive_prod metastore > "$BACKUP_DIR/metastore_backup.sql"

echo "Backup completed: $BACKUP_DIR"
EOF

chmod +x backup.sh
```

### Step 9: Security Hardening

#### 9.1 Firewall Configuration
```bash
# Allow only necessary ports
sudo ufw allow 22/tcp      # SSH
sudo ufw allow 80/tcp      # HTTP (redirect)
sudo ufw allow 443/tcp     # HTTPS
sudo ufw allow 9083/tcp    # Hive Metastore (if needed externally)
sudo ufw --force enable
```

#### 9.2 Update Passwords
```bash
# Generate secure passwords
openssl rand -base64 32  # For shared secrets
openssl rand -base64 16  # For keystore passwords

# Update in configuration files and .env
```

### Step 10: Post-Deployment Verification

#### 10.1 Functional Tests
```bash
# Test Trino connectivity
curl -k -u p2p:password https://localhost:8443/v1/info

# Test query execution
curl -k -u p2p:password -X POST https://localhost:8443/v1/statement \
    -H "Content-Type: application/json" \
    -d '{"query": "SHOW CATALOGS"}'

# Test Spark connectivity
curl http://localhost:8082/json/

# Verify Hive tables
docker compose -f docker-compose.prod.yml exec spark-master \
    spark-sql -e "SHOW DATABASES;"
```

#### 10.2 Performance Baseline
```bash
# Run performance tests
docker compose -f docker-compose.prod.yml exec trino-coordinator \
    trino --server https://localhost:8443 --user p2p --password \
    --execute "SELECT count(*) FROM hive.infinity.your_large_table"
```

This production deployment guide ensures a robust, secure, and scalable Trino cluster deployment suitable for production workloads.

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

### Trino Cluster Issues

#### Authentication Problems:
If you see "Password not allowed" in the Trino Web UI:
1. **Check coordinator configuration**: Ensure `http-server.authentication.type=PASSWORD` is set
2. **Verify password file**: Check `/trino/etc/password.db` contains user entries
3. **Test API access**: Use `curl -k -u p2p:password https://localhost:8443/v1/info`
4. **Use HTTPS**: Password authentication may require HTTPS endpoint

```bash
# Reset password for user p2p
docker run --rm trinodb/trino:440 htpasswd -B -C 10 -n p2p > temp_pass
# Copy the hash to trino/etc/password.db
```

#### Workers not starting:
1. **Check configuration conflicts**: Ensure workers don't have `discovery-server.enabled` property
2. **Verify data directory permissions**: Workers use `/tmp/trino/data` to avoid conflicts
3. **Check coordinator connectivity**: Workers must reach `trino-coordinator:8080`

```bash
# Debug worker startup issues
docker compose logs trino-worker --tail=50

# Check if coordinator is accessible from worker
docker compose exec trino-worker curl -k http://trino-coordinator:8080/v1/info
```

#### Permission denied errors:
If Trino workers show "Permission denied: '/var/trino'" errors:
- Workers are configured to use `/tmp/trino/data` instead of shared volumes
- Ensure the worker configuration has correct `node.data-dir` setting

#### Query performance issues:
- Monitor worker utilization in Trino Web UI
- Consider increasing worker replicas: `docker compose up -d --scale trino-worker=4`
- Adjust memory settings in worker `config.properties`

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
