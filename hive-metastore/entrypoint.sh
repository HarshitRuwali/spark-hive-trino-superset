#!/bin/bash

# Substitute environment variables into the template
envsubst < /opt/hive/conf/core-site-template.xml > /opt/hive/conf/core-site.xml

# Then start the default Hive Metastore service
exec /opt/hive/bin/hive --service metastore

