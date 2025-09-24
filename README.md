# buildernet-orderflow-proxy-v2

## Clickhouse

Install the Clickhouse client:
```bash
curl https://clickhouse.com/ | sh
```

Copy the example environment variables and fill in the values:
```bash
cp .env.example .env
```

Connect to Clickhouse:
```bash
source .env
./clickhouse client --host $CLICKHOUSE_HOST --user $CLICKHOUSE_USER --secure --password $CLICKHOUSE_PASSWORD
```

Create the database:
```sql
CREATE DATABASE IF NOT EXISTS buildernet_orderflow_proxy COMMENT 'Buildernet orderflow proxy database';
```
Or
```bash
source .env
./clickhouse client --host $CLICKHOUSE_HOST --user $CLICKHOUSE_USER --secure --password $CLICKHOUSE_PASSWORD --query "CREATE DATABASE IF NOT EXISTS buildernet_orderflow_proxy COMMENT 'Buildernet orderflow proxy database';"
```

Create the bundles table:
```bash
./clickhouse client --host $CLICKHOUSE_HOST --user $CLICKHOUSE_USER --secure --password $CLICKHOUSE_PASSWORD -d buildernet_orderflow_proxy --queries-file ./fixtures/create_bundles_table.sql 
```
