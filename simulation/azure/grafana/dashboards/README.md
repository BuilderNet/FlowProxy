# Grafana Dashboards

Place your Grafana dashboard JSON files in this directory.

## Usage

1. Export a dashboard from Grafana as JSON
2. Place the `.json` file in this directory
3. Run `pulumi up` to deploy the dashboard to the builderhub VM
4. The dashboard will be automatically loaded by Grafana

## Example

```bash
# Add a dashboard
cp my-dashboard.json grafana/dashboards/

# Deploy
pulumi up
```

Dashboards will be available at: `http://builderhub.flowproxy.internal/dashboards`
