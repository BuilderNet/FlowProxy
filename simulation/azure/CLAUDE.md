# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a Pulumi-based Infrastructure as Code (IaC) project that provisions Azure staging infrastructure for FlowProxy, an orderflow proxy service. It deploys a multi-region setup with HAProxy load balancers and FlowProxy instances in East US and West Europe.

## Essential Commands

### Pulumi Operations
```bash
# Preview infrastructure changes
pulumi preview

# Deploy infrastructure
pulumi up

# Destroy all resources
pulumi destroy

# View current stack outputs
pulumi stack output

# Set configuration values
pulumi config set flowproxy-staging:adminUsername "<username>"
pulumi config set flowproxy-staging:haProxyVersion "3.0.6@sha256:..."
pulumi config set --secret flowproxy-staging:sshPublicKey "ssh-ed25519 ..."
pulumi config set --secret flowproxy-staging:githubToken "<token>"
pulumi config set flowproxy-staging:flowProxyArtifact "<github-artifact-url>"
pulumi config set --secret flowproxy-staging:tailscaleAuthKey "<key>"  # optional
```

### Azure CLI
```bash
# Login to Azure
az login

# View current subscription
az account show
```

### Package Management
```bash
# Install dependencies
pnpm install

# TypeScript compilation
npx tsc
```

## Architecture

### Multi-Region Deployment
The infrastructure creates a complete staging environment across two Azure regions:
- **East US** (`eastus`): Primary region with BuilderHub VM and FlowProxy instance
- **West Europe** (`westeurope`): Secondary region with FlowProxy instance

#### East US Resources:
1. **vm-builderhub**: Internal-only VM (no public IP) running BuilderHub mock service
   - Accessible via Tailscale VPN or SSH jump host through vm-eastus
   - DNS: `builderhub.flowproxy.internal`
   - Purpose: Credential registration endpoint for FlowProxy instances
2. **vm-eastus**: FlowProxy instance with HAProxy frontend (has public IP)

#### West Europe Resources:
1. **vm-westeurope**: FlowProxy instance with HAProxy frontend (has public IP)

Each FlowProxy region consists of:
1. **Resource Group**: Contains all regional resources
2. **Virtual Network (VNet)**: Isolated network (10.10.0.0/16 for East, 10.20.0.0/16 for West)
3. **Subnet**: Dedicated subnet for VMs (10.10.1.0/24 for East, 10.20.1.0/24 for West)
4. **Network Security Group (NSG)**: Firewall rules allowing SSH (22), HTTP (80), HTTPS (443), HAProxy (5544), and ICMP from peer VNet
5. **Public IP**: Static IP for external access
6. **Network Interface (NIC)**: Connected to subnet with public IP
7. **Virtual Machine**: Ubuntu 22.04 LTS (Standard_B2s: 2 vCPU, 4GB RAM)
8. **VM Extension**: CustomScript extension for automated provisioning

### Cross-Region Connectivity
- **VNet Peering**: Bidirectional peering between East and West VNets enables private network communication
- **Private DNS Zone**: `flowproxy.internal` zone linked to both VNets provides cross-region name resolution
  - `builderhub.flowproxy.internal` → BuilderHub VM private IP (internal-only)
  - `vm-eastus.flowproxy.internal` → East US FlowProxy VM private IP
  - `vm-westeurope.flowproxy.internal` → West Europe FlowProxy VM private IP

### BuilderHub VM Provisioning
The BuilderHub VM (vm-builderhub) is provisioned first since FlowProxy instances depend on it for credential registration. Provisioning steps (index.ts:385-414):

1. **Base System Setup**:
   - Install Docker, curl, ca-certificates
   - Enable and start Docker service

2. **Optional Tailscale Setup**:
   - Install and configure Tailscale if `tailscaleAuthKey` is provided
   - Registers with hostname `vm-builderhub` and enables SSH over Tailscale

3. **MockHub Service Deployment**:
   - Run mockhub Docker container from `mempirate/mockhub:latest`
   - Exposed on port 3000
   - TLS enabled via `ENABLE_TLS=true` environment variable
   - Accessible at `http://builderhub.flowproxy.internal` from other VMs

### FlowProxy VM Provisioning Flow
Each FlowProxy VM (vm-eastus, vm-westeurope) is provisioned via the CustomScript extension with the following automated steps:

1. **Base System Setup**:
   - Install Docker, curl, ca-certificates, openssl, unzip, jq
   - Create HAProxy directories: `/usr/local/etc/haproxy/{certs,static}`

2. **HAProxy Configuration**:
   - Deploy `haproxy.cfg` (base64-encoded from local file)
   - Generate self-signed TLS certificate if not present
   - Pull and run HAProxy container on ports 80, 443, 5544, 8405

3. **Optional Tailscale Setup**:
   - Install and configure Tailscale if `tailscaleAuthKey` is provided
   - Registers VM with hostname (e.g., `vm-eastus`, `vm-westeurope`)

4. **FlowProxy Installation**:
   - Download FlowProxy binary from GitHub Actions artifact using GitHub API
   - Extract to `/usr/local/bin/flowproxy`
   - Deploy systemd service template `flowproxy@.service`
   - Deploy environment file `flowproxy.env` to `/etc/flowproxy/`

5. **ECDSA Key Generation**:
   - Install Foundry toolchain (for `cast` command)
   - Generate new ECDSA private key using `cast wallet new`
   - Append private key to `/etc/flowproxy/flowproxy.env` as `FLASHBOTS_ORDERFLOW_SIGNER`

6. **BuilderHub Registration**:
   - Extract TLS certificate from HAProxy
   - POST credentials (TLS cert + ECDSA public key address) to BuilderHub at `http://builderhub.flowproxy.internal/api/l1-builder/v1/register_credentials/orderflow_proxy`
   - Retry up to 5 times with 10s delays
   - Registration is required before FlowProxy can start serving traffic

7. **Service Activation**:
   - Start FlowProxy as systemd service: `flowproxy@<hostname>.service`
   - Service reads `/etc/flowproxy/flowproxy.env` for configuration

### Extension Update Mechanism
The VM extension uses a `forceUpdateTag` (index.ts:24-28) computed from the SHA256 hash of:
- `haproxy.cfg` content
- `haProxyVersion` value
- `flowproxy.env` content
- `flowproxy@.service` template
- `flowProxyArtifact` URL

When any of these inputs change, the hash changes, triggering a re-run of the CustomScript extension without VM recreation. This enables zero-downtime configuration updates.

### HAProxy Configuration
HAProxy (haproxy.cfg) acts as a reverse proxy with:
- **Frontend `default`** (ports 80, 443): Routes to `user_of` backend (FlowProxy user endpoint at 127.0.0.1:5543)
- **Frontend `system`** (port 5544): Routes to `system_of` backend (FlowProxy system endpoint at 127.0.0.1:5542)
- **Frontend `prometheus`** (port 8405, localhost only): Exposes HAProxy metrics at `/metrics`
- **Frontend `public_cert`** (port 14727, localhost only): Serves certificate from `/usr/local/etc/haproxy/static/le.cer`
- Health checks on `/livez` endpoint for both backends
- Stick tables for connection tracking and rate limiting
- TLS termination with certificates from `/usr/local/etc/haproxy/certs/`

### FlowProxy Service
FlowProxy runs as a templated systemd service `flowproxy@.service`:
- Instance name (e.g., `vm-eastus`) is passed via systemd template variable `%i` and set as `BUILDER_NAME` environment variable
- Reads configuration from `/etc/flowproxy/flowproxy.env`
- Connects to BuilderHub at `http://builderhub.flowproxy.internal`
- Sends metrics to ClickHouse (configured via environment variables)
- Listens on:
  - 127.0.0.1:5543 (user orderflow endpoint)
  - 127.0.0.1:5542 (system orderflow endpoint)
  - 127.0.0.1:8090 (metrics endpoint)

## Required Configuration

Before running `pulumi up`, you must:

1. **Copy example config**: `cp Pulumi.dev.example.yaml Pulumi.dev.yaml`

2. **Set required Pulumi config values**:
   - `adminUsername`: SSH username for VMs
   - `sshPublicKey`: SSH public key for authentication (must be secret)
   - `flowProxyArtifact`: GitHub Actions artifact URL (e.g., `https://github.com/ORG/REPO/actions/runs/RUN_ID/artifacts/ARTIFACT_ID`)
   - `githubToken`: GitHub personal access token with `actions:read` scope (must be secret)
   - `haProxyVersion`: Docker image tag for HAProxy (can include SHA256 digest)

3. **Optional config values**:
   - `tailscaleAuthKey`: Tailscale auth key for VPN access (secret)
   - `allowedSshCidr`: CIDR range for SSH access (defaults to `0.0.0.0/0`)

## Local Files Required

The following files must exist in this directory before deployment:
- `haproxy.cfg`: HAProxy configuration file
- `flowproxy.env`: FlowProxy environment variables (contains ClickHouse credentials)
- `flowproxy@.service`: systemd service unit template

**Note**: The mockhub service is deployed via Docker Hub (`mempirate/mockhub:latest`), so no local binary is required.

## Key Implementation Details

### GitHub Artifact URL Conversion
The `toApiZip` function (index.ts:392-403) converts GitHub UI artifact URLs to API endpoints:
- Input: `https://github.com/ORG/REPO/actions/runs/RUN_ID/artifacts/ARTIFACT_ID`
- Output: `https://api.github.com/repos/ORG/REPO/actions/artifacts/ARTIFACT_ID/zip`

This enables automated download of FlowProxy binaries from GitHub Actions.

### VM Recreation Prevention
VMs are created with `ignoreChanges: ["osProfile"]` (index.ts:227-228) to prevent recreation when SSH keys or admin usernames change in Pulumi config. This preserves VM state across deployments.

### Pulumi Outputs
The stack exports the following outputs for operational use:
- `eastPublicIp`, `westPublicIp`: Public IPs for external access to FlowProxy VMs
- `eastPrivateIp`, `westPrivateIp`, `builderhubPrivateIp`: Private IPs within VNets
- `sshEast`, `sshWest`: Ready-to-use SSH commands (e.g., `ssh azureuser@1.2.3.4`)
- `builderhubSshNote`: Instructions for SSH access to builderhub (via jump host or Tailscale)
- `eastPrivateFqdn`, `westPrivateFqdn`, `builderhubPrivateFqdn`: Private DNS names (e.g., `builderhub.flowproxy.internal`)
- `eastNicPrivateFqdn`, `westNicPrivateFqdn`, `builderhubNicPrivateFqdn`: Azure-provided internal FQDNs

## Development Workflow

1. Make changes to infrastructure code in `index.ts`
2. Update configuration files (`haproxy.cfg`, `flowproxy.env`, `flowproxy@.service`) as needed
3. Run `pulumi preview` to see planned changes
4. Run `pulumi up` to apply changes
5. Monitor VM provisioning via Azure Portal or SSH to VMs
6. Check service status: `ssh <user>@<public-ip> "systemctl status flowproxy@<hostname>"`
7. View logs: `ssh <user>@<public-ip> "journalctl -u flowproxy@<hostname> -f"`

## Debugging

### VM Extension Logs
```bash
# SSH to VM
ssh <adminUsername>@<publicIp>

# Check extension execution logs
sudo cat /var/lib/waagent/custom-script/download/0/stderr
sudo cat /var/lib/waagent/custom-script/download/0/stdout

# Check provisioning script
cat /tmp/provision.sh
```

### Service Logs
```bash
# FlowProxy service status
systemctl status flowproxy@vm-eastus  # or vm-westeurope

# FlowProxy logs
journalctl -u flowproxy@vm-eastus -f --no-pager -n 100

# HAProxy logs
docker logs haproxy -f
```

### Network Connectivity
```bash
# Test cross-region DNS resolution
nslookup vm-westeurope.flowproxy.internal  # from East VM
nslookup vm-eastus.flowproxy.internal      # from West VM
nslookup builderhub.flowproxy.internal     # should resolve to East VM

# Test cross-region connectivity
ping vm-westeurope.flowproxy.internal      # from East VM
curl http://builderhub.flowproxy.internal  # from West VM
```

## Security Considerations

- SSH access is controlled via `allowedSshCidr` (default allows all IPs; restrict in production)
- Secrets (SSH keys, GitHub token, Tailscale key) must be set using `--secret` flag in Pulumi config
- VMs use SSH key authentication only (password auth disabled)
- TLS certificates are self-signed by default; production should use valid certificates
- Credentials are registered with BuilderHub before FlowProxy starts accepting traffic
