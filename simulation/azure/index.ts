import * as pulumi from "@pulumi/pulumi";
import * as resources from "@pulumi/azure-native/resources";
import * as network from "@pulumi/azure-native/network";
import * as compute from "@pulumi/azure-native/compute";
import * as privatedns from "@pulumi/azure-native/privatedns";
import * as fs from "fs";
import * as crypto from "crypto";
import { execSync } from "child_process";

// Configuration
const config = new pulumi.Config();
const adminUsername = config.get("adminUsername") || "azureuser";
const sshPublicKey = config.require("sshPublicKey"); // e.g. contents of ~/.ssh/id_rsa.pub
const allowedSshCidr = config.get("allowedSshCidr") || "0.0.0.0/0"; // restrict in config for better security
const tailscaleAuthKey = config.getSecret("tailscaleAuthKey"); // optional: set as Pulumi secret
const haProxyVersion = config.get("haProxyVersion") || "3.0.6"; // can include @sha256:digest for Docker image
const flowProxyArtifact = config.require("flowProxyArtifact");
const githubToken = config.requireSecret("githubToken"); // required: GH token for artifact download
const haproxyCfg = fs.readFileSync("haproxy.cfg", "utf8");
const flowproxyEnv = fs.readFileSync("flowproxy.env", "utf8");
const flowproxyServiceTmpl = fs.readFileSync("flowproxy@.service", "utf8");
const flowproxyEnvB64 = Buffer.from(flowproxyEnv).toString("base64");
const flowproxySvcB64 = Buffer.from(flowproxyServiceTmpl).toString("base64");
// Monitoring configs
const prometheusConfig = fs.readFileSync("prometheus.yml", "utf8");
const grafanaDatasource = fs.readFileSync("grafana/provisioning/datasources/datasource.yml", "utf8");
const grafanaDashboardConfig = fs.readFileSync("grafana/provisioning/dashboards/dashboard.yml", "utf8");
const prometheusConfigB64 = Buffer.from(prometheusConfig).toString("base64");
const grafanaDatasourceB64 = Buffer.from(grafanaDatasource).toString("base64");
const grafanaDashboardConfigB64 = Buffer.from(grafanaDashboardConfig).toString("base64");
// Create a compressed tarball of dashboard files (much smaller than individual base64 encoding)
const dashboardFiles = fs.readdirSync("grafana/dashboards").filter(f => f.endsWith(".json"));
// Create tarball in memory
execSync("tar -czf /tmp/dashboards.tar.gz -C grafana/dashboards " + dashboardFiles.join(" "));
const dashboardsTarballB64 = fs.readFileSync("/tmp/dashboards.tar.gz").toString("base64");

const builderhubExtTag = crypto
    .createHash("sha256")
    .update(prometheusConfig + grafanaDatasource + grafanaDashboardConfig + dashboardsTarballB64)
    .digest("hex")
    .slice(0, 32);

// Make sure FlowProxy is updated when BuilderHub is updated, so we trigger a re-registration of the orderflow proxy credentials.
const haExtTag = crypto
    .createHash("sha256")
    .update(haproxyCfg + String(haProxyVersion) + flowproxyEnv + flowproxyServiceTmpl + String(flowProxyArtifact) + builderhubExtTag)
    .digest("hex")
    .slice(0, 32);

// Target regions (Azure names)
const eastRegion = "eastus";
const westEuropeRegion = "westeurope"; // user requested "europewest"; Azure region is "westeurope"

// Resource Groups per region
const rgEast = new resources.ResourceGroup("rg-east", { location: eastRegion });
const rgWest = new resources.ResourceGroup("rg-west", { location: westEuropeRegion });

// Virtual Networks and Subnets
const vnetEast = new network.VirtualNetwork("vnet-east", {
    resourceGroupName: rgEast.name,
    location: rgEast.location,
    addressSpace: { addressPrefixes: ["10.10.0.0/16"] },
});

const vnetWest = new network.VirtualNetwork("vnet-west", {
    resourceGroupName: rgWest.name,
    location: rgWest.location,
    addressSpace: { addressPrefixes: ["10.20.0.0/16"] },
});

// Create explicit Subnets so IDs are definite
const subnetEast = new network.Subnet("subnet-east", {
    resourceGroupName: rgEast.name,
    virtualNetworkName: vnetEast.name,
    addressPrefix: "10.10.1.0/24",
});

const subnetWest = new network.Subnet("subnet-west", {
    resourceGroupName: rgWest.name,
    virtualNetworkName: vnetWest.name,
    addressPrefix: "10.20.1.0/24",
});

// NSGs allowing SSH and intra-VNet traffic (defaults include AllowVnetInBound)
function createNsg(name: string, rg: resources.ResourceGroup, location: pulumi.Input<string>, allowFromPeerCidr: string) {
    return new network.NetworkSecurityGroup(name, {
        resourceGroupName: rg.name,
        location,
        securityRules: [
            {
                name: "Allow-SSH",
                priority: 1000,
                direction: "Inbound",
                access: "Allow",
                protocol: "Tcp",
                sourcePortRange: "*",
                destinationPortRange: "22",
                sourceAddressPrefix: allowedSshCidr,
                destinationAddressPrefix: "*",
            },
            {
                name: "Allow-HTTP",
                priority: 1010,
                direction: "Inbound",
                access: "Allow",
                protocol: "Tcp",
                sourcePortRange: "*",
                destinationPortRange: "80",
                sourceAddressPrefix: "*",
                destinationAddressPrefix: "*",
            },
            {
                name: "Allow-HTTPS",
                priority: 1020,
                direction: "Inbound",
                access: "Allow",
                protocol: "Tcp",
                sourcePortRange: "*",
                destinationPortRange: "443",
                sourceAddressPrefix: "*",
                destinationAddressPrefix: "*",
            },
            {
                name: "Allow-HAProxy-5544",
                priority: 1030,
                direction: "Inbound",
                access: "Allow",
                protocol: "Tcp",
                sourcePortRange: "*",
                destinationPortRange: "5544",
                sourceAddressPrefix: "*",
                destinationAddressPrefix: "*",
            },
            {
                name: "Allow-HAProxy-5554",
                priority: 1040,
                direction: "Inbound",
                access: "Allow",
                protocol: "Tcp",
                sourcePortRange: "*",
                destinationPortRange: "5554",
                sourceAddressPrefix: "*",
                destinationAddressPrefix: "*",
            },
            {
                name: "Allow-ICMP-From-Peer",
                priority: 1100,
                direction: "Inbound",
                access: "Allow",
                protocol: "Icmp",
                sourcePortRange: "*",
                destinationPortRange: "*",
                sourceAddressPrefix: allowFromPeerCidr,
                destinationAddressPrefix: "*",
            },
        ],
    });
}

const nsgEast = createNsg("nsg-east", rgEast, rgEast.location, "10.20.0.0/16");
const nsgWest = createNsg("nsg-west", rgWest, rgWest.location, "10.10.0.0/16");

// Public IPs
const pipEast = new network.PublicIPAddress("pip-east", {
    resourceGroupName: rgEast.name,
    location: rgEast.location,
    publicIPAllocationMethod: "Static",
    sku: { name: "Standard" },
});

const pipWest = new network.PublicIPAddress("pip-west", {
    resourceGroupName: rgWest.name,
    location: rgWest.location,
    publicIPAllocationMethod: "Static",
    sku: { name: "Standard" },
});

// NICs
const nicEast = new network.NetworkInterface("nic-east", {
    resourceGroupName: rgEast.name,
    location: rgEast.location,
    networkSecurityGroup: { id: nsgEast.id },
    dnsSettings: {
        internalDnsNameLabel: "vm-eastus",
    },
    ipConfigurations: [
        {
            name: "ipconfig1",
            privateIPAllocationMethod: "Dynamic",
            subnet: { id: subnetEast.id },
            publicIPAddress: { id: pipEast.id },
        },
    ],
});

const nicWest = new network.NetworkInterface("nic-west", {
    resourceGroupName: rgWest.name,
    location: rgWest.location,
    networkSecurityGroup: { id: nsgWest.id },
    dnsSettings: {
        internalDnsNameLabel: "vm-westeurope",
    },
    ipConfigurations: [
        {
            name: "ipconfig1",
            privateIPAllocationMethod: "Dynamic",
            subnet: { id: subnetWest.id },
            publicIPAddress: { id: pipWest.id },
        },
    ],
});

// BuilderHub NIC (internal-only, no public IP)
const nicBuilderhub = new network.NetworkInterface("nic-builderhub", {
    resourceGroupName: rgEast.name,
    location: rgEast.location,
    networkSecurityGroup: { id: nsgEast.id },
    dnsSettings: {
        internalDnsNameLabel: "vm-builderhub",
    },
    ipConfigurations: [
        {
            name: "ipconfig1",
            privateIPAllocationMethod: "Dynamic",
            subnet: { id: subnetEast.id },
        },
    ],
});

// Additional FlowProxy NICs (internal-only, no public IPs)
const nicEast2 = new network.NetworkInterface("nic-east-2", {
    resourceGroupName: rgEast.name,
    location: rgEast.location,
    networkSecurityGroup: { id: nsgEast.id },
    dnsSettings: {
        internalDnsNameLabel: "vm-eastus-2",
    },
    ipConfigurations: [
        {
            name: "ipconfig1",
            privateIPAllocationMethod: "Dynamic",
            subnet: { id: subnetEast.id },
        },
    ],
});

const nicWest2 = new network.NetworkInterface("nic-west-2", {
    resourceGroupName: rgWest.name,
    location: rgWest.location,
    networkSecurityGroup: { id: nsgWest.id },
    dnsSettings: {
        internalDnsNameLabel: "vm-westeurope-2",
    },
    ipConfigurations: [
        {
            name: "ipconfig1",
            privateIPAllocationMethod: "Dynamic",
            subnet: { id: subnetWest.id },
        },
    ],
});

// VM size: 2 vCPU, 4GB RAM
const smallVm = "Standard_B2s";

const bigVm = "Standard_B4ls_v2"

// Ubuntu image reference (22.04 LTS)
const ubuntuImageRef = {
    publisher: "Canonical",
    offer: "0001-com-ubuntu-server-jammy",
    sku: "22_04-lts",
    version: "latest",
};

function createVm(name: string, rg: resources.ResourceGroup, location: pulumi.Input<string>, nicId: pulumi.Input<string>, vmSize: string, opts?: pulumi.CustomResourceOptions) {
    return new compute.VirtualMachine(name, {
        resourceGroupName: rg.name,
        location,
        networkProfile: {
            networkInterfaces: [{ id: nicId, primary: true }],
        },
        hardwareProfile: { vmSize },
        osProfile: {
            adminUsername,
            computerName: name,
            linuxConfiguration: {
                disablePasswordAuthentication: true,
                ssh: {
                    publicKeys: [
                        {
                            path: pulumi.interpolate`/home/${adminUsername}/.ssh/authorized_keys`,
                            keyData: sshPublicKey,
                        },
                    ],
                },
            },
        },
        storageProfile: {
            imageReference: ubuntuImageRef,
            osDisk: {
                name: pulumi.interpolate`${name}-osdisk`,
                createOption: "FromImage",
                managedDisk: { storageAccountType: "StandardSSD_LRS" },
            },
        },
    }, opts);
}

// Create BuilderHub VM first (other VMs depend on it for credential registration)
const vmBuilderhub = createVm("vm-builderhub", rgEast, rgEast.location, nicBuilderhub.id, smallVm, { ignoreChanges: ["osProfile", "storageProfile"] });

const vmEast = createVm("vm-eastus", rgEast, rgEast.location, nicEast.id, smallVm, { ignoreChanges: ["osProfile", "storageProfile"] });
const vmWest = createVm("vm-westeurope", rgWest, rgWest.location, nicWest.id, bigVm, { ignoreChanges: ["osProfile", "storageProfile"] });
const vmEast2 = createVm("vm-eastus-2", rgEast, rgEast.location, nicEast2.id, smallVm, { ignoreChanges: ["osProfile", "storageProfile"] });
const vmWest2 = createVm("vm-westeurope-2", rgWest, rgWest.location, nicWest2.id, smallVm, { ignoreChanges: ["osProfile", "storageProfile"] });

// VNet Peering (bi-directional)
const eastToWest = new network.VirtualNetworkPeering("east-to-west", {
    resourceGroupName: rgEast.name,
    virtualNetworkName: vnetEast.name,
    remoteVirtualNetwork: { id: vnetWest.id },
    allowVirtualNetworkAccess: true,
    allowForwardedTraffic: true,
});

const westToEast = new network.VirtualNetworkPeering("west-to-east", {
    resourceGroupName: rgWest.name,
    virtualNetworkName: vnetWest.name,
    remoteVirtualNetwork: { id: vnetEast.id },
    allowVirtualNetworkAccess: true,
    allowForwardedTraffic: true,
});

// VM Extensions: Provision HAProxy via Docker (no VM recreate)
const haproxyCfgB64 = Buffer.from(haproxyCfg).toString("base64");
function buildInstanceCommand(name: string): pulumi.Input<string> {
    const artifactApiUrl = toApiZip(flowProxyArtifact);
    const script = (key?: string, token?: string) => `#!/usr/bin/env bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

# Install essential packages first (with retry logic for transient apt issues)
rm -rf /var/lib/apt/lists/*
for i in 1 2 3; do apt-get update && break || sleep 5; done
for i in 1 2 3; do apt-get install -y curl ca-certificates openssl unzip jq && break || sleep 5; done

# Install Docker using official script
curl -fsSL https://get.docker.com | sh

mkdir -p /usr/local/etc/haproxy/certs /usr/local/etc/haproxy/static
echo "${haproxyCfgB64}" | base64 -d > /usr/local/etc/haproxy/haproxy.cfg
printf "\n" >> /usr/local/etc/haproxy/haproxy.cfg || true
if [ ! -f /usr/local/etc/haproxy/static/le.cer ]; then echo "placeholder-certificate-content" > /usr/local/etc/haproxy/static/le.cer; fi
if [ ! -f /usr/local/etc/haproxy/certs/default.pem ]; then
  # Get the private IP address
  PRIVATE_IP=$(hostname -I | awk '{print $1}')

  # Create OpenSSL config with SANs
  cat > /tmp/openssl.cnf <<EOF
[req]
default_bits = 2048
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = localhost

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1 = \${PRIVATE_IP}
IP.2 = 127.0.0.1
EOF

  openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout /usr/local/etc/haproxy/certs/default.key \
    -out /usr/local/etc/haproxy/certs/default.crt \
    -config /tmp/openssl.cnf \
    -extensions v3_req

  cat /usr/local/etc/haproxy/certs/default.key /usr/local/etc/haproxy/certs/default.crt > /usr/local/etc/haproxy/certs/default.pem
  rm /tmp/openssl.cnf
fi

CLIENT_CERT_DIR=/usr/local/etc/flowproxy
CLIENT_KEY="\${CLIENT_CERT_DIR}/client.key"
CLIENT_CERT="\${CLIENT_CERT_DIR}/client.crt"
mkdir -p "\${CLIENT_CERT_DIR}"
if [ ! -f "\${CLIENT_KEY}" ] || [ ! -f "\${CLIENT_CERT}" ]; then
  openssl req -newkey rsa:2048 -nodes \
    -keyout "\${CLIENT_KEY}" \
    -out /tmp/client.csr \
    -subj "/CN=client"
  openssl x509 -req -days 365 -sha256 \
    -in /tmp/client.csr \
    -signkey "\${CLIENT_KEY}" \
    -out "\${CLIENT_CERT}"
  rm /tmp/client.csr
fi
chmod 600 "\${CLIENT_KEY}"
chmod 644 "\${CLIENT_CERT}"

rm -f /usr/local/etc/haproxy/certs/default.crt /usr/local/etc/haproxy/certs/default.key || true
systemctl enable --now docker
docker rm -f haproxy || true
docker pull haproxy:${haProxyVersion}
docker run -d --name haproxy --restart unless-stopped \
    --network host \
    --cap-add=NET_BIND_SERVICE \
    --user root \
    -v /usr/local/etc/haproxy:/usr/local/etc/haproxy:ro \
    haproxy:${haProxyVersion}

# Optionally install Tailscale
${key ? `
rm -rf /var/lib/apt/lists/*
for i in 1 2 3; do apt-get update && break || sleep 5; done
apt-get install -y curl
curl -fsSL https://tailscale.com/install.sh | sh
systemctl enable --now tailscaled
tailscale up --authkey=${key} --hostname=${name} --ssh
` : ``}

# --- FlowProxy install ---
tmpzip=$(mktemp /tmp/flowproxy.XXXXXX.zip)
curl -fL -o "$tmpzip" \
    -H "Authorization: Bearer ${token}" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "${artifactApiUrl}" 
mkdir -p /usr/local/bin
unzip -o "$tmpzip" -d /usr/local/bin || true
chmod +x /usr/local/bin/flowproxy
mkdir -p /etc/flowproxy
echo "${flowproxySvcB64}" | base64 -d > /etc/systemd/system/flowproxy@.service
echo "${flowproxyEnvB64}" | base64 -d > /etc/flowproxy/flowproxy.env
systemctl daemon-reload

rm /.bashrc || true
export HOME=/root
# This bashrc contains the foundry bin path
# Install Foundry (for cast) to manage ECDSA keys — best effort to avoid failing the extension
if ! command -v cast &> /dev/null; then
    (
      set +e
      success=0
      for i in 1 2 3; do
        if curl -L https://foundry.paradigm.xyz | bash; then
          success=1
          break
        fi
        echo "foundry install attempt $i failed; retrying in 5s..." >&2
        sleep 5
      done
      # foundryup installs binaries under either ~/.foundry/bin or ~/.cargo/bin
      export PATH="$HOME/.foundry/bin:$HOME/.cargo/bin:$PATH"
      if [ -x "$HOME/.foundry/bin/foundryup" ]; then
        "$HOME/.foundry/bin/foundryup" || true
      fi
      # Ensure cast is discoverable in PATH even if installation paths differ
      if [ -x "$HOME/.foundry/bin/cast" ] && [ ! -x "/usr/local/bin/cast" ]; then
        ln -sf "$HOME/.foundry/bin/cast" /usr/local/bin/cast || true
      fi
      if [ -x "$HOME/.cargo/bin/cast" ] && [ ! -x "/usr/local/bin/cast" ]; then
        ln -sf "$HOME/.cargo/bin/cast" /usr/local/bin/cast || true
      fi
      if ! command -v cast &> /dev/null; then
        echo "Warning: cast not installed; continuing without it" >&2
      fi
    )
fi

# Install Samply for profiling — best effort; do not fail provisioning if unavailable
if ! command -v samply &> /dev/null; then
    (
      set +e
      for i in 1 2; do
        curl --proto '=https' --tlsv1.2 -LsSf https://github.com/mstange/samply/releases/download/samply-v0.13.1/samply-installer.sh | sh && break
        echo "samply install attempt $i failed; retrying in 5s..." >&2
        sleep 5
      done
      if ! command -v samply &> /dev/null; then
        echo "Warning: samply not installed; continuing without it" >&2
      fi
    )
fi

# Persist ECDSA key across extension updates
KEY_FILE="/etc/flowproxy/ecdsa_private_key"

# Resolve cast binary (may live in ~/.foundry/bin or ~/.cargo/bin)
CAST_BIN="$(command -v cast || true)"
if [ -z "$CAST_BIN" ] && [ -x "$HOME/.foundry/bin/cast" ]; then
  CAST_BIN="$HOME/.foundry/bin/cast"
fi
if [ -z "$CAST_BIN" ] && [ -x "$HOME/.cargo/bin/cast" ]; then
  CAST_BIN="$HOME/.cargo/bin/cast"
fi

ADDRESS=""
if [ -z "$CAST_BIN" ]; then
  echo "Warning: cast not available; skipping address derivation" >&2
else
  if [ -f "$KEY_FILE" ]; then
    # Use existing key
    PRIVATE_KEY=$(cat "$KEY_FILE")
    echo "Using existing ECDSA key"
  else
    # Generate new key and save it
    set +e
    key_output=$("$CAST_BIN" wallet new --json 2>/dev/null)
    set -e
    if [ -z "$key_output" ]; then
      echo "Error: failed to generate ECDSA key with cast" >&2
      exit 1
    fi
    ADDRESS=$(echo "$key_output" | jq -r '.[0].address')
    PRIVATE_KEY=$(echo "$key_output" | jq -r '.[0].private_key')
    echo "$PRIVATE_KEY" > "$KEY_FILE"
    chmod 600 "$KEY_FILE"
    echo "Generated new ECDSA key with address: $ADDRESS"
  fi

  # Derive address from private key (in case we loaded an existing one)
  set +e
  if [ -z "$ADDRESS" ]; then
    ADDRESS=$("$CAST_BIN" wallet address "$PRIVATE_KEY" 2>/dev/null || true)
  fi
  set -e
  echo "Using ECDSA key with address: \${ADDRESS:-unknown}"
fi

# Update flowproxy.env with the ECDSA private key (idempotent)
sed -i '/^FLASHBOTS_ORDERFLOW_SIGNER=/d' /etc/flowproxy/flowproxy.env
echo "FLASHBOTS_ORDERFLOW_SIGNER=\${PRIVATE_KEY:-}" >> /etc/flowproxy/flowproxy.env

# Register credentials with BuilderHub before enabling the orderflow proxy
set +e
set +o pipefail
set -x
set -a
source /etc/flowproxy/flowproxy.env || true
set +a
CERT=$(openssl x509 -in /usr/local/etc/haproxy/certs/default.pem -outform PEM 2>/dev/null || true)
PAYLOAD=$(jq -n --arg cert "$CERT" --arg addr "$ADDRESS" '{tls_cert:$cert, ecdsa_pubkey_address:$addr}')
URL="http://builderhub.flowproxy.internal:3000/api/l1-builder/v1/register_credentials/orderflow_proxy"
echo "Registering orderflow proxy credentials to $URL (address=$ADDRESS)"
success=0
for i in 1 2 3 4 5; do
  code=$(curl -sS -o /tmp/register.out -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' \
    --data "$PAYLOAD" "$URL" || true)
  if [ "$code" = "200" ] || [ "$code" = "201" ]; then
    echo "Registration succeeded (HTTP $code)"
    success=1
    break
  fi
  echo "Registration attempt $i failed (HTTP $code). Retrying in 10s..."
  sleep 10
done
if [ "$success" != "1" ]; then
  echo "Warning: BuilderHub registration did not succeed after 5 attempts; continuing." >&2
fi
set -e
set -o pipefail

systemctl enable --now flowproxy@${name}
systemctl restart flowproxy@${name}
systemctl status flowproxy@${name} --no-pager || journalctl -u flowproxy@${name} --no-pager -n 100
`;

    const build = (k?: string, t?: string) => {
        const b64 = Buffer.from(script(k, t)).toString("base64");
        return `/bin/bash -lc 'echo ${b64} | base64 -d > /tmp/provision.sh && bash /tmp/provision.sh'`;
    };

    const ts = (tailscaleAuthKey as any)?.apply ? (tailscaleAuthKey as pulumi.Output<string>) : undefined;
    const gh = (githubToken as any)?.apply ? (githubToken as pulumi.Output<string>) : undefined;
    if (ts && gh) {
        return pulumi.all([ts, gh]).apply(([k, t]) => build(k, t));
    } else if (ts) {
        return ts.apply(k => build(k, undefined as any));
    } else if (gh) {
        return gh.apply(t => build(undefined as any, t));
    }
    return build(undefined, undefined);
}

// BuilderHub provisioning script - installs Docker only for now
function buildBuilderhubCommand(name: string): pulumi.Input<string> {
    const script = (key?: string) => `#!/usr/bin/env bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

# Install Docker using official script
curl -fsSL https://get.docker.com | sh
systemctl enable --now docker

# Configure journald to limit log storage to 12GB
sed -i 's/#SystemMaxUse=/SystemMaxUse=12G/' /etc/systemd/journald.conf
systemctl restart systemd-journald

# Optionally install Tailscale
${key ? `
rm -rf /var/lib/apt/lists/*
for i in 1 2 3; do apt-get update && break || sleep 5; done
apt-get install -y curl
curl -fsSL https://tailscale.com/install.sh | sh
systemctl enable --now tailscaled
tailscale up --authkey=${key} --hostname=${name} --ssh
` : ``}

# Make sure we delete the old builderhub container
docker rm -f builderhub || true
docker run -d --name builderhub --restart unless-stopped -p 3000:3000 -e ENABLE_TLS=true mempirate/mockhub:v0.0.6

# === Prometheus Setup ===
mkdir -p /etc/prometheus
mkdir -p /var/lib/prometheus
chown -R 65534:65534 /var/lib/prometheus
echo "${prometheusConfigB64}" | base64 -d > /etc/prometheus/prometheus.yml

docker rm -f prometheus || true
docker run -d --name prometheus --restart unless-stopped \
  --network host \
  -v /etc/prometheus:/etc/prometheus \
  -v /var/lib/prometheus:/prometheus \
  prom/prometheus:latest \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus

# === Grafana Setup ===
mkdir -p /etc/grafana/provisioning/datasources
mkdir -p /etc/grafana/provisioning/dashboards
mkdir -p /etc/grafana/dashboards
mkdir -p /var/lib/grafana

echo "${grafanaDatasourceB64}" | base64 -d > /etc/grafana/provisioning/datasources/datasource.yml
echo "${grafanaDashboardConfigB64}" | base64 -d > /etc/grafana/provisioning/dashboards/dashboard.yml

# Extract dashboard files from compressed tarball
echo "${dashboardsTarballB64}" | base64 -d | tar -xzf - -C /etc/grafana/dashboards/

docker rm -f grafana || true
docker run -d --name grafana --restart unless-stopped \
  --network host \
  --user root \
  --cap-add=NET_BIND_SERVICE \
  -v /etc/grafana/provisioning:/etc/grafana/provisioning \
  -v /etc/grafana/dashboards:/etc/grafana/dashboards \
  -v /var/lib/grafana:/var/lib/grafana \
  -e GF_SERVER_HTTP_PORT=80 \
  -e GF_SECURITY_ADMIN_USER=admin \
  -e GF_SECURITY_ADMIN_PASSWORD=grafana \
  -e GF_AUTH_ANONYMOUS_ENABLED=true \
  -e GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer \
  grafana/grafana:latest
`;

    const build = (k?: string) => {
        const b64 = Buffer.from(script(k)).toString("base64");
        return `/bin/bash -lc 'echo ${b64} | base64 -d > /tmp/provision.sh && bash /tmp/provision.sh'`;
    };

    const ts = (tailscaleAuthKey as any)?.apply ? (tailscaleAuthKey as pulumi.Output<string>) : undefined;
    if (ts) {
        return ts.apply(k => build(k));
    }
    return build(undefined);
}


const builderhubExt = new compute.VirtualMachineExtension("builderhub-ext", {
    resourceGroupName: rgEast.name,
    vmName: vmBuilderhub.name,
    location: rgEast.location,
    publisher: "Microsoft.Azure.Extensions",
    type: "CustomScript",
    typeHandlerVersion: "2.1",
    forceUpdateTag: builderhubExtTag,
    autoUpgradeMinorVersion: true,
    protectedSettings: {
        commandToExecute: buildBuilderhubCommand("flowproxy-builderhub"),
    },
}, { dependsOn: [vmBuilderhub] });

const haExtEast = new compute.VirtualMachineExtension("haproxy-ext-east", {
    resourceGroupName: rgEast.name,
    vmName: vmEast.name,
    location: rgEast.location,
    publisher: "Microsoft.Azure.Extensions",
    type: "CustomScript",
    typeHandlerVersion: "2.1",
    forceUpdateTag: haExtTag,
    autoUpgradeMinorVersion: true,
    protectedSettings: {
        commandToExecute: buildInstanceCommand("flowproxy-eastus"),
    },
}, { dependsOn: [vmEast] });

const haExtWest = new compute.VirtualMachineExtension("haproxy-ext-west", {
    resourceGroupName: rgWest.name,
    vmName: vmWest.name,
    location: rgWest.location,
    publisher: "Microsoft.Azure.Extensions",
    type: "CustomScript",
    typeHandlerVersion: "2.1",
    forceUpdateTag: haExtTag,
    autoUpgradeMinorVersion: true,
    protectedSettings: {
        commandToExecute: buildInstanceCommand("flowproxy-westeurope"),
    },
}, { dependsOn: [vmWest] });

const haExtEast2 = new compute.VirtualMachineExtension("haproxy-ext-east-2", {
    resourceGroupName: rgEast.name,
    vmName: vmEast2.name,
    location: rgEast.location,
    publisher: "Microsoft.Azure.Extensions",
    type: "CustomScript",
    typeHandlerVersion: "2.1",
    forceUpdateTag: haExtTag,
    autoUpgradeMinorVersion: true,
    protectedSettings: {
        commandToExecute: buildInstanceCommand("flowproxy-eastus-2"),
    },
}, { dependsOn: [vmEast2] });

const haExtWest2 = new compute.VirtualMachineExtension("haproxy-ext-west-2", {
    resourceGroupName: rgWest.name,
    vmName: vmWest2.name,
    location: rgWest.location,
    publisher: "Microsoft.Azure.Extensions",
    type: "CustomScript",
    typeHandlerVersion: "2.1",
    forceUpdateTag: haExtTag,
    autoUpgradeMinorVersion: true,
    protectedSettings: {
        commandToExecute: buildInstanceCommand("flowproxy-westeurope-2"),
    },
}, { dependsOn: [vmWest2] });

function toApiZip(url: string): string {
    // Convert GitHub actions artifact page URL to API ZIP endpoint when possible
    // Example input: https://github.com/ORG/REPO/actions/runs/<run>/artifacts/<artifactId>
    // Output: https://api.github.com/repos/ORG/REPO/actions/artifacts/<artifactId>/zip
    const m = url.match(/^https:\/\/github\.com\/([^/]+)\/([^/]+)\/actions\/runs\/\d+\/artifacts\/(\d+)/);
    if (m) {
        const [_, org, repo, id] = m;
        return `https://api.github.com/repos/${org}/${repo}/actions/artifacts/${id}/zip`;
    }
    // If it's already an API URL or something else, return as-is
    return url;
}

// Private DNS zone for cross-VNet name resolution
const privateZoneName = "flowproxy.internal";
const dnsZone = new privatedns.PrivateZone("private-dns-zone", {
    resourceGroupName: rgEast.name,
    location: "global",
    privateZoneName,
});

// Link the zone to both VNets
const linkEast = new privatedns.VirtualNetworkLink("dns-link-east", {
    resourceGroupName: rgEast.name,
    location: "global",
    privateZoneName: dnsZone.name,
    virtualNetworkLinkName: "link-east",
    registrationEnabled: false,
    virtualNetwork: { id: vnetEast.id },
});

const linkWest = new privatedns.VirtualNetworkLink("dns-link-west", {
    resourceGroupName: rgEast.name,
    location: "global",
    privateZoneName: dnsZone.name,
    virtualNetworkLinkName: "link-west",
    registrationEnabled: false,
    virtualNetwork: { id: vnetWest.id },
});

// Reverse DNS zones for PTR records
const reverseDnsZoneEast = new privatedns.PrivateZone("reverse-dns-east", {
    resourceGroupName: rgEast.name,
    location: "global",
    privateZoneName: "1.10.10.in-addr.arpa",
});

const reverseDnsZoneWest = new privatedns.PrivateZone("reverse-dns-west", {
    resourceGroupName: rgEast.name,
    location: "global",
    privateZoneName: "1.20.10.in-addr.arpa",
});

// Link reverse DNS zones to both VNets
const reverseLinkEastToEast = new privatedns.VirtualNetworkLink("reverse-link-east-east", {
    resourceGroupName: rgEast.name,
    location: "global",
    privateZoneName: reverseDnsZoneEast.name,
    virtualNetworkLinkName: "link-east",
    registrationEnabled: false,
    virtualNetwork: { id: vnetEast.id },
});

const reverseLinkEastToWest = new privatedns.VirtualNetworkLink("reverse-link-east-west", {
    resourceGroupName: rgEast.name,
    location: "global",
    privateZoneName: reverseDnsZoneEast.name,
    virtualNetworkLinkName: "link-west",
    registrationEnabled: false,
    virtualNetwork: { id: vnetWest.id },
});

const reverseLinkWestToEast = new privatedns.VirtualNetworkLink("reverse-link-west-east", {
    resourceGroupName: rgEast.name,
    location: "global",
    privateZoneName: reverseDnsZoneWest.name,
    virtualNetworkLinkName: "link-east",
    registrationEnabled: false,
    virtualNetwork: { id: vnetEast.id },
});

const reverseLinkWestToWest = new privatedns.VirtualNetworkLink("reverse-link-west-west", {
    resourceGroupName: rgEast.name,
    location: "global",
    privateZoneName: reverseDnsZoneWest.name,
    virtualNetworkLinkName: "link-west",
    registrationEnabled: false,
    virtualNetwork: { id: vnetWest.id },
});

// A records for VMs in the private zone
const eastARecord = new privatedns.PrivateRecordSet("east-a", {
    resourceGroupName: rgEast.name,
    privateZoneName: dnsZone.name,
    relativeRecordSetName: "vm-eastus",
    recordType: "A",
    ttl: 60,
    aRecords: [
        { ipv4Address: nicEast.ipConfigurations.apply(cfg => cfg?.[0]?.privateIPAddress || "") },
    ],
});

const westARecord = new privatedns.PrivateRecordSet("west-a", {
    resourceGroupName: rgEast.name,
    privateZoneName: dnsZone.name,
    relativeRecordSetName: "vm-westeurope",
    recordType: "A",
    ttl: 60,
    aRecords: [
        { ipv4Address: nicWest.ipConfigurations.apply(cfg => cfg?.[0]?.privateIPAddress || "") },
    ],
});

const builderhubARecord = new privatedns.PrivateRecordSet("builderhub-a", {
    resourceGroupName: rgEast.name,
    privateZoneName: dnsZone.name,
    relativeRecordSetName: "builderhub",
    recordType: "A",
    ttl: 60,
    aRecords: [
        { ipv4Address: nicBuilderhub.ipConfigurations.apply(cfg => cfg?.[0]?.privateIPAddress || "") },
    ],
});

const east2ARecord = new privatedns.PrivateRecordSet("east-2-a", {
    resourceGroupName: rgEast.name,
    privateZoneName: dnsZone.name,
    relativeRecordSetName: "vm-eastus-2",
    recordType: "A",
    ttl: 60,
    aRecords: [
        { ipv4Address: nicEast2.ipConfigurations.apply(cfg => cfg?.[0]?.privateIPAddress || "") },
    ],
});

const west2ARecord = new privatedns.PrivateRecordSet("west-2-a", {
    resourceGroupName: rgEast.name,
    privateZoneName: dnsZone.name,
    relativeRecordSetName: "vm-westeurope-2",
    recordType: "A",
    ttl: 60,
    aRecords: [
        { ipv4Address: nicWest2.ipConfigurations.apply(cfg => cfg?.[0]?.privateIPAddress || "") },
    ],
});

// PTR records for reverse DNS lookups (East US VMs in 1.10.10.in-addr.arpa zone)
const eastPtrRecord = new privatedns.PrivateRecordSet("east-ptr", {
    resourceGroupName: rgEast.name,
    privateZoneName: reverseDnsZoneEast.name,
    relativeRecordSetName: nicEast.ipConfigurations.apply(cfg => {
        const ip = cfg?.[0]?.privateIPAddress || "";
        return ip.split(".")[3] || "";
    }),
    recordType: "PTR",
    ttl: 60,
    ptrRecords: [
        { ptrdname: pulumi.interpolate`vm-eastus.${privateZoneName}` },
    ],
});

const east2PtrRecord = new privatedns.PrivateRecordSet("east-2-ptr", {
    resourceGroupName: rgEast.name,
    privateZoneName: reverseDnsZoneEast.name,
    relativeRecordSetName: nicEast2.ipConfigurations.apply(cfg => {
        const ip = cfg?.[0]?.privateIPAddress || "";
        return ip.split(".")[3] || "";
    }),
    recordType: "PTR",
    ttl: 60,
    ptrRecords: [
        { ptrdname: pulumi.interpolate`vm-eastus-2.${privateZoneName}` },
    ],
});

const builderhubPtrRecord = new privatedns.PrivateRecordSet("builderhub-ptr", {
    resourceGroupName: rgEast.name,
    privateZoneName: reverseDnsZoneEast.name,
    relativeRecordSetName: nicBuilderhub.ipConfigurations.apply(cfg => {
        const ip = cfg?.[0]?.privateIPAddress || "";
        return ip.split(".")[3] || "";
    }),
    recordType: "PTR",
    ttl: 60,
    ptrRecords: [
        { ptrdname: pulumi.interpolate`builderhub.${privateZoneName}` },
    ],
});

// PTR records for West Europe VMs in 1.20.10.in-addr.arpa zone
const westPtrRecord = new privatedns.PrivateRecordSet("west-ptr", {
    resourceGroupName: rgEast.name,
    privateZoneName: reverseDnsZoneWest.name,
    relativeRecordSetName: nicWest.ipConfigurations.apply(cfg => {
        const ip = cfg?.[0]?.privateIPAddress || "";
        return ip.split(".")[3] || "";
    }),
    recordType: "PTR",
    ttl: 60,
    ptrRecords: [
        { ptrdname: pulumi.interpolate`vm-westeurope.${privateZoneName}` },
    ],
});

const west2PtrRecord = new privatedns.PrivateRecordSet("west-2-ptr", {
    resourceGroupName: rgEast.name,
    privateZoneName: reverseDnsZoneWest.name,
    relativeRecordSetName: nicWest2.ipConfigurations.apply(cfg => {
        const ip = cfg?.[0]?.privateIPAddress || "";
        return ip.split(".")[3] || "";
    }),
    recordType: "PTR",
    ttl: 60,
    ptrRecords: [
        { ptrdname: pulumi.interpolate`vm-westeurope-2.${privateZoneName}` },
    ],
});

// Outputs
export const eastPublicIp = pipEast.ipAddress;
export const westPublicIp = pipWest.ipAddress;
export const eastPrivateIp = nicEast.ipConfigurations.apply(cfg => cfg?.[0]?.privateIPAddress);
export const westPrivateIp = nicWest.ipConfigurations.apply(cfg => cfg?.[0]?.privateIPAddress);
export const builderhubPrivateIp = nicBuilderhub.ipConfigurations.apply(cfg => cfg?.[0]?.privateIPAddress);
export const builderhubNicPrivateFqdn = nicBuilderhub.dnsSettings.apply(ds => ds?.internalFqdn);
export const eastPrivateFqdn = pulumi.interpolate`vm-eastus.${privateZoneName}`;
export const westPrivateFqdn = pulumi.interpolate`vm-westeurope.${privateZoneName}`;
export const east2PrivateFqdn = pulumi.interpolate`vm-eastus-2.${privateZoneName}`;
export const west2PrivateFqdn = pulumi.interpolate`vm-westeurope-2.${privateZoneName}`;
export const builderhubPrivateFqdn = pulumi.interpolate`builderhub.${privateZoneName}`;
export const east2PrivateIp = nicEast2.ipConfigurations.apply(cfg => cfg?.[0]?.privateIPAddress);
export const west2PrivateIp = nicWest2.ipConfigurations.apply(cfg => cfg?.[0]?.privateIPAddress);
export const east2NicPrivateFqdn = nicEast2.dnsSettings.apply(ds => ds?.internalFqdn);
export const west2NicPrivateFqdn = nicWest2.dnsSettings.apply(ds => ds?.internalFqdn);