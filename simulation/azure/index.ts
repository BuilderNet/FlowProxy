import * as pulumi from "@pulumi/pulumi";
import * as resources from "@pulumi/azure-native/resources";
import * as network from "@pulumi/azure-native/network";
import * as compute from "@pulumi/azure-native/compute";
import * as privatedns from "@pulumi/azure-native/privatedns";
import * as fs from "fs";
import * as crypto from "crypto";

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
// Azure limit: forceUpdateTag max 50 chars; use a short hash (32 chars)
const haExtTag = crypto
    .createHash("sha256")
    .update(haproxyCfg + String(haProxyVersion) + flowproxyEnv + flowproxyServiceTmpl + String(flowProxyArtifact))
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

// VM size: 2 vCPU, 4GB RAM
const vmSize = "Standard_B2s";

// Ubuntu image reference (22.04 LTS)
const ubuntuImageRef = {
    publisher: "Canonical",
    offer: "0001-com-ubuntu-server-jammy",
    sku: "22_04-lts",
    version: "latest",
};

function createVm(name: string, rg: resources.ResourceGroup, location: pulumi.Input<string>, nicId: pulumi.Input<string>, opts?: pulumi.CustomResourceOptions) {
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
                managedDisk: { storageAccountType: "Standard_LRS" },
            },
        },
    }, opts);
}

const vmEast = createVm("vm-eastus", rgEast, rgEast.location, nicEast.id, { ignoreChanges: ["osProfile"] });
const vmWest = createVm("vm-westeurope", rgWest, rgWest.location, nicWest.id, { ignoreChanges: ["osProfile"] });

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
function buildInstanceCommand(hostname: string): pulumi.Input<string> {
    const artifactApiUrl = toApiZip(flowProxyArtifact);
    const script = (key?: string, token?: string) => `#!/usr/bin/env bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y docker.io curl ca-certificates openssl unzip
mkdir -p /usr/local/etc/haproxy/certs /usr/local/etc/haproxy/static
echo "${haproxyCfgB64}" | base64 -d > /usr/local/etc/haproxy/haproxy.cfg
printf "\n" >> /usr/local/etc/haproxy/haproxy.cfg || true
if [ ! -f /usr/local/etc/haproxy/static/le.cer ]; then echo "placeholder-certificate-content" > /usr/local/etc/haproxy/static/le.cer; fi
if [ ! -f /usr/local/etc/haproxy/certs/default.pem ]; then
  openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout /usr/local/etc/haproxy/certs/default.key \
    -out /usr/local/etc/haproxy/certs/default.crt \
    -subj "/CN=localhost";
  cat /usr/local/etc/haproxy/certs/default.key /usr/local/etc/haproxy/certs/default.crt > /usr/local/etc/haproxy/certs/default.pem;
fi
rm -f /usr/local/etc/haproxy/certs/default.crt || true
systemctl enable --now docker
docker rm -f haproxy || true
docker pull haproxy:${haProxyVersion}
docker run -d --name haproxy --restart unless-stopped -p 80:80 -p 443:443 -p 5544:5544 -p 8405:8405 -v /usr/local/etc/haproxy:/usr/local/etc/haproxy:ro haproxy:${haProxyVersion}
${key ? `apt-get update
apt-get install -y curl
curl -fsSL https://tailscale.com/install.sh | sh
systemctl enable --now tailscaled
tailscale up --authkey=${key} --hostname=${hostname} --ssh` : ``}

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
echo "${flowproxyEnvB64}" | base64 -d > /etc/flowproxy/flowproxy.env
echo "${flowproxySvcB64}" | base64 -d > /etc/systemd/system/flowproxy@.service
systemctl daemon-reload
systemctl enable --now flowproxy@${hostname}
systemctl status flowproxy@${hostname} --no-pager || journalctl -u flowproxy@${hostname} --no-pager -n 100
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
        commandToExecute: buildInstanceCommand("vm-eastus"),
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
        commandToExecute: buildInstanceCommand("vm-westeurope"),
    },
}, { dependsOn: [vmWest] });

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

// Outputs
export const eastPublicIp = pipEast.ipAddress;
export const westPublicIp = pipWest.ipAddress;
export const sshEast = pulumi.interpolate`ssh ${adminUsername}@${eastPublicIp}`;
export const sshWest = pulumi.interpolate`ssh ${adminUsername}@${westPublicIp}`;
export const eastPrivateIp = nicEast.ipConfigurations.apply(cfg => cfg?.[0]?.privateIPAddress);
export const westPrivateIp = nicWest.ipConfigurations.apply(cfg => cfg?.[0]?.privateIPAddress);
export const eastNicPrivateFqdn = nicEast.dnsSettings.apply(ds => ds?.internalFqdn);
export const westNicPrivateFqdn = nicWest.dnsSettings.apply(ds => ds?.internalFqdn);
export const eastPrivateFqdn = pulumi.interpolate`vm-eastus.${privateZoneName}`;
export const westPrivateFqdn = pulumi.interpolate`vm-westeurope.${privateZoneName}`;
