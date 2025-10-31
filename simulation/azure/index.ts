import * as pulumi from "@pulumi/pulumi";
import * as resources from "@pulumi/azure-native/resources";
import * as network from "@pulumi/azure-native/network";
import * as compute from "@pulumi/azure-native/compute";
import * as privatedns from "@pulumi/azure-native/privatedns";

// Configuration
const config = new pulumi.Config();
const adminUsername = config.get("adminUsername") || "azureuser";
const sshPublicKey = config.require("sshPublicKey"); // e.g. contents of ~/.ssh/id_rsa.pub
const allowedSshCidr = config.get("allowedSshCidr") || "0.0.0.0/0"; // restrict in config for better security
const tailscaleAuthKey = config.getSecret("tailscaleAuthKey"); // optional: set as Pulumi secret

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
                name: "Allow-ICMP-From-Peer",
                priority: 1010,
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

function createVm(name: string, rg: resources.ResourceGroup, location: pulumi.Input<string>, nicId: pulumi.Input<string>) {
    // Optional cloud-init to install and bring up Tailscale
    const customData = tailscaleAuthKey
        ? tailscaleAuthKey.apply(key =>
              Buffer.from(`#cloud-config
package_update: true
package_upgrade: false
write_files:
  - path: /etc/sysctl.d/99-tailscale-ipforward.conf
    permissions: '0644'
    owner: root:root
    content: |
      net.ipv4.ip_forward=1
      net.ipv6.conf.all.forwarding=1
runcmd:
  - apt-get update
  - apt-get install -y curl
  - curl -fsSL https://tailscale.com/install.sh | sh
  - systemctl enable --now tailscaled
  - tailscale up --authkey=${key} --hostname=${name} --ssh
`).toString("base64"))
        : undefined;

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
            customData,
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
    });
}

const vmEast = createVm("vm-eastus", rgEast, rgEast.location, nicEast.id);
const vmWest = createVm("vm-westeurope", rgWest, rgWest.location, nicWest.id);

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
