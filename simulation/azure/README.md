# Azure Staging Environment

## Requirements
- Azure CLI
- Pulumi CLI

## Setup
- Log in to Azure: `az login`
- Copy `Pulumi.dev.example.yaml` to `Pulumi.dev.yaml`

- Set config values:
```bash
pulumi config set --secret flowproxy-staging:sshPublicKey "ssh-ed25519 ..."
pulumi config set flowproxy-staging:adminUsername "<your-username>"
pulumi config set flowproxy-staging:haProxyVersion "3.0.6@sha256:0f3127e63b00982c3f12b2a9a17ecbd0595003a191ec1cb403741a692f7a39a9"
# Optional
pulumi config set --secret flowproxy-staging:tailscaleAuthKey "<your-tailscale-auth-key>"
```

### Config Files
- [`haproxy.cfg`](haproxy.cfg): Stripped down HAProxy configuration for FlowProxy, based on [BuilderNet configuration file](https://github.com/flashbots/meta-evm/blob/main/recipes-nodes/haproxy/haproxy.cfg.mustache).

## Run
- `pulumi up`

## Destroy
- `pulumi destroy`