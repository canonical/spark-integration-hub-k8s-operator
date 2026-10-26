# Integration Hub for Apache Spark K8s Operator

The Integration Hub for Apache Spark K8s charm is the charm that enables the integration of the Charmed Apache Spark solution with existing charms (PostgreSQL, S3-integrator, etc) and bundles (COS, Identity, etc) and properly configure the corresponding option in the Apache Spark ecosystem.

The main tasks of the Integration Hub charm are the following:

- Handle relations with other charms and translate them in configuration options
  that Spark can handle (like the Spark History Server charm does with the S3-integrator).
- Store the configuration options in Kubernetes secrets for the service account
  managed by the Charmed Apache Spark solution (all service account are labeled with predefined labels)
- Handle the addition/removal of service accounts with corresponding updates of
  configurations.
- Handle updates from the related charms (i.e., new database credentials,
  change of endpoints, updated S3 credentials.)
- The Integration Hub charm may also be able to create and configure services
  accounts that are able to run Spark jobs.

The Integration Hub for Apache Spark K8s charm is responsible for the generation of the
proper configuration options for the Spark jobs that will be run with the
Client tools snap for Apache Spark.

The Integration Hub for Apache Spark K8s charm will need to get all service accounts that are associated with the Charmed Apache Spark solution.
This implies that the charm needs to detect existing and new service accounts in order to add the corresponding configuration in the Kubernetes secrets related to those service accounts.

## Usage

```shell
juju add-model <my-model>
juju deploy spark-integration-hub-k8s --channel edge --trust
```

You can use the `spark-integration-hub` to automatically configure Spark service account. For instance, you can inject s3 credentials by deploying the `s3-integrator`, configuring it and then relating with the `spark-integration-hub` as follows:

```shell
juju deploy s3-integrator --channel 2/stable

SECRET_URI=$(juju add-secret s3-creds access-key=<access-key> secret-key=<secret-key>)
juju grant-secret s3-creds s3-integrator

juju config s3-integrator \
  endpoint=<s3-endpoint> \
  bucket=<bucket> \
  path=<path> \
  region=<region> \
  credentials=$SECRET_URI

juju integrate spark-integration-hub-k8s s3-integrator
```

> [!NOTE]  
> If the `region` is not configured in the `s3-integrator` charm before integrating it with `spark-integration-hub-k8s` charm, the `spark-integration-hub-k8s` charm uses `us-east-1` as the region to send requests to S3.

When creating new Spark service account using the [`spark-client` snap](https://snapcraft.io/spark-client)

```shell
spark-client.service-account-registry create --username <spark-user> --namespace <namespace>
```

The Integration Hub will take care of adding relevant configuration to the Charmed Apache Spark properties. You can check this by waiting a few seconds and then check that the Spark service account configuration have been updated to include s3 configurations, e.g. 

```shell
spark-client.service-account-registry get-config --username <spark-user> --namespace <namespace>
```

should show something like:

```shell
spark.eventLog.dir=s3a://<bucket>/<path>
spark.eventLog.enabled=true
spark.hadoop.fs.s3a.access.key=<access-key>
spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.connection.ssl.enabled=false
spark.hadoop.fs.s3a.endpoint=<s3-endpoint>
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.secret.key=<secret-key>
spark.history.fs.logDirectory=s3a://<bucket>/<path>
spark.kubernetes.file.upload.path=s3a://<bucket>/
spark.sql.warehouse.dir=s3a://<bucket>/warehouse
spark.kubernetes.authenticate.driver.serviceAccountName=<spark-user>
spark.kubernetes.namespace=<namespace>
```

## Live Spark UI exposure (POC)

This POC exposes live **cluster-mode applications** through Istio ingress.
Two additional, separate Pebble services provide exposure: `spark-ui` watches
driver Pods and manages application HTTPRoutes; `spark-ui-index` serves a basic
read-only index on port 8080. There is no UI proxy in the Hub.
Each application can contain multiple Spark jobs.

Build the matching image from `../spark-integration-hub-rock` (`rockcraft pack`)
and publish it to a registry reachable by the Kubernetes cluster. The released
image does not contain the new `app.spark_ui` module.

Build this charm (`charmcraft pack`), then deploy both artifacts:

```shell
juju deploy ./<built-charm>.charm spark-integration-hub-k8s --trust \
  --resource integration-hub-image=<registry>/<image>:<poc-tag>

juju integrate spark-integration-hub-k8s:ingress istio-ingress-k8s:ingress
juju integrate spark-integration-hub-k8s:gateway-metadata istio-ingress-k8s:gateway-metadata
```

Both relations must point at the **same Istio ingress**. For ambient workloads,
also relate the hub to the beacon as usual:

```shell
juju integrate spark-integration-hub-k8s:service-mesh istio-beacon-k8s:service-mesh
```

Keep the normal monitored-service-account configuration or client relations.
Wait until `spark-client.service-account-registry get-config` shows
`spark.ui.proxyRedirectUri`, then submit a new application. Exposure follows the
Hub's existing monitored service-account scope; no per-application opt-in label
is required. Do **not** set `spark.ui.proxyBase` or
`APPLICATION_WEB_PROXY_BASE`; they override the per-request prefix header.
Restart/re-submit applications after the external ingress URL changes.

The hub's active status displays the index URL allocated by ingress. The page
lists this hub's application routes with links, querying Kubernetes on each page
request rather than sharing in-memory state with the Pod watcher.
You can also construct a URL directly: append the
workload namespace and the driver's `spark-app-selector` value:

```text
http://<gateway-IP>/<model>-<hub-app>/<namespace>/<application-id>/
```

There is no extra `/spark-ui` prefix. For example:

```text
http://10.64.140.44/my-model-spark-integration-hub/bikalpa/spark-abc123/
```

The ordinary ingress relation routes the base URL to `spark-ui-index` on port 8080.
The longer application-specific paths route directly to drivers, bypassing the
index service. Restarting the index does not stop the watcher or existing routes.

### How it works

- The charm keeps relation data in its context and generates the controller
  configuration in `/etc/hub/conf/spark-ui.json`.
- A separate Pebble service, `spark-ui`, watches Pods labelled `spark-role=driver`.
  It validates their accounts against the same allowlist as the existing
  service-account watcher, then looks up Services by namespace and application ID,
  checking Pod ownership and selectors. It does not watch Services separately.
  An initial reconciliation and a reconciliation after watch events or a 30-second
  idle wait recover missed events and retry Service discovery. Watch failures
  cause the process to fail explicitly so Pebble can restart it.
- The controller creates one `HTTPRoute` in the workload namespace per running
  application, under `<ingress-url>/<namespace>/<spark-app-selector>/`. The route
  strips that prefix, sets `X-Forwarded-Context`, and forwards directly from the
  gateway to the driver's Service UI port (normally 4040). The hub is not a UI proxy.
- With mesh enabled and both ingress relations ready, the charm adds
  `<gateway-namespace>:<gateway-service-account>` from `gateway-metadata` to
  `CLIENT_APP_SERVICE_ACCOUNTS` (the existing variable, not
  `CLIENT_SERVICE_ACCOUNTS`), preserving related clients such as Kyuubi.
  The existing `integration-hub` account watcher creates the corresponding
  `AuthorizationPolicy`. The new service never creates or deletes policies.
- These existing client policies select all `spark-role=driver` Pods in each
  monitored workload namespace and permit the gateway on **all ports**, not just
  the UI port. They assume the `cluster.local` trust domain. This deliberately
  reuses existing POC behaviour rather than adding a port-scoped policy path.
- Routes are owned by the driver Service. Kubernetes removes them
  when that Service is deleted; reconciliation also removes routes for
  terminated drivers, disallowed accounts, or disabled exposure. Each hub has an
  isolated ownership label. The stop hook stops reconciliation before cleaning up.
- Account-watcher policies have a separate lifecycle, tied to monitored service
  accounts and configured client identities, not individual driver Pods. Removing
  ingress or gateway metadata drops the gateway from the client list and restarts
  the account watcher so its existing reconciliation removes obsolete policies.

### POC boundaries

- Requires Gateway API v1 and the Istio ingress implementation with standard
  `http-80` / `https-443` listeners and permission for routes from workload
  namespaces. There is no Traefik mode, client-mode driver support, History Server
  redirect, or `spark-submit` URL printing.
- The route watcher needs cluster-wide `list/watch` on driver Pods,
  `list` on Services and service accounts, and `list/create/patch/delete` on
  HTTPRoutes. The existing account watcher retains its Secret and
  AuthorizationPolicy permissions. Trust is required; inspect workload logs
  for explicit RBAC failures.
- The separate index process only lists this hub's HTTPRoutes. It does not
  create resources or perform Pod watches, and needs no shared snapshot file.
  It returns 503 with logged errors if route discovery fails, rather than
  presenting an empty success page. Its process health is included in charm status.
- There is no per-user or tenant authorization. Authentication is only provided
  when the shared ingress has been configured for it. Use a trusted POC
  environment: Spark UIs can reveal application data and offer kill actions.
  The index exposes application IDs and namespaces for the whole monitored scope;
  it does not filter the list by browser user.
- Discovery also finds existing drivers in monitored accounts, but those drivers
  may lack the injected `spark.ui.proxyRedirectUri` and need re-submission for
  correct UI redirects. Avoid overlapping monitored-account scopes across hubs:
  this POC does not arbitrate competing exposure configurations for one application.
- Routes are created for running drivers, but `Running` does not prove that
  the UI is listening. Check HTTPRoute `Accepted`/`ResolvedRefs` conditions and
  expect transient startup 503s. Successfully applying a route is not a
  reachability guarantee; there are no HTTP probes in this POC.
- Per-application routes are created outside the ingress charm's conflict
  detection. Do not allow another app to claim paths beneath this hub's prefix.
- A driver crash can leave a Service behind until its Pod is deleted. During a hub
  outage, a stale route may therefore remain until Kubernetes ownership cleanup
  or the next successful reconciliation.
- This POC requires the rebuilt rock and charm together. Unit/integration tests
  and live-cluster validation have not yet been added.

## Contributing

Please see the [Juju SDK docs](https://juju.is/docs/sdk) for guidelines on enhancements
to this charm following best practice guidelines, and
[CONTRIBUTING.md](https://github.com/canonical/spark-integration-hub-k8s-operator/blob/main/CONTRIBUTING.md)
for developer guidance.

### We are Hiring!

Also, if you truly enjoy working on open-source projects like this one and you
would like to be part of the OSS revolution, please don't forget to check out
the [open positions](https://canonical.com/careers/all) we have at [Canonical](https://canonical.com/).

## License

The Integration Hub for Apache Spark K8s charm is free software, distributed under the Apache Software License, version 2.0.
See [LICENSE](https://github.com/canonical/spark-integration-hub-k8s-operator/blob/main/LICENSE) for more information.

Apache Spark is a free, open-source software project by the Apache Software Foundation.
Users can find out more at the [Apache Spark project page](https://spark.apache.org/).
