# Integration hub for Apache Spark documentation

The Integration Hub for Apache Spark K8s is an open-source [Kubernetes charm](https://juju.is/docs/olm/charmed-operator) that manages and centralizes the integration of Charmed Apache Spark with other charms and services.

The Integration Hub charm handles relations (integrations) with other charmed operators (PostgreSQL, S3-integrator, etc.) and bundles (COS, Identity, etc.), manages service accounts, updates from integrated charms, and prepares configuration options for Spark jobs.

The Integration Hub charm helps manage integrations and configure Spark jobs for Charmed Apache Spark.
It can be useful for teams and engineers using Charmed Apache Spark with multiple integrations to other charms or bundles.

<!--

# Navigation

SEE TEMPLATE
-->

## Usage

<!--TODO: Remove this entire section once we have a dedicated page-->

To deploy this charm, use:

```shell
juju add-model <my-model>
juju deploy s3-integrator --channel latest/edge
juju deploy spark-integration-hub-k8s --channel edge
juju relate spark-integration-hub-k8s s3-integrator
```

> **Note**: You may use a different object storage, such as `azure-storage-integrator`.

When creating new Spark service account using the [`spark-client` snap](https://snapcraft.io/spark-client)

```shell
spark-client.service-account-registry create --username <spark-user> --namespace <namespace>
```

The Integration Hub charm will take care of adding relevant configuration to the
Charmed Spark properties,

```shell
spark-client.service-account-registry get-config --username <spark-user> --namespace <namespace>
```

## Project and community

The Integration Hub for Apache Spark K8s is a member of the Ubuntu family.
It is an open-source project that warmly welcomes community projects, contributions, suggestions, fixes and constructive feedback.

- [Code of conduct](https://ubuntu.com/community/code-of-conduct)
- [Get support](https://canonical.com/data)
- Meet the community and chat with us on [Matrix](https://matrix.to/#/#charmhub-data-platform:ubuntu.com)
- [Contribute](https://github.com/canonical/spark-integration-hub-k8s-operator/blob/main/CONTRIBUTING.md) and report [issues](https://github.com/canonical/spark-integration-hub-k8s-operator/issues/new)

Thinking about using Charmed Apache Spark for your next project? [Get in touch!](https://canonical.com/data)

## License

The Integration Hub for Apache Spark K8s charm is free software, distributed under the Apache Software License, version 2.0.
See [LICENSE](https://github.com/canonical/spark-integration-hub-k8s-operator/blob/main/LICENSE) for more information.

Apache Spark is a free, open-source software project by the Apache Software Foundation.
Users can find out more at the [Apache Spark project page](https://spark.apache.org/).
