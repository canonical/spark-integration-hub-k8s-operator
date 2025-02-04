# Spark Integration Hub K8s documentation

Spark Integration Hub K8s is an open-source [Kubernetes charm](https://juju.is/docs/olm/charmed-operator) that enables the integration of the Charmed Apache Spark solution with existing charms (PostgreSQL, S3-integrator, etc.) or bundles (COS, Identity, etc.) and properly configure the corresponding option in the Spark ecosystem.

The Spark Integration Hub K8s charm provides centralization for other charmed components and generates the configuration options for the Spark jobs that will be run with the Spark Client snap, abstracting low-level configuration option settings.

The needs met by the Spark Integration Hub K8s charm are the following:

- Handle relations with other charms and translate them into configuration options that Spark can handle.
- Store the configuration options in Kubernetes secrets for the service account managed by the Apache Charmed Spark solution (all service accounts are labeled with predefined labels)
- Handle the addition/removal of service accounts with corresponding updates of configurations.
- Handle updates from the related charms (i.e., new database credentials, change of endpoints, updated Azure storage credentials.)
- The configuration hub charm may also be able to create and configure services accounts that can run Spark jobs.

Spark Integration Hub K8s is a solution designed and developed to help teams streamline the setup and hands-on experience of Charmed Apache Spark.

Spark Integration Hub K8s is developed and supported by [Canonical](https://canonical.com/), as part of its commitment to provide open-source, self-driving solutions, seamlessly integrated using the Operator Framework Juju.
Please refer to [Charmhub](https://charmhub.io/), for more charmed operators that can be integrated by [Juju](https://juju.is/).


<!--

# Navigation

SEE TEMPLATE
-->

## Project and community

Spark Integration Hub K8s is a member of the Ubuntu family.
It is an open-source project that warmly welcomes community projects, contributions, suggestions, fixes and constructive feedback.

- [Code of conduct](https://ubuntu.com/community/code-of-conduct)
- [Get support](https://canonical.com/data)
- Meet the community and chat with us on [Matrix](https://matrix.to/#/#charmhub-data-platform:ubuntu.com)
- [Contribute](https://github.com/canonical/spark-integration-hub-k8s-operator/blob/main/CONTRIBUTING.md) and report [issues](https://github.com/canonical/spark-integration-hub-k8s-operator/issues/new)

Thinking about using Charmed Apache Spark for your next project? [Get in touch!](https://canonical.com/data)

## License

The Spark Integration Hub K8s charm is free software, distributed under the Apache Software License, version 2.0.
See [LICENSE](https://github.com/canonical/spark-integration-hub-k8s-operator/blob/main/LICENSE) for more information.

Apache Spark is a free, open-source software project by the Apache Software Foundation.
Users can find out more at the [Apache Spark project page](https://spark.apache.org/).
