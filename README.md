# Spark Integration Hub K8s Operator

The Spark Configuration Hub charm, is the charm that enables the integration of 
the Charmed Spark solution with existing charms (PostgreSQL, S3-integrator, etc) 
and bundles (COS, Identity, etc) and properly configure the corresponding 
option in the Spark ecosystem.

The main tasks of the configuration hub charm are the following:

* Handle relations with other charms and translate them in configuration options 
  that Spark can handle (like the Spark History Server charm does with the S3-integrator).
* Store the configuration options in Kubernetes secrets for the service account
  managed by the Charmed Spark solution (all service account are labeled with predefined labels)
* Handle the addition/removal of service accounts with corresponding updates of 
  configurations.
* Handle updates from the related charms (i.e., new database credentials, 
  change of endpoints, updated S3 credentials.)
* The configuration hub charm may also be able to create and configure services
  accounts that are able to run Spark jobs. This can be a possible scenario if
  we relate our solution with Kubeflow. 

The Spark Configuration Hub charm is responsible for the generation of the 
proper configuration options for the Spark jobs that will be run with the 
Spark Client snap. 

The configuration hub charm will need to get all service accounts that are 
associated with the Charmed Spark solution. This implies that the charm needs 
to detect existing and new service accounts in order to add the corresponding 
configuration in the kubernetes secrets related to those service accounts. 


## Usage

```shell
juju add-model <my-model>
juju deploy s3-integrator --channel latest/edge
juju deploy spark-integration-hub-k8s --channel edge
juju relate spark-integration-hub-k8s s3-integrator
```

When creating new Spark service account using the [`spark-client` snap](https://snapcraft.io/spark-client)

```shell
spark-client.service-account-registry create --username <spark-user> --namespace <my-model>
```

The Spark Integration Hub will take care of adding relevant configuration to the 
Charmed Spark properties, 

```shell
spark-client.service-account-registry get-config --username <spark-user> --namespace <my-model>
```

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
The Charmed Kafka Operator is free software, distributed under the 
Apache Software License, version 2.0. See [LICENSE](https://github.com/canonical/spark-integration-hub-k8s-operator/blob/main/LICENSE) 
for more information.

