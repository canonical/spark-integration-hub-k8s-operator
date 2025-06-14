import json
from unittest import mock

import yaml
from charms.spark_integration_hub_k8s.v0.spark_service_account import (
    ServiceAccountGoneEvent,
    ServiceAccountGrantedEvent,
    ServiceAccountPropertyChangedEvent,
    ServiceAccountReleasedEvent,
    ServiceAccountRequestedEvent,
    SparkServiceAccountProvider,
    SparkServiceAccountRequirer,
)
from ops.charm import ActionEvent, CharmBase
from ops.testing import Context, Relation, Secret, State

PROVIDER_APP = "service-account-provider"
REQUIRER_APP = "service-account-requirer"
SERVICE_ACCOUNT = "default:user"
RELATION_INTERFACE = "spark_service_account"
RELATION_NAME = "spark-service-account"
SPARK_PROPS = {"foo": "bar"}
RESOURCE_MANIFEST = {"foo": "bar"}


class SparkServiceAccountProviderCharm(CharmBase):
    """Mock the provider charm for Spark Service Account relation for testing."""

    META = {
        "name": PROVIDER_APP,
        "provides": {RELATION_NAME: {"interface": RELATION_INTERFACE}},
    }

    ACTIONS = {"add-config": {"params": {"conf": {"type": "string"}}}}

    def __init__(self, *args):
        super().__init__(*args)
        self.provider = SparkServiceAccountProvider(self, relation_name=RELATION_NAME)
        self.framework.observe(self.provider.on.account_requested, self._on_account_requested)
        self.framework.observe(self.provider.on.account_released, self._on_account_released)
        self.framework.observe(self.on.add_config_action, self._on_add_config_action)

    def _generate_service_account_manifest(self, service_account: str) -> dict:
        print(f"Generated service account resource manifest: {service_account}")
        return RESOURCE_MANIFEST

    def _create_service_account(self, service_account: str) -> None:
        print(f"Created service account: {service_account}")

    def _delete_service_account(self, service_account: str) -> None:
        print(f"Deleted service account: {service_account}")

    def _on_account_requested(self, event: ServiceAccountRequestedEvent) -> None:
        if not event.service_account:
            return
        service_account = event.service_account
        if not event.skip_creation:
            self._create_service_account(service_account)
        manifest = self._generate_service_account_manifest(service_account=service_account)
        self.provider.set_service_account(event.relation.id, service_account)
        self.provider.set_spark_properties(event.relation.id, json.dumps(SPARK_PROPS))
        self.provider.set_resource_manifest(
            event.relation.id, resource_manifest=yaml.dump(manifest)
        )

    def _on_account_released(self, event: ServiceAccountReleasedEvent) -> None:
        if not event.service_account:
            return
        service_account = event.service_account
        skip_creation = event.skip_creation
        if not skip_creation:
            self._delete_service_account(service_account)

    def _on_add_config_action(self, event: ActionEvent) -> None:
        conf = event.params["conf"]
        key, val = conf.split("=", 1)
        props = SPARK_PROPS.copy()
        props.update({key: val})
        for rel in self.provider.relations:
            self.provider.set_spark_properties(rel.id, json.dumps(props))


class SparkServiceAccountRequirerCharm(CharmBase):
    META = {"name": REQUIRER_APP, "requires": {RELATION_NAME: {"interface": RELATION_INTERFACE}}}

    CONFIG = {"options": {"skip-creation": {"type": "string", "default": "false"}}}

    def __init__(self, *args):
        super().__init__(*args)
        skip_creation = self.config.get("skip-creation", "false") == "true"
        self.requirer = SparkServiceAccountRequirer(
            self,
            relation_name=RELATION_NAME,
            service_account=SERVICE_ACCOUNT,
            skip_creation=skip_creation,
        )
        self.framework.observe(self.requirer.on.account_granted, self._on_account_granted)
        self.framework.observe(self.requirer.on.account_gone, self._on_account_gone)
        self.framework.observe(self.requirer.on.properties_changed, self._on_properties_changed)

    def _consume_service_account(
        self,
        service_account: str | None,
        spark_properties: str | None,
        resource_manifest: str | None,
    ) -> None:
        if not service_account or not spark_properties or not resource_manifest:
            return
        print(f"Consuming service account: {service_account}.")
        props = json.loads(spark_properties)
        print(f"Spark properties are: {props}")
        manifest = yaml.safe_load(resource_manifest) if resource_manifest else None
        print(f"Resource manifest is: {manifest}")

    def _on_account_granted(self, event: ServiceAccountGrantedEvent) -> None:
        service_account = event.service_account
        spark_properties = event.spark_properties
        resource_manifest = event.resource_manifest
        self._consume_service_account(service_account, spark_properties, resource_manifest)

    def _on_account_gone(self, event: ServiceAccountGoneEvent) -> None:
        self._consume_service_account(None, None, None)

    def _on_properties_changed(self, event: ServiceAccountPropertyChangedEvent) -> None:
        service_account = event.service_account
        spark_properties = event.spark_properties
        resource_manifest = event.resource_manifest
        self._consume_service_account(service_account, spark_properties, resource_manifest)


class TestSparkServiceAccountProvider:
    def get_relation(self):
        return Relation(
            endpoint=RELATION_NAME,
            interface=RELATION_INTERFACE,
            remote_app_name=REQUIRER_APP,
            local_app_data={},
            remote_app_data={},
        )

    @property
    def context(self):
        return Context(
            charm_type=SparkServiceAccountProviderCharm,
            meta=SparkServiceAccountProviderCharm.META,
            actions=SparkServiceAccountProviderCharm.ACTIONS,
        )

    @mock.patch.object(SparkServiceAccountProviderCharm, "_create_service_account")
    def test_service_account_created_by_provider(self, mock_create_sa):
        relation = self.get_relation()
        state1 = State(relations=[relation], leader=True)
        relation.remote_app_data.update(
            {
                "service-account": SERVICE_ACCOUNT,
                "requested-secrets": json.dumps(["spark-properties", "resource-manifest"]),
            }
        )

        state2 = self.context.run(self.context.on.relation_changed(relation), state1)
        mock_create_sa.assert_called_with(SERVICE_ACCOUNT)

        local_app_data = state2.get_relation(relation.id).local_app_data
        assert local_app_data["service-account"] == SERVICE_ACCOUNT
        # assert local_app_data["resource-manifest"] == yaml.dump(RESOURCE_MANIFEST)

        assert "secret-extra" in local_app_data

        secret_id = local_app_data["secret-extra"]
        secret_content = state2.get_secret(id=secret_id).latest_content
        assert secret_content is not None
        spark_properties = json.loads(secret_content["spark-properties"])
        assert spark_properties == SPARK_PROPS
        resource_manifest = yaml.safe_load(secret_content["resource-manifest"])
        assert resource_manifest == RESOURCE_MANIFEST

    @mock.patch.object(SparkServiceAccountProviderCharm, "_create_service_account")
    def test_service_account_creation_skipped(self, mock_create_sa):
        relation = self.get_relation()
        state1 = State(relations=[relation], leader=True)
        relation.remote_app_data.update(
            {
                "service-account": SERVICE_ACCOUNT,
                "requested-secrets": json.dumps(["spark-properties", "resource-manifest"]),
                "skip-creation": "true",
            }
        )

        state2 = self.context.run(self.context.on.relation_changed(relation), state1)
        assert not mock_create_sa.called

        local_app_data = state2.get_relation(relation.id).local_app_data
        assert local_app_data["service-account"] == SERVICE_ACCOUNT
        # assert local_app_data["resource-manifest"] == yaml.dump(RESOURCE_MANIFEST)
        assert "secret-extra" in local_app_data

        secret_id = local_app_data["secret-extra"]
        secret_content = state2.get_secret(id=secret_id).latest_content
        assert secret_content is not None
        spark_properties = json.loads(secret_content["spark-properties"])
        assert spark_properties == SPARK_PROPS
        resource_manifest = yaml.safe_load(secret_content["resource-manifest"])
        assert resource_manifest == RESOURCE_MANIFEST

    def test_service_account_property_changed(
        self,
    ):
        relation = self.get_relation()
        state1 = State(relations=[relation], leader=True)
        relation.remote_app_data.update(
            {
                "service-account": SERVICE_ACCOUNT,
                "requested-secrets": json.dumps(["spark-properties", "resource-manifest"]),
            }
        )
        state2 = self.context.run(self.context.on.relation_changed(relation), state1)
        state3 = self.context.run(
            self.context.on.action("add-config", params={"conf": "newkey=newval"}), state2
        )

        local_app_data = state3.get_relation(relation.id).local_app_data
        assert local_app_data["service-account"] == SERVICE_ACCOUNT
        assert "secret-extra" in local_app_data

        secret_id = local_app_data["secret-extra"]
        secret_content = state3.get_secret(id=secret_id).latest_content
        assert secret_content is not None
        spark_properties = json.loads(secret_content["spark-properties"])
        assert spark_properties["newkey"] == "newval"
        resource_manifest = yaml.safe_load(secret_content["resource-manifest"])
        assert resource_manifest == RESOURCE_MANIFEST

    @mock.patch.object(SparkServiceAccountProviderCharm, "_delete_service_account")
    def test_service_account_released(self, mock_delete_sa):
        relation = self.get_relation()
        relation.remote_app_data.update(
            {
                "service-account": SERVICE_ACCOUNT,
                "requested-secrets": json.dumps(["spark-properties", "resource-manifest"]),
            }
        )
        state1 = State(relations=[relation], leader=True)
        self.context.run(self.context.on.relation_broken(relation), state1)
        mock_delete_sa.assert_called_with(SERVICE_ACCOUNT)

    @mock.patch.object(SparkServiceAccountProviderCharm, "_delete_service_account")
    def test_service_account_released_skip_deletion(self, mock_delete_sa):
        relation = self.get_relation()
        relation.remote_app_data.update(
            {
                "service-account": SERVICE_ACCOUNT,
                "requested-secrets": json.dumps(["spark-properties", "resource-manifest"]),
                "skip-creation": "true",
            }
        )
        state1 = State(relations=[relation], leader=True)
        self.context.run(self.context.on.relation_broken(relation), state1)
        assert not mock_delete_sa.called


class TestSparkServiceAccountRequirer:
    def get_relation(self) -> Relation:
        return Relation(
            endpoint=RELATION_NAME,
            interface=RELATION_INTERFACE,
            remote_app_name=PROVIDER_APP,
            local_app_data={
                "service-account": SERVICE_ACCOUNT,
                "requested-secrets": json.dumps(["spark-properties", "resource-manifest"]),
            },
            remote_app_data={},
        )

    @property
    def context(self):
        return Context(
            charm_type=SparkServiceAccountRequirerCharm,
            meta=SparkServiceAccountRequirerCharm.META,
            config=SparkServiceAccountRequirerCharm.CONFIG,
        )

    @mock.patch.object(SparkServiceAccountRequirerCharm, "_consume_service_account")
    def test_service_account_granted(self, mock_consume_sa):
        relation = self.get_relation()
        props_secret = Secret(
            tracked_content={
                "spark-properties": json.dumps(SPARK_PROPS),
                "resource-manifest": yaml.dump(RESOURCE_MANIFEST),
            },
            label=f"{RELATION_NAME}.{relation.id}.extra.secret",
        )
        relation.remote_app_data.update(
            {"service-account": SERVICE_ACCOUNT, "secret-extra": props_secret.id}
        )

        state1 = State(
            relations=[relation],
            secrets=[props_secret],
            leader=True,
        )
        self.context.run(self.context.on.relation_changed(relation), state1)

        args, kwargs = mock_consume_sa.call_args
        service_account, spark_properties, resource_manifest = args

        assert service_account == SERVICE_ACCOUNT
        assert spark_properties == json.dumps(SPARK_PROPS)
        assert resource_manifest == yaml.dump(RESOURCE_MANIFEST)

    @mock.patch.object(SparkServiceAccountRequirerCharm, "_consume_service_account")
    def test_spark_properties_changed(self, mock_consume_sa):
        relation = self.get_relation()
        props_secret = Secret(
            tracked_content={
                "spark-properties": json.dumps(SPARK_PROPS),
                "resource-manifest": yaml.dump(RESOURCE_MANIFEST),
            },
            label=f"{RELATION_NAME}.{relation.id}.extra.secret",
        )
        relation.remote_app_data.update(
            {"service-account": SERVICE_ACCOUNT, "secret-extra": props_secret.id}
        )
        state1 = State(
            relations=[relation],
            secrets=[props_secret],
            leader=True,
            config={"skip-creation": "true"},
        )

        self.context.run(self.context.on.secret_changed(props_secret), state1)

        args, kwargs = mock_consume_sa.call_args
        service_account, spark_properties, resource_manifest = args
        assert service_account == SERVICE_ACCOUNT
        assert "foo" in json.loads(spark_properties)
        assert resource_manifest == yaml.dump(RESOURCE_MANIFEST)

    @mock.patch.object(SparkServiceAccountRequirerCharm, "_consume_service_account")
    def test_service_account_gone(self, mock_consume_sa):
        relation = self.get_relation()
        props_secret = Secret(
            tracked_content={"spark-properties": json.dumps(SPARK_PROPS)},
            label=f"{RELATION_NAME}.{relation.id}.extra.secret",
        )
        relation.remote_app_data.update(
            {"service-account": SERVICE_ACCOUNT, "secret-extra": props_secret.id}
        )
        state1 = State(relations=[relation], secrets=[props_secret], leader=True)

        self.context.run(self.context.on.relation_broken(relation), state1)

        args, kwargs = mock_consume_sa.call_args
        service_account, spark_properties, resource_manifest = args
        assert service_account is None
        assert spark_properties is None
        assert resource_manifest is None
