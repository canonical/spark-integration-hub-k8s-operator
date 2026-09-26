import json

import jubilant

from .k8s import get_pods_by_label


def get_unit_pod_names(model: str, application_name: str) -> list[str]:
    """Retrieve names of all pods belonging to a specific Juju application."""
    return get_pods_by_label(labels={"app.kubernetes.io/name": application_name}, namespace=model)


def get_unit_address(
    juju: jubilant.Juju,
    app_name: str,
    unit_number: int = 0,
) -> str:
    """Retrieve the IP address of a specific unit of an application."""
    status = juju.status()
    address = status.apps[app_name].units[f"{app_name}/{unit_number}"].address
    return address


def get_related_unit_data(
    juju: jubilant.Juju, app_name: str, relation_name: str, remote_app_name: str
) -> dict[str, dict[str, str]]:
    """Retrieves the application data from a specific relation.

    Args:
        juju: The Juju client object used to execute CLI commands.
        app_name: The name of the Juju application.
        relation_name: The name of the relation endpoint to query.
        remote_app_name: The name of the remote application on the other side of the relation.

    Returns:
        A dictionary containing the application data for the specified relation.

    Raises:
        ValueError: If no relation data can be found for the specified
            relation endpoint.
    """
    unit_name = f"{app_name}/0"
    remote_unit_name = f"{remote_app_name}/0"
    command_stdout = juju.cli("show-unit", unit_name, "--format=json")
    result = json.loads(command_stdout)
    relation_data = [
        v for v in result[unit_name]["relation-info"] if v["endpoint"] == relation_name
    ]
    if len(relation_data) == 0:
        raise ValueError(
            f"No relation data could be grabbed on relation with endpoint {relation_name}"
        )
    return {
        relation["relation-id"]: relation["related-units"][remote_unit_name]["data"]
        for relation in relation_data
    }
