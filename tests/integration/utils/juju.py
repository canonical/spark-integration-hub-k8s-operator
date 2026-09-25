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
