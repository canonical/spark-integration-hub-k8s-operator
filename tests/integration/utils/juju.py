from .k8s import get_pods_by_label


def get_unit_pod_names(model: str, application_name: str) -> list[str]:
    """Retrieve names of all pods belonging to a specific Juju application."""
    return get_pods_by_label(labels={"app.kubernetes.io/name": application_name}, namespace=model)
