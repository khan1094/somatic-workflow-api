from kubernetes import client, config

def get_nodes():

    # Local test için
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()

    v1 = client.CoreV1Api()
    nodes = v1.list_node().items

    result = []

    for node in nodes:
        name = node.metadata.name

        conditions = node.status.conditions
        status = "Unknown"

        for condition in conditions:
            if condition.type == "Ready":
                status = condition.status
                break

        result.append({
            "name": name,
            "status": "Ready" if status == "True" else "NotReady"
        })

    return result
