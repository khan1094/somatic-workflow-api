import requests
import os

ARGO_HOST = os.getenv(
    "ARGO_HOST",
    "https://argo-server.argo.svc.cluster.local:2746"
)
NAMESPACE = "argo"


def submit_workflow(sample_vcf: str, node: str = None):

    url = f"{ARGO_HOST}/api/v1/workflows/{NAMESPACE}"

    workflow_spec = {
        "workflowTemplateRef": {
            "name": "somatic-classifier-template"
        },
        "arguments": {
            "parameters": [
                {"name": "sample-vcf", "value": sample_vcf}
            ]
        }
    }

    if node:
        workflow_spec["nodeSelector"] = {
            "kubernetes.io/hostname": node
        }

    workflow_manifest = {
        "workflow": {
            "metadata": {
                "generateName": "somatic-api-"
            },
            "spec": workflow_spec
        }
    }

    response = requests.post(
        url,
        json=workflow_manifest,
        verify=False
    )

    response.raise_for_status()
    return response.json()


def get_workflow(name: str):
    url = f"{ARGO_HOST}/api/v1/workflows/{NAMESPACE}/{name}"
    response = requests.get(url, verify=False)
    response.raise_for_status()
    return response.json()


def list_workflows():
    url = f"{ARGO_HOST}/api/v1/workflows/{NAMESPACE}"
    response = requests.get(url, verify=False)
    response.raise_for_status()
    return response.json()


def delete_workflow(name: str):
    url = f"{ARGO_HOST}/api/v1/workflows/{NAMESPACE}/{name}"
    response = requests.delete(url, verify=False)
    response.raise_for_status()
    return response.json()
