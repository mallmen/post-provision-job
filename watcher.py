import os
import logging
import re
from kubernetes import client, config, watch

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

config.load_incluster_config()
v1 = client.CoreV1Api()
batch_v1 = client.BatchV1Api()

JOB_NAMESPACE = os.getenv("JOB_NAMESPACE", "custom-automation")
AAP_URL = os.getenv("AAP_URL", "https://aap.example.com/api/v2/workflow_job_templates/42/launch/")
PROCESSED_NODES = set()

def sanitize_job_name(node_name):
    clean_name = re.sub(r'[^a-z0-9-]', '-', node_name.lower())
    return f"aap-trigger-{clean_name}"[:63].rstrip('-')

def get_node_ip(node):
    for addr in node.status.addresses:
        if addr.type == "InternalIP":
            return addr.address
    return None

def is_node_tainted(node):
    if not node.spec.taints:
        return False
    for taint in node.spec.taints:
        if taint.key in ["node.kubernetes.io/unschedulable", "unschedulable"]:
            return True
    return False

def create_aap_job(node_name, node_ip):
    job_name = sanitize_job_name(node_name)
    try:
        batch_v1.read_namespaced_job(name=job_name, namespace=JOB_NAMESPACE)
        logging.info(f"Job {job_name} already exists. Skipping.")
        return
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    job_manifest = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name, namespace=JOB_NAMESPACE),
        spec=client.V1JobSpec(
            backoff_limit=3,
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    containers=[
                        client.V1Container(
                            name="aap-caller",
                            image="image-registry.openshift-image-registry.svc:5000/openshift/cli:latest",
                            env=[
                                client.V1EnvVar(
                                    name="AAP_TOKEN",
                                    value_from=client.V1EnvVarSource(
                                        secret_key_ref=client.V1SecretKeySelector(
                                            name="aap-credentials", key="token"
                                        )
                                    )
                                ),
                                client.V1EnvVar(name="TARGET_NODE_NAME", value=node_name),
                                client.V1EnvVar(name="TARGET_NODE_IP", value=node_ip),
                                client.V1EnvVar(name="AAP_URL", value=AAP_URL)
                            ],
                            command=["/bin/bash", "-c"],
                            args=[
                                'curl -k -s -X POST "${AAP_URL}" '
                                '-H "Authorization: Bearer ${AAP_TOKEN}" '
                                '-H "Content-Type: application/json" '
                                '-d "{\\"extra_vars\\": {\\"target_node_name\\": \\"${TARGET_NODE_NAME}\\", \\"target_node_ip\\": \\"${TARGET_NODE_IP}\\"}}"'
                            ]
                        )
                    ]
                )
            )
        )
    )
    batch_v1.create_namespaced_job(namespace=JOB_NAMESPACE, body=job_manifest)
    logging.info(f"Triggered job {job_name} for Node {node_name} ({node_ip})")

def watch_nodes():
    w = watch.Watch()
    for event in w.stream(v1.list_node):
        event_type = event['type']
        node = event['object']
        node_name = node.metadata.name

        if event_type in ["ADDED", "MODIFIED"]:
            if node_name not in PROCESSED_NODES and is_node_tainted(node):
                node_ip = get_node_ip(node)
                if node_ip:
                    try:
                        create_aap_job(node_name, node_ip)
                        PROCESSED_NODES.add(node_name)
                    except Exception as err:
                        logging.error(f"Failed to trigger job for {node_name}: {err}")

if __name__ == "__main__":
    while True:
        try:
            watch_nodes()
        except Exception as e:
            logging.error(f"Watch connection reset ({e}). Reconnecting...")
