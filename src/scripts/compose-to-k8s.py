#!/usr/bin/env python3
"""
Convert docker-compose.yml to Kubernetes manifests
"""
import yaml
import os
import sys

def main():
    """Convert docker-compose.yml to Kubernetes manifests"""
    # Read docker-compose.yml
    with open('docker-compose.yml', 'r') as f:
        compose = yaml.safe_load(f)

    # Create output directory
    os.makedirs('k8s/base', exist_ok=True)

    # Create namespace
    namespace = {
        'apiVersion': 'v1',
        'kind': 'Namespace',
        'metadata': {
            'name': 'frauddetectx'
        }
    }

    with open('k8s/base/namespace.yaml', 'w') as f:
        yaml.dump(namespace, f)

    # Process each service
    for service_name, service in compose.get('services', {}).items():
        convert_service(service_name, service)

def convert_service(name, service):
    """Convert a single service to Kubernetes manifests"""
    # Determine the right kind of manifest
    if 'image' in service:
        deployment = create_deployment(name, service)
        service_manifest = create_service(name, service)

        # Write manifests
        with open(f'k8s/base/{name}.yaml', 'w') as f:
            yaml.dump_all([deployment, service_manifest], f)
            print(f"Created manifest for {name}")

def create_deployment(name, service):
    """Create a Kubernetes Deployment from a docker-compose service"""
    # Basic deployment template
    deployment = {
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'metadata': {
            'name': name,
            'namespace': 'frauddetectx',
            'labels': {
                'app.kubernetes.io/name': name,
                'app.kubernetes.io/part-of': 'frauddetectx'
            }
        },
        'spec': {
            'replicas': 1,
            'selector': {
                'matchLabels': {
                    'app.kubernetes.io/name': name
                }
            },
            'template': {
                'metadata': {
                    'labels': {
                        'app.kubernetes.io/name': name,
                        'app.kubernetes.io/part-of': 'frauddetectx'
                    }
                },
                'spec': {
                    'containers': [{
                        'name': name,
                        'image': service.get('image', f'frauddetectx/{name}:latest'),
                        'ports': [],
                        'env': []
                    }]
                }
            }
        }
    }

    # Add ports
    if 'ports' in service:
        for port_mapping in service['ports']:
            parts = port_mapping.split(':')
            if len(parts) == 2:
                container_port = int(parts[1])
                deployment['spec']['template']['spec']['containers'][0]['ports'].append({
                    'containerPort': container_port
                })

    # Add environment variables
    if 'environment' in service:
        for env_var in service['environment']:
            if isinstance(env_var, str) and '=' in env_var:
                key, value = env_var.split('=', 1)
                deployment['spec']['template']['spec']['containers'][0]['env'].append({
                    'name': key,
                    'value': value
                })
            elif isinstance(env_var, dict):
                for key, value in env_var.items():
                    deployment['spec']['template']['spec']['containers'][0]['env'].append({
                        'name': key,
                        'value': value
                    })

    return deployment

def create_service(name, service):
    """Create a Kubernetes Service from a docker-compose service"""
    # Basic service template
    k8s_service = {
        'apiVersion': 'v1',
        'kind': 'Service',
        'metadata': {
            'name': name,
            'namespace': 'frauddetectx',
            'labels': {
                'app.kubernetes.io/name': name,
                'app.kubernetes.io/part-of': 'frauddetectx'
            }
        },
        'spec': {
            'selector': {
                'app.kubernetes.io/name': name
            },
            'ports': []
        }
    }

    # Add ports
    if 'ports' in service:
        for port_mapping in service['ports']:
            parts = port_mapping.split(':')
            if len(parts) == 2:
                host_port = int(parts[0])
                container_port = int(parts[1])
                k8s_service['spec']['ports'].append({
                    'port': container_port,
                    'targetPort': container_port,
                    'name': f'port-{container_port}'
                })

    return k8s_service

if __name__ == "__main__":
    main()