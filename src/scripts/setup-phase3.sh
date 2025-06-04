#!/bin/bash
# Set up Phase 3 components for FraudDetectX

# Create basic directory structure
mkdir -p k8s/{base,overlays/{dev,prod}}
mkdir -p security/{vault,rbac}
mkdir -p performance
mkdir -p ml-training/ab-testing

echo "✅ Created directory structure"

# Initialize Git repository if not already
if [ ! -d ".git" ]; then
    git init
    echo "✅ Initialized Git repository"
else
    echo "✅ Git repository already initialized"
fi

# Create GitHub Actions workflow directory
mkdir -p .github/workflows
echo "✅ Created GitHub Actions workflow directory"

# Run Docker Compose to ensure everything is running
docker-compose ps
if [ $? -ne 0 ]; then
    echo "Starting Docker Compose services..."
    docker-compose up -d
    echo "✅ Started Docker Compose services"
else
    echo "✅ Docker Compose services are running"
fi

# Check if Grafana dashboard is available
curl -s http://localhost:3000 > /dev/null
if [ $? -eq 0 ]; then
    echo "✅ Grafana is accessible at http://localhost:3000"
else
    echo "❌ Grafana is not accessible. Check your Docker Compose setup."
fi

echo "
Phase 3 setup is complete! Next steps:

1. Create Kubernetes manifests:
   - Convert Docker Compose to K8s with scripts/compose-to-k8s.py
   - Customize manifests in k8s/base directory

2. Set up GitHub repository:
   - Push to GitHub
   - Configure GitHub Actions secrets

3. Install ArgoCD in your Kubernetes cluster:
   - Follow the ArgoCD installation guide
   - Connect ArgoCD to your Git repository

4. Start performance testing:
   - Run performance/load_test.py to benchmark your system
   - Use the results to optimize your configuration

5. Set up A/B testing:
   - Train multiple model versions
   - Use ml-training/ab-testing/run_comparison.py to compare

For detailed instructions, see the Phase 3 documentation.
"
EOF

# Make script executable
chmod +x scripts/setup-phase3.sh