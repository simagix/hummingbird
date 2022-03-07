# Kubernetes Development Environment

## Deploy Neutrino
```bash
kubectl apply -f neutrino.yaml
```

## Deploy Service
```bash
kubectl apply -f service.yaml
```

## Deploy Worker (Optional)
```bash
kubectl apply -f worker.yaml
```

## Deploy Simulator (Optional)
```bash
kubectl apply -f simulator.yaml
```

## Monitoring
```bash
kubectl get pods,svc
```

View progress at http://localhost:30629.

## Delete k8s
```bash
kubectl delete -f service.yaml

kubectl delete -f simulator.yaml
kubectl delete -f worker.yaml
kubectl delete -f neutrino.yaml
```