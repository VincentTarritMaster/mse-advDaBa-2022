docker buildx build --platform linux/amd64 -t vincenttarrit/neo4jtp:latest --push .
kubectl delete all --all -n tarrit-adv-daba-26
kubectl apply -f kube/neo4j.yaml -n tarrit-adv-daba-26
kubectl apply -f kube/import-data.yaml -n tarrit-adv-daba-26
kubectl get all -n tarrit-adv-daba-26
kubectl logs importer-wq64w -n tarrit-adv-daba-26