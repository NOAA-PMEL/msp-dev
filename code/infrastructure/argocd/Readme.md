To install [ArgoCD](https://argo-cd.readthedocs.io/en/stable/getting_started/) 

```
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
```

For k3d, edit the cmd configmap to allow for insecure traffic:
```bash
kubectl edit cm argocd-cmd-params-cm -n argocd
```

add the following lines to the end of the file (by default, uses vi commands):
```
data:
  server.insecure: "true"
```

Next, create IngressRoute using Traefik to allow access to the argocd-server. Create a manifest file simlar
to the following:
```yaml
apiVersion: traefik.io/v1alpha1
kind: IngressRoute
metadata:
  name: argocd-server
  namespace: argocd
spec:
  entryPoints:
     - websecure
  routes:
    - kind: Rule
      match: Host(`mspbase02.pmel.noaa.gov`)
      priority: 10
      services:
        - name: argocd-server
          port: 80
    - kind: Rule
      match: Host(`mspbase02.pmel.noaa.gov`) && Header(`Content-Type`, `application/grpc`)
      priority: 11
      services:
        - name: argocd-server
          port: 80
          scheme: h2c
  tls:
    certResolver: default
```
