<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Ozone Distribution

This folder contains the project to create the binary ozone distribution and provide all the helper script and docker files to start it locally or in the cluster.

## Testing with local docker based cluster

After a full dist build you can find multiple docker-compose based cluster definition in the `target/ozone-*/compose` folder.

Please check the README files there.

Usually you can start the cluster with:

```
cd compose/ozone
docker-compose up -d
```

## Testing on Kubernetes

You can also test the ozone cluster in kubernetes. If you have no active kubernetes cluster you can start a local one with minikube:

```
minikube start
```

For testing in kubernetes you need to:

1. Create a docker image with the new build
2. Upload it to a docker registery
3. Deploy the cluster with apply kubernetes resources

The easiest way to do all these steps is using the [skaffold](https://github.com/GoogleContainerTools/skaffold) tool. After the [installation of skaffold](https://github.com/GoogleContainerTools/skaffold#installation), you can execute

```
skaffold run
```

in this  (`hadoop-ozone/dist`) folder.

The default kubernetes resources set (`src/main/k8s/`) contains NodePort based service definitions for the Ozone Manager, Storage Container Manager and the S3 gateway.

With minikube you can access the services with:

```
minikube service s3g-public
minikube service om-public
minikube service scm-public
```

### Monitoring

Apache Hadoop Ozone supports Prometheus out-of the box. It contains a prometheus compatible exporter servlet. To start the monitoring you need a prometheus deploy in your kubernetes  cluster:

```
cd src/main/k8s/prometheus
kubectl apply -f .
```

The prometheus ui also could be access via a NodePort service:

```
minikube service prometheus-public
```

### Notes on the Kubernetes setup

Please not that the provided kubernetes resources are not suitable production:

1. There are no security setup
2. The datanode is started in StatefulSet instead of DaemonSet (To make it possible to scale it up on one node minikube cluster)
3. All the UI pages are published with NodePort services