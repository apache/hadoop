---
title: Minikube & Ozone
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->


{{< requirements >}}
 * Working minikube setup
 * kubectl
{{< /requirements >}}

`kubernetes/examples` folder of the ozone distribution contains kubernetes deployment resource files for multiple use cases. By default the kubernetes resource files are configured to use `apache/ozone` image from the dockerhub.

To deploy it to minikube use the minikube configuration set:

```
cd kubernetes/examples/minikube
kubectl apply -f .
```

And you can check the results with

```
kubectl get pod
```

Note: the kubernetes/examples/minikube resource set is optimized for minikube usage:

 * You can have multiple datanodes even if you have only one host (in a real production cluster usually you need one datanode per physical host)
 * The services are published with node port

## Access the services

Now you can access any of the services. For each web endpoint an additional NodeType service is defined in the minikube k8s resource set. NodeType services are available via a generated port of any of the host nodes:

```bash
kubectl get svc
NAME         TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)          AGE
datanode     ClusterIP   None            <none>        <none>           27s
kubernetes   ClusterIP   10.96.0.1       <none>        443/TCP          118m
om           ClusterIP   None            <none>        9874/TCP         27s
om-public    NodePort    10.108.48.148   <none>        9874:32649/TCP   27s
s3g          ClusterIP   None            <none>        9878/TCP         27s
s3g-public   NodePort    10.97.133.137   <none>        9878:31880/TCP   27s
scm          ClusterIP   None            <none>        9876/TCP         27s
scm-public   NodePort    10.105.231.28   <none>        9876:32171/TCP   27s
```

Minikube contains a convenience command to access any of the NodePort services:

```
minikube service s3g-public
Opening kubernetes service default/s3g-public in default browser...
```