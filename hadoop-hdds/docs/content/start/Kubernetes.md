---
title: Ozone on Kubernetes
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
 * Working kubernetes cluster (LoadBalancer, PersistentVolume are not required)
 * kubectl
{{< /requirements >}}


As the _apache/ozone_ docker images are available from the dockerhub the deployment process is very similar Minikube deployment. The only big difference is that we have dedicated set of k8s files for hosted clusters (for example we can use one datanode per host)
Deploy to kubernetes

`kubernetes/examples` folder of the ozone distribution contains kubernetes deployment resource files for multiple use cases.

To deploy to a hosted cluster use the ozone subdirectory:

```
cd kubernetes/examples/ozone
kubectl apply -f .
```

And you can check the results with

```
kubectl get pod
Access the services
```

Now you can access any of the services. By default the services are not published but you can access them with port-foward rules.

```
kubectl port-forward s3g-0 9878:9878
kubectl port-forward scm-0 9876:9876
```
