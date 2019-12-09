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

# YARN Service Examples

This document describes some example service definitions (`Yarnfile`).

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

## Apache web server - httpd (with registry DNS)

For this example to work, centos/httpd-24-centos7 image must be included in `docker.trusted.registries`.
For server side configuration, please refer to [Running Applications in Docker Containers](../DockerContainers.html) document.

Below is the `Yarnfile` for a service called `httpd-service` with two `httpd` instances.
There is also an httpd proxy instance (httpd-proxy-0) that proxies between the other two httpd instances (httpd-0 and httpd-1).

Note this example requires registry DNS.
```
{
  "name": "httpd-service",
  "version": "1.0",
  "lifetime": "3600",
  "components": [
    {
      "name": "httpd",
      "number_of_containers": 2,
      "artifact": {
        "id": "centos/httpd-24-centos7:latest",
        "type": "DOCKER"
      },
      "launch_command": "/usr/bin/run-httpd",
      "resource": {
        "cpus": 1,
        "memory": "1024"
      },
      "configuration": {
        "files": [
          {
            "type": "TEMPLATE",
            "dest_file": "/var/www/html/index.html",
            "properties": {
              "content": "<html><header><title>Title</title></header><body>Hello from ${COMPONENT_INSTANCE_NAME}!</body></html>"
            }
          }
        ]
      }
    },
    {
      "name": "httpd-proxy",
      "number_of_containers": 1,
      "artifact": {
        "id": "centos/httpd-24-centos7:latest",
        "type": "DOCKER"
      },
      "launch_command": "/usr/bin/run-httpd",
      "resource": {
        "cpus": 1,
        "memory": "1024"
      },
      "configuration": {
        "files": [
          {
            "type": "TEMPLATE",
            "dest_file": "/etc/httpd/conf.d/httpd-proxy.conf",
            "src_file": "httpd-proxy.conf"
          }
        ]
      }
    }
  ],
  "quicklinks": {
    "Apache HTTP Server": "http://httpd-proxy-0.${SERVICE_NAME}.${USER}.${DOMAIN}:8080"
  }
}
```
This `Yarnfile` is already included in the Hadoop distribution, along with the required configuration template `httpd-proxy.conf`.
First upload the configuration template file to HDFS:
```
hdfs dfs -copyFromLocal ${HADOOP_YARN_HOME}/share/hadoop/yarn/yarn-service-examples/httpd/httpd-proxy.conf .
```

The proxy configuration template looks like the following and will configure the httpd-proxy-0 container to balance between the httpd-0 and httpd-1 containers evenly:
```
<Proxy balancer://test>
  BalancerMember http://httpd-0.${SERVICE_NAME}.${USER}.${DOMAIN}:8080
  BalancerMember http://httpd-1.${SERVICE_NAME}.${USER}.${DOMAIN}:8080
  ProxySet lbmethod=bytraffic
</Proxy>

ProxyPass "/"  "balancer://test/"
ProxyPassReverse "/"  "balancer://test/"
```

Then run the service with the command:
```
yarn app -launch <service-name> httpd
```

The last argument is either the path to a JSON specification of the service, or in this case, the name of an example service.
The directory where examples can be found can be configured by setting the YARN\_EXAMPLES\_DIR environment variable.

Once the service is running, navigate to `http://httpd-proxy-0.${SERVICE_NAME}.${USER}.${DOMAIN}:8080` to see the root page.
The pages should alternately show "Hello from httpd-0!" or "Hello from httpd-1!"

The individual httpd URLs can also be visited, `http://httpd-0.${SERVICE_NAME}.${USER}.${DOMAIN}:8080` and `http://httpd-1.${SERVICE_NAME}.${USER}.${DOMAIN}:8080`.

If unsure of your hostnames, visit the RM REST endpoint `http://<RM host>:8088/app/v1/services/httpd-service`.

## Apache web server - httpd (without registry DNS)

A similar IP-based example is provided for environments that do not have registry DNS set up.
The service name for this example is `httpd-service-no-dns`.
There are a couple of additions to the `Yarnfile` for the `httpd-service` described above.
A readiness check is added for the `httpd` component:
```
      "readiness_check": {
        "type": "HTTP",
        "properties": {
          "url": "http://${THIS_HOST}:8080"
        }
      },
```
and `httpd` is added as a dependency for the `httpd-proxy` component:
```
      "dependencies": [ "httpd" ],
```

This means that the httpd-proxy-0 instance will not be started until after an HTTP probe has succeeded for the httpd-0 and httpd-1 containers.
This is necessary so that the IPs of the containers can be used in the configuration of httpd-proxy-0.
The proxy configuration is similar to that of the previous example, with the BalancerMember lines changed as follows:
```
  BalancerMember http://${HTTPD-0_IP}:8080
  BalancerMember http://${HTTPD-1_IP}:8080
```

Note that IP and HOST variables such as `${HTTPD-0_IP}` and `${HTTPD-0_HOST}` should only be used by a component that has a dependency on the named component (`httpd` in this case) AND should only be used when the named component specifies a readiness check.
Here, `httpd-proxy` has a dependency on `httpd` and `httpd` has an HTTP readiness check.
Without the dependency and readiness check, the httpd-proxy-0 container would be started in parallel with the httpd-0 and http-1 containers, and the IPs and hosts would not be assigned yet for httpd-0 and httpd-1.

Other variables can be used by any component.

Before creating the service, upload the proxy configuration to HDFS:
```
hdfs dfs -copyFromLocal ${HADOOP_YARN_HOME}/share/hadoop/yarn/yarn-service-examples/httpd-no-dns/httpd-proxy-no-dns.conf .
```

Then run the service with the command:
```
yarn app -launch <service-name> httpd-no-dns
```
where `service-name` is optional. If omitted, it uses the name defined in the `Yarnfile`.

Look up your IPs at the RM REST endpoint `http://<RM host>:8088/app/v1/services/httpd-service`.
Then visit port 8080 for each IP to view the pages.

## Docker image ENTRYPOINT support

Docker images may have built with ENTRYPOINT to enable start up of docker image without any parameters.
When passing parameters to ENTRYPOINT enabled image, `launch_command` is delimited by comma (,).

```
{
  "name": "sleeper-service",
  "version": "1.0",
  "components" :
  [
    {
      "name": "sleeper",
      "number_of_containers": 2,
      "artifact": {
        "id": "hadoop/centos:latest",
        "type": "DOCKER"
      },
      "launch_command": "sleep,90000",
      "resource": {
        "cpus": 1,
        "memory": "256"
      },
      "restart_policy": "ON_FAILURE",
      "configuration": {
        "env": {
          "YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE":"true"
        },
        "properties": {
          "docker.network": "host"
        }
      }
    }
  ]
}
```
