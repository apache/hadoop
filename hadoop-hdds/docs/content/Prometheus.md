---
title: Monitoring with Prometheus
menu:
   main:
      parent: Recipes
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

[Prometheus](https://prometheus.io/) is an open-source monitoring server developed under under the [Cloud Native Foundation](Cloud Native Foundation).

Ozone supports Prometheus out of the box. The servers start a prometheus 
compatible metrics endpoint where all the available hadoop metrics are published in prometheus exporter format.

## Prerequisites

 1. [Install the and start]({{< ref "RunningViaDocker.md" >}}) an Ozone cluster.
 2. [Download](https://prometheus.io/download/#prometheus) the prometheus binary.

## Monitoring with prometheus

(1) To enable the Prometheus metrics endpoint you need to add a new configuration to the `ozone-site.xml` file:

```
  <property>
    <name>hdds.prometheus.endpoint.enabled</name>
    <value>true</value>
  </property>
```

_Note_: for Docker compose based pseudo cluster put the `OZONE-SITE.XML_hdds.prometheus.endpoint.enabled=true` line to the `docker-config` file.

(2) Restart the Ozone Manager and Storage Container Manager and check the prometheus endpoints:

 * http://scm:9874/prom

 * http://ozoneManager:9876/prom

(3) Create a prometheus.yaml configuration with the previous endpoints:

```yaml
global:
  scrape_interval:     15s

scrape_configs:
  - job_name: ozone
    metrics_path: /prom
    static_configs:
     - targets:
        - "scm:9876"
        - "ozoneManager:9874"
```

(4) Start with prometheus from the directory where you have the prometheus.yaml file:

```
prometheus
```

(5) Check the active targets in the prometheus web-ui:

http://localhost:9090/targets

![Prometheus target page example](../../prometheus.png)


(6) Check any metrics on the prometheus web ui. For example:

http://localhost:9090/graph?g0.range_input=1h&g0.expr=om_metrics_num_key_allocate&g0.tab=1

![Prometheus target page example](../../prometheus-key-allocate.png)

## Note

The ozone distribution contains a ready-to-use, dockerized environment to try out ozone and prometheus. It can be found under `compose/ozoneperf` directory.

```bash
cd compose/ozoneperf
docker-compose up -d
```