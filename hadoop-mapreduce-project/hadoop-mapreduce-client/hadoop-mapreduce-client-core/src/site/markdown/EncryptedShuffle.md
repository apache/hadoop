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

Hadoop: Encrypted Shuffle
=========================

Introduction
------------

The Encrypted Shuffle capability allows encryption of the MapReduce shuffle using HTTPS and with optional client authentication (also known as bi-directional HTTPS, or HTTPS with client certificates). It comprises:

*   A Hadoop configuration setting for toggling the shuffle between HTTP and
    HTTPS.

*   A Hadoop configuration settings for specifying the keystore and truststore
    properties (location, type, passwords) used by the shuffle service and the
    reducers tasks fetching shuffle data.

*   A way to re-load truststores across the cluster (when a node is added or
    removed).

Configuration
-------------

### **core-site.xml** Properties

To enable encrypted shuffle, set the following properties in core-site.xml of all nodes in the cluster:

| **Property** | **Default Value** | **Explanation** |
|:---- |:---- |:---- |
| `hadoop.ssl.require.client.cert` | `false` | Whether client certificates are required |
| `hadoop.ssl.hostname.verifier` | `DEFAULT` | The hostname verifier to provide for HttpsURLConnections. Valid values are: **DEFAULT**, **STRICT**, **STRICT\_I6**, **DEFAULT\_AND\_LOCALHOST** and **ALLOW\_ALL** |
| `hadoop.ssl.keystores.factory.class` | `org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory` | The KeyStoresFactory implementation to use |
| `hadoop.ssl.server.conf` | `ssl-server.xml` | Resource file from which ssl server keystore information will be extracted. This file is looked up in the classpath, typically it should be in Hadoop conf/ directory |
| `hadoop.ssl.client.conf` | `ssl-client.xml` | Resource file from which ssl server keystore information will be extracted. This file is looked up in the classpath, typically it should be in Hadoop conf/ directory |
| `hadoop.ssl.enabled.protocols` | `TLSv1` | The supported SSL protocols (JDK6 can use **TLSv1**, JDK7+ can use **TLSv1,TLSv1.1,TLSv1.2**) |

**IMPORTANT:** Currently requiring client certificates should be set to false. Refer the [Client Certificates](#Client_Certificates) section for details.

**IMPORTANT:** All these properties should be marked as final in the cluster configuration files.

#### Example:

```xml
  <property>
    <name>hadoop.ssl.require.client.cert</name>
    <value>false</value>
    <final>true</final>
  </property>

  <property>
    <name>hadoop.ssl.hostname.verifier</name>
    <value>DEFAULT</value>
    <final>true</final>
  </property>

  <property>
    <name>hadoop.ssl.keystores.factory.class</name>
    <value>org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory</value>
    <final>true</final>
  </property>

  <property>
    <name>hadoop.ssl.server.conf</name>
    <value>ssl-server.xml</value>
    <final>true</final>
  </property>

  <property>
    <name>hadoop.ssl.client.conf</name>
    <value>ssl-client.xml</value>
    <final>true</final>
  </property>
```

### `mapred-site.xml` Properties

To enable encrypted shuffle, set the following property in mapred-site.xml of all nodes in the cluster:

| **Property** | **Default Value** | **Explanation** |
|:---- |:---- |:---- |
| `mapreduce.shuffle.ssl.enabled` | `false` | Whether encrypted shuffle is enabled |

**IMPORTANT:** This property should be marked as final in the cluster configuration files.

#### Example:

```xml
  <property>
    <name>mapreduce.shuffle.ssl.enabled</name>
    <value>true</value>
    <final>true</final>
  </property>
```

The Linux container executor should be set to prevent job tasks from reading the server keystore information and gaining access to the shuffle server certificates.

Refer to Hadoop Kerberos configuration for details on how to do this.

Keystore and Truststore Settings
--------------------------------

Currently `FileBasedKeyStoresFactory` is the only `KeyStoresFactory` implementation. The `FileBasedKeyStoresFactory` implementation uses the following properties, in the **ssl-server.xml** and **ssl-client.xml** files, to configure the keystores and truststores.

### `ssl-server.xml` (Shuffle server) Configuration:

The mapred user should own the **ssl-server.xml** file and have exclusive read access to it.

| **Property** | **Default Value** | **Explanation** |
|:---- |:---- |:---- |
| `ssl.server.keystore.type` | `jks` | Keystore file type |
| `ssl.server.keystore.location` | NONE | Keystore file location. The mapred user should own this file and have exclusive read access to it. |
| `ssl.server.keystore.password` | NONE | Keystore file password |
| `ssl.server.truststore.type` | `jks` | Truststore file type |
| `ssl.server.truststore.location` | NONE | Truststore file location. The mapred user should own this file and have exclusive read access to it. |
| `ssl.server.truststore.password` | NONE | Truststore file password |
| `ssl.server.truststore.reload.interval` | 10000 | Truststore reload interval, in milliseconds |

#### Example:

```xml
<configuration>

  <!-- Server Certificate Store -->
  <property>
    <name>ssl.server.keystore.type</name>
    <value>jks</value>
  </property>
  <property>
    <name>ssl.server.keystore.location</name>
    <value>${user.home}/keystores/server-keystore.jks</value>
  </property>
  <property>
    <name>ssl.server.keystore.password</name>
    <value>serverfoo</value>
  </property>

  <!-- Server Trust Store -->
  <property>
    <name>ssl.server.truststore.type</name>
    <value>jks</value>
  </property>
  <property>
    <name>ssl.server.truststore.location</name>
    <value>${user.home}/keystores/truststore.jks</value>
  </property>
  <property>
    <name>ssl.server.truststore.password</name>
    <value>clientserverbar</value>
  </property>
  <property>
    <name>ssl.server.truststore.reload.interval</name>
    <value>10000</value>
  </property>
</configuration>
```

### `ssl-client.xml` (Reducer/Fetcher) Configuration:

The mapred user should own the **ssl-client.xml** file and it should have default permissions.

| **Property** | **Default Value** | **Explanation** |
|:---- |:---- |:---- |
| `ssl.client.keystore.type` | `jks` | Keystore file type |
| `ssl.client.keystore.location` | NONE | Keystore file location. The mapred user should own this file and it should have default permissions. |
| `ssl.client.keystore.password` | NONE | Keystore file password |
| `ssl.client.truststore.type` | `jks` | Truststore file type |
| `ssl.client.truststore.location` | NONE | Truststore file location. The mapred user should own this file and it should have default permissions. |
| `ssl.client.truststore.password` | NONE | Truststore file password |
| `ssl.client.truststore.reload.interval` | 10000 | Truststore reload interval, in milliseconds |

#### Example:

```xml
<configuration>

  <!-- Client certificate Store -->
  <property>
    <name>ssl.client.keystore.type</name>
    <value>jks</value>
  </property>
  <property>
    <name>ssl.client.keystore.location</name>
    <value>${user.home}/keystores/client-keystore.jks</value>
  </property>
  <property>
    <name>ssl.client.keystore.password</name>
    <value>clientfoo</value>
  </property>

  <!-- Client Trust Store -->
  <property>
    <name>ssl.client.truststore.type</name>
    <value>jks</value>
  </property>
  <property>
    <name>ssl.client.truststore.location</name>
    <value>${user.home}/keystores/truststore.jks</value>
  </property>
  <property>
    <name>ssl.client.truststore.password</name>
    <value>clientserverbar</value>
  </property>
  <property>
    <name>ssl.client.truststore.reload.interval</name>
    <value>10000</value>
  </property>
</configuration>
```

Activating Encrypted Shuffle
----------------------------

When you have made the above configuration changes, activate Encrypted Shuffle by re-starting all NodeManagers.

**IMPORTANT:** Using encrypted shuffle will incur in a significant performance impact. Users should profile this and potentially reserve 1 or more cores for encrypted shuffle.

Client Certificates
-------------------

Using Client Certificates does not fully ensure that the client is a reducer task for the job. Currently, Client Certificates (their private key) keystore files must be readable by all users submitting jobs to the cluster. This means that a rogue job could read such those keystore files and use the client certificates in them to establish a secure connection with a Shuffle server. However, unless the rogue job has a proper JobToken, it won't be able to retrieve shuffle data from the Shuffle server. A job, using its own JobToken, can only retrieve shuffle data that belongs to itself.

Reloading Truststores
---------------------

By default the truststores will reload their configuration every 10 seconds. If a new truststore file is copied over the old one, it will be re-read, and its certificates will replace the old ones. This mechanism is useful for adding or removing nodes from the cluster, or for adding or removing trusted clients. In these cases, the client or NodeManager certificate is added to (or removed from) all the truststore files in the system, and the new configuration will be picked up without you having to restart the NodeManager daemons.

Debugging
---------

**NOTE:** Enable debugging only for troubleshooting, and then only for jobs running on small amounts of data. It is very verbose and slows down jobs by several orders of magnitude. (You might need to increase mapred.task.timeout to prevent jobs from failing because tasks run so slowly.)

To enable SSL debugging in the reducers, set `-Djavax.net.debug=all` in the `mapreduce.reduce.child.java.opts` property; for example:

      <property>
        <name>mapred.reduce.child.java.opts</name>
        <value>-Xmx-200m -Djavax.net.debug=all</value>
      </property>

You can do this on a per-job basis, or by means of a cluster-wide setting in the `mapred-site.xml` file.

To set this property in NodeManager, set it in the `yarn-env.sh` file:

      YARN_NODEMANAGER_OPTS="-Djavax.net.debug=all"

Encrypted Intermediate Data Spill files
---------------------------------------

This capability allows encryption of the intermediate files generated during the merge and shuffle phases.
It can be enabled by setting the `mapreduce.job.encrypted-intermediate-data` job property to `true`.

**NOTE:** Currently, enabling encrypted intermediate data spills would restrict the number of attempts of the job to 1.