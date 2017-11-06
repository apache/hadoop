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

# Registry Configuration


The YARN service registry is built on top of Apache Zookeeper.
It is configured by way of a Hadoop `Configuration` class:
the instance used to create the service controls the behavior of the client.

This document lists the configuration parameters which control the
registry client and its deployment in the YARN Resource Manager.

The default values of all these settings are defined in `core-default.xml`.
The values in this file may not match those listed in this document.
If this is the case, the values in `core-default.xml` MUST be considered normative.

## Changing the configuration values

Changes to the configuration values SHOULD be done in `core-site.xml`.
This will ensure that client and non-YARN applications will pick up
the values, so enabling them to read from and potentially write to the registry.


## Core Settings


### Enabling the Registry in the Resource Manager

The Resource Manager manages user directory creation and record cleanup
on YARN container/application attempt/application completion.

```
  <property>
    <description>
      Is the registry enabled in the YARN Resource Manager?

      If true, the YARN RM will, as needed.
      create the user and system paths, and purge
      service records when containers, application attempts
      and applications complete.

      If false, the paths must be created by other means,
      and no automatic cleanup of service records will take place.
    </description>
    <name>hadoop.registry.rm.enabled</name>
    <value>false</value>
  </property>
```

If the property is set in `core-site.xml` or `yarn-site.xml`,
the YARN Resource Manager will behave as follows:
1. On startup: create the initial root paths of `/`, `/services` and `/users`.
  On a secure cluster, access will be restricted to the system accounts (see below).
2. When a user submits a job: create the user path under `/users`.
3. When a container is completed: delete from the registry all service records
   with a `yarn:persistence` field of value `container`, and a `yarn:id` field
   whose value matches the ID of the completed container.
4. When an application attempt is completed: remove all service records with
   `yarn:persistence` set to `application-attempt` and `yarn:id` set to the
   pplication attempt ID.
5. When an application finishes: remove all service records with
   `yarn:persistence` set to `application` and `yarn:id` set to the
   application ID.

All these operations are asynchronous, so that zookeeper connectivity problems
do not delay RM operations or work scheduling.

If the property `hadoop.registry.rm.enabled` is set to `false`, the RM will
not interact with the registry —and the listed operations will not take place.
The root paths may be created by other means, but service record cleanup will not take place.

### Setting the Zookeeper Quorum: `hadoop.registry.zk.quorum`

This is an essential setting: it identifies the lists of zookeeper hosts
and the ports on which the ZK services are listening.


```
  <property>
    <description>
      A comma separated list of hostname:port pairs defining the
      zookeeper quorum binding for the registry
    </description>
    <name>hadoop.registry.zk.quorum</name>
    <value>localhost:2181</value>
  </property>
```

It takes a comma-separated list, such as `zk1:2181 ,zk2:2181, zk3:2181`

### Setting the Zookeeper Registry Base path: `hadoop.registry.zk.root`

This path sets the base zookeeper node for the registry

```
  <property>
    <description>
      The root zookeeper node for the registry
    </description>
    <name>hadoop.registry.zk.root</name>
    <value>/registry</value>
  </property>
```

The default value of `/registry` is normally sufficient. A different value
may be needed for security reasons or because the `/registry` path is in use.

The root value is prepended to all registry paths so as to create the absolute
path. For example:

* `/` maps to `/registry`
* `/services` maps to `/registry/services`
* `/users/yarn` maps to `/registry/users/yarn`

A different value of `hadoop.registry.zk.root` would result in a different
mapping to absolute zookeeper paths.


## Security Options

Registry security is enabled when the property `hadoop.registry.secure`
is set to `true`. Once set, nodes are created with permissions, so that
only a specific user *and the configured cluster "superuser" accounts*
can write under their home path of `${hadoop.registry.zk.root}/users`.
Only the superuser accounts
will be able to manipulate the root path, including `${hadoop.registry.zk.root}/services`
and `${hadoop.registry.zk.root}/users`.

All write operations on the registry (including deleting entries and paths)
must be authenticated. Read operations are still permitted by unauthenticated
callers.

The key settings for secure registry support are:

* enabling the secure mode:  `hadoop.registry.secure`
* listing the superuser zookeeper ACLs:  `hadoop.registry.system.acls`
* listing the kerberos realm for the principals: `hadoop.registry.kerberos.realm`
* identifying the JAAS context within the JAAS configuration which defines
the user: `hadoop.registry.jaas.context`


### Enabling security

```
  <property>
    <description>
      Key to set if the registry is secure. Turning it on
      changes the permissions policy from "open access"
      to restrictions on kerberos with the option of
      a user adding one or more auth key pairs down their
      own tree.
    </description>
    <name>hadoop.registry.secure</name>
    <value>false</value>
  </property>
```

### Identifying the client JAAS context

The registry clients must identify the JAAS context which they use
to authenticate to the registry.

```
  <property>
    <description>
      Key to define the JAAS context. Used in secure mode
    </description>
    <name>hadoop.registry.jaas.context</name>
    <value>Client</value>
  </property>
```

*Note* as the Resource Manager is simply another client of the registry, it
too must have this context defined.


### Identifying the system accounts `hadoop.registry.system.acls`

These are the the accounts which are given full access to the base of the
registry. The Resource Manager needs this option to create the root paths.

Client applications writing to the registry access to the nodes it creates.

1. The property `hadoop.registry.system.acls` takes a comma-separated list
of zookeeper `ACLs` which are given full access to created nodes; the permissions
`READ | WRITE | CREATE | DELETE | ADMIN`.
2. Any zookeeper ACL scheme may be added to this, such as the `digest:` scheme.
3. The SASL scheme, `sasl:`, is used to identify which callers identified
by sasl have full access. These are the superuser accounts.
4. They may be identified by elements such as `sasl:yarn@REALM.COM`.
5. To aid portability of SASL settings, especially the default value,
any `sasl:` entry without the realm value —that
is, any entry that terminates in the `@` symbol— has the current realm appended
to it.
6. This realm is set to that of the current user.
7. It may be overridden by the property `hadoop.registry.kerberos.realm`.


```
  <property>
    <description>
      A comma separated list of Zookeeper ACL identifiers with
      system access to the registry in a secure cluster.
      These are given full access to all entries.
      If there is an "@" at the end of a SASL entry it
      instructs the registry client to append the default kerberos domain.
    </description>
    <name>hadoop.registry.system.acls</name>
    <value>sasl:yarn@, sasl:mapred@, sasl:mapred@, sasl:hdfs@</value>
  </property>

  <property>
    <description>
      The kerberos realm: used to set the realm of
      system principals which do not declare their realm,
      and any other accounts that need the value.
      If empty, the default realm of the running process
      is used.
      If neither are known and the realm is needed, then the registry
      service/client will fail.
    </description>
    <name>hadoop.registry.kerberos.realm</name>
    <value></value>
  </property>
```

Example: an `hadoop.registry.system.acls` entry of
 `sasl:yarn@, sasl:admin@EXAMPLE.COM, sasl:system@REALM2`,
would, in a YARN cluster with the realm `EXAMPLE.COM`, add the following
admin accounts to every node

* `sasl:yarn@EXAMPLE.COM`
* `sasl:admin@EXAMPLE.COM`
* `sasl:system@REALM2`

The identity of a client application creating registry entries will be
automatically included in the permissions of all entries created.
If, for example, the account creating an entry was `hbase`, another
entry would be created

* `sasl:hbase@EXAMPLE.COM`


**Important**: when setting the system ACLS, *it is critical to include the
identity of the YARN Resource Manager*.

The RM needs to be able to create the root and user paths, and delete service
records during application and container cleanup.


## Zookeeper connection management options

Some low level options manage the ZK connection —more specifically, its failure
handling.

The Zookeeper registry clients use Apache Curator to connect to Zookeeper,
a library which detects timeouts and attempts to reconnect to one of the
servers which forms the zookeeper quorum. It is only after a timeout is detected
that a retry is triggered.

```
  <property>
    <description>
      Zookeeper session timeout in milliseconds
    </description>
    <name>hadoop.registry.zk.session.timeout.ms</name>
    <value>60000</value>
  </property>

  <property>
    <description>
      Zookeeper connection timeout in milliseconds
    </description>
    <name>hadoop.registry.zk.connection.timeout.ms</name>
    <value>15000</value>
  </property>

  <property>
    <description>
      Zookeeper connection retry count before failing
    </description>
    <name>hadoop.registry.zk.retry.times</name>
    <value>5</value>
  </property>

  <property>
    <description>
    </description>
    <name>hadoop.registry.zk.retry.interval.ms</name>
    <value>1000</value>
  </property>

  <property>
    <description>
      Zookeeper retry limit in milliseconds, during
      exponential backoff.
      This places a limit even
      if the retry times and interval limit, combined
      with the backoff policy, result in a long retry
      period
    </description>
    <name>hadoop.registry.zk.retry.ceiling.ms</name>
    <value>60000</value>
  </property>
```

The retry strategy used in the registry client is
[`BoundedExponentialBackoffRetry`](https://curator.apache.org/apidocs/org/apache/curator/retry/BoundedExponentialBackoffRetry.html):
This backs off exponentially on connection failures before eventually
concluding that the quorum is unreachable and failing.

## Complete Set of Configuration Options

```
  <!-- YARN registry -->

  <property>
    <description>
      Is the registry enabled: does the RM start it up,
      create the user and system paths, and purge
      service records when containers, application attempts
      and applications complete
    </description>
    <name>hadoop.registry.rm.enabled</name>
    <value>false</value>
  </property>

  <property>
    <description>
      A comma separated list of hostname:port pairs defining the
      zookeeper quorum binding for the registry
    </description>
    <name>hadoop.registry.zk.quorum</name>
    <value>localhost:2181</value>
  </property>

  <property>
    <description>
      The root zookeeper node for the registry
    </description>
    <name>hadoop.registry.zk.root</name>
    <value>/registry</value>
  </property>

  <property>
    <description>
      Key to set if the registry is secure. Turning it on
      changes the permissions policy from "open access"
      to restrictions on kerberos with the option of
      a user adding one or more auth key pairs down their
      own tree.
    </description>
    <name>hadoop.registry.secure</name>
    <value>false</value>
  </property>

  <property>
    <description>
      A comma separated list of Zookeeper ACL identifiers with
      system access to the registry in a secure cluster.

      These are given full access to all entries.

      If there is an "@" at the end of a SASL entry it
      instructs the registry client to append the default kerberos domain.
    </description>
    <name>hadoop.registry.system.acls</name>
    <value>sasl:yarn@, sasl:mapred@, sasl:mapred@, sasl:hdfs@</value>
  </property>

  <property>
    <description>
      The kerberos realm: used to set the realm of
      system principals which do not declare their realm,
      and any other accounts that need the value.

      If empty, the default realm of the running process
      is used.

      If neither are known and the realm is needed, then the registry
      service/client will fail.
    </description>
    <name>hadoop.registry.kerberos.realm</name>
    <value></value>
  </property>

  <property>
    <description>
      Key to define the JAAS context. Used in secure
      mode
    </description>
    <name>hadoop.registry.jaas.context</name>
    <value>Client</value>
  </property>


  <property>
    <description>
      Zookeeper session timeout in milliseconds
    </description>
    <name>hadoop.registry.zk.session.timeout.ms</name>
    <value>60000</value>
  </property>

  <property>
    <description>
      Zookeeper session timeout in milliseconds
    </description>
    <name>hadoop.registry.zk.connection.timeout.ms</name>
    <value>15000</value>
  </property>

  <property>
    <description>
      Zookeeper connection retry count before failing
    </description>
    <name>hadoop.registry.zk.retry.times</name>
    <value>5</value>
  </property>

  <property>
    <description>
    </description>
    <name>hadoop.registry.zk.retry.interval.ms</name>
    <value>1000</value>
  </property>

  <property>
    <description>
      Zookeeper retry limit in milliseconds, during
      exponential backoff: {@value}

      This places a limit even
      if the retry times and interval limit, combined
      with the backoff policy, result in a long retry
      period
    </description>
    <name>hadoop.registry.zk.retry.ceiling.ms</name>
    <value>60000</value>
  </property>
```
