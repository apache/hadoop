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

Hadoop in Secure Mode
=====================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Introduction
------------

This document describes how to configure authentication for Hadoop in secure mode. When Hadoop is configured to run in secure mode, each Hadoop service and each user must be authenticated by Kerberos.

Forward and reverse host lookup for all service hosts must be configured correctly to allow services to authenticate with each other. Host lookups may be configured using either DNS or `/etc/hosts` files. Working knowledge of Kerberos and DNS is recommended before attempting to configure Hadoop services in Secure Mode.

Security features of Hadoop consist of [Authentication](#Authentication), [Service Level Authorization](./ServiceLevelAuth.html), [Authentication for Web Consoles](./HttpAuthentication.html) and [Data Confidentiality](#Data_confidentiality).

Authentication
--------------

### End User Accounts

When service level authentication is turned on, end users must authenticate themselves before interacting with Hadoop services. The simplest way is for a user to authenticate interactively using the [Kerberos `kinit` command](http://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/kinit.html "MIT Kerberos Documentation of kinit"). Programmatic authentication using Kerberos keytab files may be used when interactive login with `kinit` is infeasible.

### User Accounts for Hadoop Daemons

Ensure that HDFS and YARN daemons run as different Unix users, e.g. `hdfs` and `yarn`. Also, ensure that the MapReduce JobHistory server runs as different user such as `mapred`.

It's recommended to have them share a Unix group, for e.g. `hadoop`. See also "[Mapping from user to group](#Mapping_from_user_to_group)" for group management.

| User:Group    | Daemons                                             |
|:--------------|:----------------------------------------------------|
| hdfs:hadoop   | NameNode, Secondary NameNode, JournalNode, DataNode |
| yarn:hadoop   | ResourceManager, NodeManager                        |
| mapred:hadoop | MapReduce JobHistory Server                         |

### Kerberos principals for Hadoop Daemons

Each Hadoop Service instance must be configured with its Kerberos principal and keytab file location.

The general format of a Service principal is `ServiceName/_HOST@REALM.TLD`. e.g. `dn/_HOST@EXAMPLE.COM`.

Hadoop simplifies the deployment of configuration files by allowing the hostname component of the service principal to be specified as the `_HOST` wildcard. Each service instance will substitute `_HOST` with its own fully qualified hostname at runtime. This allows administrators to deploy the same set of configuration files on all nodes. However, the keytab files will be different.

#### HDFS

The NameNode keytab file, on each NameNode host, should look like the following:

    $ klist -e -k -t /etc/security/keytab/nn.service.keytab
    Keytab name: FILE:/etc/security/keytab/nn.service.keytab
    KVNO Timestamp         Principal
       4 07/18/11 21:08:09 nn/full.qualified.domain.name@REALM.TLD (AES-256 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 nn/full.qualified.domain.name@REALM.TLD (AES-128 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 nn/full.qualified.domain.name@REALM.TLD (ArcFour with HMAC/md5)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (AES-256 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (AES-128 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (ArcFour with HMAC/md5)

The Secondary NameNode keytab file, on that host, should look like the following:

    $ klist -e -k -t /etc/security/keytab/sn.service.keytab
    Keytab name: FILE:/etc/security/keytab/sn.service.keytab
    KVNO Timestamp         Principal
       4 07/18/11 21:08:09 sn/full.qualified.domain.name@REALM.TLD (AES-256 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 sn/full.qualified.domain.name@REALM.TLD (AES-128 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 sn/full.qualified.domain.name@REALM.TLD (ArcFour with HMAC/md5)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (AES-256 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (AES-128 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (ArcFour with HMAC/md5)

The DataNode keytab file, on each host, should look like the following:

    $ klist -e -k -t /etc/security/keytab/dn.service.keytab
    Keytab name: FILE:/etc/security/keytab/dn.service.keytab
    KVNO Timestamp         Principal
       4 07/18/11 21:08:09 dn/full.qualified.domain.name@REALM.TLD (AES-256 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 dn/full.qualified.domain.name@REALM.TLD (AES-128 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 dn/full.qualified.domain.name@REALM.TLD (ArcFour with HMAC/md5)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (AES-256 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (AES-128 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (ArcFour with HMAC/md5)

#### YARN

The ResourceManager keytab file, on the ResourceManager host, should look like the following:

    $ klist -e -k -t /etc/security/keytab/rm.service.keytab
    Keytab name: FILE:/etc/security/keytab/rm.service.keytab
    KVNO Timestamp         Principal
       4 07/18/11 21:08:09 rm/full.qualified.domain.name@REALM.TLD (AES-256 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 rm/full.qualified.domain.name@REALM.TLD (AES-128 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 rm/full.qualified.domain.name@REALM.TLD (ArcFour with HMAC/md5)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (AES-256 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (AES-128 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (ArcFour with HMAC/md5)

The NodeManager keytab file, on each host, should look like the following:

    $ klist -e -k -t /etc/security/keytab/nm.service.keytab
    Keytab name: FILE:/etc/security/keytab/nm.service.keytab
    KVNO Timestamp         Principal
       4 07/18/11 21:08:09 nm/full.qualified.domain.name@REALM.TLD (AES-256 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 nm/full.qualified.domain.name@REALM.TLD (AES-128 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 nm/full.qualified.domain.name@REALM.TLD (ArcFour with HMAC/md5)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (AES-256 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (AES-128 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (ArcFour with HMAC/md5)

#### MapReduce JobHistory Server

The MapReduce JobHistory Server keytab file, on that host, should look like the following:

    $ klist -e -k -t /etc/security/keytab/jhs.service.keytab
    Keytab name: FILE:/etc/security/keytab/jhs.service.keytab
    KVNO Timestamp         Principal
       4 07/18/11 21:08:09 jhs/full.qualified.domain.name@REALM.TLD (AES-256 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 jhs/full.qualified.domain.name@REALM.TLD (AES-128 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 jhs/full.qualified.domain.name@REALM.TLD (ArcFour with HMAC/md5)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (AES-256 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (AES-128 CTS mode with 96-bit SHA-1 HMAC)
       4 07/18/11 21:08:09 host/full.qualified.domain.name@REALM.TLD (ArcFour with HMAC/md5)

### Mapping from Kerberos principals to OS user accounts

Hadoop maps Kerberos principals to OS user (system) accounts using rules specified by `hadoop.security.auth_to_local`. These rules work in the same way as the `auth_to_local` in [Kerberos configuration file (krb5.conf)](http://web.mit.edu/Kerberos/krb5-latest/doc/admin/conf_files/krb5_conf.html). In addition, Hadoop `auth_to_local` mapping supports the **/L** flag that lowercases the returned name.

The default is to pick the first component of the principal name as the system user name if the realm matches the `default_realm` (usually defined in /etc/krb5.conf). e.g. The default rule maps the principal `host/full.qualified.domain.name@REALM.TLD` to system user `host`. The default rule will *not be appropriate* for most clusters.

In a typical cluster HDFS and YARN services will be launched as the system `hdfs` and `yarn` users respectively. `hadoop.security.auth_to_local` can be configured as follows:

    <property>
      <name>hadoop.security.auth_to_local</name>
      <value>
        RULE:[2:$1/$2@$0]([ndj]n/.*@REALM.TLD)s/.*/hdfs/
        RULE:[2:$1/$2@$0]([rn]m/.*@REALM.TLD)s/.*/yarn/
        RULE:[2:$1/$2@$0](jhs/.*@REALM.TLD)s/.*/mapred/
        DEFAULT
      </value>
    </property>

Custom rules can be tested using the `hadoop kerbname` command.  This command allows one to specify a principal and apply Hadoop's current `auth_to_local` ruleset.

### Mapping from user to group

The system user to system group mapping mechanism can be configured via `hadoop.security.group.mapping`. See [Hadoop Groups Mapping](GroupsMapping.html) for details.

Practically you need to manage SSO environment using Kerberos with LDAP for Hadoop in secure mode.

### Proxy user

Some products such as Apache Oozie which access the services of Hadoop on behalf of end users need to be able to impersonate end users. See [the doc of proxy user](./Superusers.html) for details.

### Secure DataNode

Because the DataNode data transfer protocol does not use the Hadoop RPC framework, DataNodes must authenticate themselves using privileged ports which are specified by `dfs.datanode.address` and `dfs.datanode.http.address`. This authentication is based on the assumption that the attacker won't be able to get root privileges on DataNode hosts.

When you execute the `hdfs datanode` command as root, the server process binds privileged ports at first, then drops privilege and runs as the user account specified by `HDFS_DATANODE_SECURE_USER`. This startup process uses [the jsvc program](https://commons.apache.org/proper/commons-daemon/jsvc.html "Link to Apache Commons Jsvc") installed to `JSVC_HOME`. You must specify `HDFS_DATANODE_SECURE_USER` and `JSVC_HOME` as environment variables on start up (in `hadoop-env.sh`).

As of version 2.6.0, SASL can be used to authenticate the data transfer protocol. In this configuration, it is no longer required for secured clusters to start the DataNode as root using `jsvc` and bind to privileged ports. To enable SASL on data transfer protocol, set `dfs.data.transfer.protection` in hdfs-site.xml. A SASL enabled DataNode can be started in secure mode in following two ways:
1. Set a non-privileged port for `dfs.datanode.address`.
1. Set `dfs.http.policy` to `HTTPS_ONLY` or set `dfs.datanode.http.address` to a privileged port and make sure the `HDFS_DATANODE_SECURE_USER` and `JSVC_HOME` environment variables are specified properly as environment variables on start up (in `hadoop-env.sh`).

In order to migrate an existing cluster that used root authentication to start using SASL instead, first ensure that version 2.6.0 or later has been deployed to all cluster nodes as well as any external applications that need to connect to the cluster. Only versions 2.6.0 and later of the HDFS client can connect to a DataNode that uses SASL for authentication of data transfer protocol, so it is vital that all callers have the correct version before migrating. After version 2.6.0 or later has been deployed everywhere, update configuration of any external applications to enable SASL. If an HDFS client is enabled for SASL, then it can connect successfully to a DataNode running with either root authentication or SASL authentication. Changing configuration for all clients guarantees that subsequent configuration changes on DataNodes will not disrupt the applications. Finally, each individual DataNode can be migrated by changing its configuration and restarting. It is acceptable to have a mix of some DataNodes running with root authentication and some DataNodes running with SASL authentication temporarily during this migration period, because an HDFS client enabled for SASL can connect to both.

Data confidentiality
--------------------

### Data Encryption on RPC

The data transfered between hadoop services and clients can be encrypted on the wire. Setting `hadoop.rpc.protection` to `privacy` in `core-site.xml` activates data encryption.

### Data Encryption on Block data transfer.

You need to set `dfs.encrypt.data.transfer` to `true` in the hdfs-site.xml in order to activate data encryption for data transfer protocol of DataNode.

Optionally, you may set `dfs.encrypt.data.transfer.algorithm` to either `3des` or `rc4` to choose the specific encryption algorithm. If unspecified, then the configured JCE default on the system is used, which is usually 3DES.

Setting `dfs.encrypt.data.transfer.cipher.suites` to `AES/CTR/NoPadding` activates AES encryption. By default, this is unspecified, so AES is not used. When AES is used, the algorithm specified in `dfs.encrypt.data.transfer.algorithm` is still used during an initial key exchange. The AES key bit length can be configured by setting `dfs.encrypt.data.transfer.cipher.key.bitlength` to 128, 192 or 256. The default is 128.

AES offers the greatest cryptographic strength and the best performance. At this time, 3DES and RC4 have been used more often in Hadoop clusters.

### Data Encryption on HTTP

Data transfer between Web-console and clients are protected by using SSL(HTTPS). SSL configuration is recommended but not required to configure Hadoop security with Kerberos.

To enable SSL for web console of HDFS daemons, set `dfs.http.policy` to either `HTTPS_ONLY` or `HTTP_AND_HTTPS` in hdfs-site.xml.
Note KMS and HttpFS do not respect this parameter. See [Hadoop KMS](../../hadoop-kms/index.html) and [Hadoop HDFS over HTTP - Server Setup](../../hadoop-hdfs-httpfs/ServerSetup.html) for instructions on enabling KMS over HTTPS and HttpFS over HTTPS, respectively.

To enable SSL for web console of YARN daemons, set `yarn.http.policy` to `HTTPS_ONLY` in yarn-site.xml.

To enable SSL for web console of MapReduce JobHistory server, set `mapreduce.jobhistory.http.policy` to `HTTPS_ONLY` in mapred-site.xml.

Configuration
-------------

### Permissions for both HDFS and local fileSystem paths

The following table lists various paths on HDFS and local filesystems (on all nodes) and recommended permissions:

| Filesystem | Path                                         | User:Group    | Permissions   |
|:-----------|:---------------------------------------------|:--------------|:--------------|
| local      | `dfs.namenode.name.dir`                      | hdfs:hadoop   | `drwx------`  |
| local      | `dfs.datanode.data.dir`                      | hdfs:hadoop   | `drwx------`  |
| local      | `$HADOOP_LOG_DIR`                            | hdfs:hadoop   | `drwxrwxr-x`  |
| local      | `$YARN_LOG_DIR`                              | yarn:hadoop   | `drwxrwxr-x`  |
| local      | `yarn.nodemanager.local-dirs`                | yarn:hadoop   | `drwxr-xr-x`  |
| local      | `yarn.nodemanager.log-dirs`                  | yarn:hadoop   | `drwxr-xr-x`  |
| local      | container-executor                           | root:hadoop   | `--Sr-s--*`   |
| local      | `conf/container-executor.cfg`                | root:hadoop   | `r-------*`   |
| hdfs       | `/`                                          | hdfs:hadoop   | `drwxr-xr-x`  |
| hdfs       | `/tmp`                                       | hdfs:hadoop   | `drwxrwxrwxt` |
| hdfs       | `/user`                                      | hdfs:hadoop   | `drwxr-xr-x`  |
| hdfs       | `yarn.nodemanager.remote-app-log-dir`        | yarn:hadoop   | `drwxrwxrwxt` |
| hdfs       | `mapreduce.jobhistory.intermediate-done-dir` | mapred:hadoop | `drwxrwxrwxt` |
| hdfs       | `mapreduce.jobhistory.done-dir`              | mapred:hadoop | `drwxr-x---`  |

### Common Configurations

In order to turn on RPC authentication in hadoop, set the value of `hadoop.security.authentication` property to `"kerberos"`, and set security related settings listed below appropriately.

The following properties should be in the `core-site.xml` of all the nodes in the cluster.

| Parameter                               | Value                                           | Notes                                                                                                                                                                               |
|:----------------------------------------|:------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `hadoop.security.authentication`        | `kerberos`                                      | `simple` : No authentication. (default)  `kerberos` : Enable authentication by Kerberos.                                                                                            |
| `hadoop.security.authorization`         | `true`                                          | Enable [RPC service-level authorization](./ServiceLevelAuth.html).                                                                                                                  |
| `hadoop.rpc.protection`                 | `authentication`                                | `authentication` : authentication only (default); `integrity` : integrity check in addition to authentication; `privacy` : data encryption in addition to integrity                 |
| `hadoop.security.auth_to_local`         | `RULE:`*`exp1`* `RULE:`*`exp2`* *...* `DEFAULT` | The value is string containing new line characters. See [Kerberos documentation](http://web.mit.edu/Kerberos/krb5-latest/doc/admin/conf_files/krb5_conf.html) for the format of *exp*. |
| `hadoop.proxyuser.`*superuser*`.hosts`  |                                                 | comma separated hosts from which *superuser* access are allowed to impersonation. `*` means wildcard.                                                                               |
| `hadoop.proxyuser.`*superuser*`.groups` |                                                 | comma separated groups to which users impersonated by *superuser* belong. `*` means wildcard.                                                                                       |

### NameNode

| Parameter                                         | Value                                        | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|:--------------------------------------------------|:---------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dfs.block.access.token.enable`                   | `true`                                       | Enable HDFS block access tokens for secure operations.                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `dfs.namenode.kerberos.principal`                 | `nn/_HOST@REALM.TLD`                         | Kerberos principal name for the NameNode.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `dfs.namenode.keytab.file`                        | `/etc/security/keytab/nn.service.keytab`     | Kerberos keytab file for the NameNode.                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `dfs.namenode.kerberos.internal.spnego.principal` | `HTTP/_HOST@REALM.TLD`                       | The server principal used by the NameNode for web UI SPNEGO authentication. The SPNEGO server principal begins with the prefix `HTTP/` by convention. If the value is `'*'`, the web server will attempt to login with every principal specified in the keytab file `dfs.web.authentication.kerberos.keytab`. For most deployments this can be set to `${dfs.web.authentication.kerberos.principal}` i.e use the value of `dfs.web.authentication.kerberos.principal`. |
| `dfs.web.authentication.kerberos.keytab`          | `/etc/security/keytab/spnego.service.keytab` | SPNEGO keytab file for the NameNode. In HA clusters this setting is shared with the Journal Nodes.                                                                                                                                                                                                                                                                                                                                                                     |

The following settings allow configuring SSL access to the NameNode web UI (optional).

| Parameter                    | Value                                           | Notes                                                                                                                                                                                                                                                                                                                                                                                              |
|:-----------------------------|:------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dfs.http.policy`            | `HTTP_ONLY` or `HTTPS_ONLY` or `HTTP_AND_HTTPS` | `HTTPS_ONLY` turns off http access. This option takes precedence over the deprecated configuration dfs.https.enable and hadoop.ssl.enabled. If using SASL to authenticate data transfer protocol instead of running DataNode as root and using privileged ports, then this property must be set to `HTTPS_ONLY` to guarantee authentication of HTTP servers. (See `dfs.data.transfer.protection`.) |
| `dfs.namenode.https-address` | `0.0.0.0:9871`                                 | This parameter is used in non-HA mode and without federation. See [HDFS High Availability](../hadoop-hdfs/HDFSHighAvailabilityWithNFS.html#Deployment) and [HDFS Federation](../hadoop-hdfs/Federation.html#Federation_Configuration) for details.                                                                                                                                                 |
| `dfs.https.enable`           | `true`                                          | This value is deprecated. `Use dfs.http.policy`                                                                                                                                                                                                                                                                                                                                                    |

### Secondary NameNode

| Parameter                                                   | Value                                    | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|:------------------------------------------------------------|:-----------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dfs.namenode.secondary.http-address`                       | `0.0.0.0:9868`                          | HTTP web UI address for the Secondary NameNode.                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `dfs.namenode.secondary.https-address`                      | `0.0.0.0:9869`                          | HTTPS web UI address for the Secondary NameNode.                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `dfs.secondary.namenode.keytab.file`                        | `/etc/security/keytab/sn.service.keytab` | Kerberos keytab file for the Secondary NameNode.                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `dfs.secondary.namenode.kerberos.principal`                 | `sn/_HOST@REALM.TLD`                     | Kerberos principal name for the Secondary NameNode.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `dfs.secondary.namenode.kerberos.internal.spnego.principal` | `HTTP/_HOST@REALM.TLD`                   | The server principal used by the Secondary NameNode for web UI SPNEGO authentication. The SPNEGO server principal begins with the prefix `HTTP/` by convention. If the value is `'*'`, the web server will attempt to login with every principal specified in the keytab file `dfs.web.authentication.kerberos.keytab`. For most deployments this can be set to `${dfs.web.authentication.kerberos.principal}` i.e use the value of `dfs.web.authentication.kerberos.principal`. |

### JournalNode

| Parameter                                            | Value                                        | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|:-----------------------------------------------------|:---------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dfs.journalnode.kerberos.principal`                 | `jn/_HOST@REALM.TLD`                         | Kerberos principal name for the JournalNode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `dfs.journalnode.keytab.file`                        | `/etc/security/keytab/jn.service.keytab`     | Kerberos keytab file for the JournalNode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `dfs.journalnode.kerberos.internal.spnego.principal` | `HTTP/_HOST@REALM.TLD`                       | The server principal used by the JournalNode for web UI SPNEGO authentication when Kerberos security is enabled. The SPNEGO server principal begins with the prefix `HTTP/` by convention. If the value is `'*'`, the web server will attempt to login with every principal specified in the keytab file `dfs.web.authentication.kerberos.keytab`. For most deployments this can be set to `${dfs.web.authentication.kerberos.principal}` i.e use the value of `dfs.web.authentication.kerberos.principal`. |
| `dfs.web.authentication.kerberos.keytab`             | `/etc/security/keytab/spnego.service.keytab` | SPNEGO keytab file for the JournalNode. In HA clusters this setting is shared with the Name Nodes.                                                                                                                                                                                                                                                                                                                                                                                                          |
| `dfs.journalnode.https-address`                      | `0.0.0.0:8481`                               | HTTPS web UI address for the JournalNode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

### DataNode

| Parameter                                        | Value                                    | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|:-------------------------------------------------|:-----------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dfs.datanode.data.dir.perm`                     | `700`                                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `dfs.datanode.address`                           | `0.0.0.0:1004`                           | Secure DataNode must use privileged port in order to assure that the server was started securely. This means that the server must be started via jsvc. Alternatively, this must be set to a non-privileged port if using SASL to authenticate data transfer protocol. (See `dfs.data.transfer.protection`.)                                                                                                                                                                                                                  |
| `dfs.datanode.http.address`                      | `0.0.0.0:1006`                           | Secure DataNode must use privileged port in order to assure that the server was started securely. This means that the server must be started via jsvc.                                                                                                                                                                                                                                                                                                                                                                       |
| `dfs.datanode.https.address`                     | `0.0.0.0:9865`                          | HTTPS web UI address for the Data Node.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `dfs.datanode.kerberos.principal`                | `dn/_HOST@REALM.TLD`                     | Kerberos principal name for the DataNode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `dfs.datanode.keytab.file`                       | `/etc/security/keytab/dn.service.keytab` | Kerberos keytab file for the DataNode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `dfs.encrypt.data.transfer`                      | `false`                                  | set to `true` when using data encryption                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `dfs.encrypt.data.transfer.algorithm`            |                                          | optionally set to `3des` or `rc4` when using data encryption to control encryption algorithm                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `dfs.encrypt.data.transfer.cipher.suites`        |                                          | optionally set to `AES/CTR/NoPadding` to activate AES encryption when using data encryption                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `dfs.encrypt.data.transfer.cipher.key.bitlength` |                                          | optionally set to `128`, `192` or `256` to control key bit length when using AES with data encryption                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `dfs.data.transfer.protection`                   |                                          | `authentication` : authentication only; `integrity` : integrity check in addition to authentication; `privacy` : data encryption in addition to integrity This property is unspecified by default. Setting this property enables SASL for authentication of data transfer protocol. If this is enabled, then `dfs.datanode.address` must use a non-privileged port, `dfs.http.policy` must be set to `HTTPS_ONLY` and the `HDFS_DATANODE_SECURE_USER` environment variable must be undefined when starting the DataNode process. |

### WebHDFS

| Parameter                                   | Value                                      | Notes                                                                                                                                                                     |
|:--------------------------------------------|:-------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dfs.web.authentication.kerberos.principal` | `http/_HOST@REALM.TLD`                     | Kerberos principal name for the WebHDFS. In HA clusters this setting is commonly used by the JournalNodes for securing access to the JournalNode HTTP server with SPNEGO. |
| `dfs.web.authentication.kerberos.keytab`    | `/etc/security/keytab/http.service.keytab` | Kerberos keytab file for WebHDFS. In HA clusters this setting is commonly used the JournalNodes for securing access to the JournalNode HTTP server with SPNEGO.           |

### ResourceManager

| Parameter                                    | Value                                    | Notes                                                                                                                                                                                                                                                                                     |
|:---------------------------------------------|:-----------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.resourcemanager.principal`             | `rm/_HOST@REALM.TLD`                     | Kerberos principal name for the ResourceManager.                                                                                                                                                                                                                                          |
| `yarn.resourcemanager.keytab`                | `/etc/security/keytab/rm.service.keytab` | Kerberos keytab file for the ResourceManager.                                                                                                                                                                                                                                             |
| `yarn.resourcemanager.webapp.https.address`  | `${yarn.resourcemanager.hostname}:8090`  | The https adddress of the RM web application for non-HA. In HA clusters, use `yarn.resourcemanager.webapp.https.address.`*rm-id* for each ResourceManager. See [ResourceManager High Availability](../../hadoop-yarn/hadoop-yarn-site/ResourceManagerHA.html#Configurations) for details. |

### NodeManager

| Parameter                                         | Value                                                              | Notes                                                   |
|:--------------------------------------------------|:-------------------------------------------------------------------|:--------------------------------------------------------|
| `yarn.nodemanager.principal`                      | `nm/_HOST@REALM.TLD`                                               | Kerberos principal name for the NodeManager.            |
| `yarn.nodemanager.keytab`                         | `/etc/security/keytab/nm.service.keytab`                           | Kerberos keytab file for the NodeManager.               |
| `yarn.nodemanager.container-executor.class`       | `org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor` | Use LinuxContainerExecutor.                             |
| `yarn.nodemanager.linux-container-executor.group` | `hadoop`                                                           | Unix group of the NodeManager.                          |
| `yarn.nodemanager.linux-container-executor.path`  | `/path/to/bin/container-executor`                                  | The path to the executable of Linux container executor. |
| `yarn.nodemanager.webapp.https.address`           | `0.0.0.0:8044`                                                     | The https adddress of the NM web application.           |

### Configuration for WebAppProxy

The `WebAppProxy` provides a proxy between the web applications exported by an application and an end user. If security is enabled it will warn users before accessing a potentially unsafe web application. Authentication and authorization using the proxy is handled just like any other privileged web application.

| Parameter                  | Value                                             | Notes                                                                                                                                                                                                     |
|:---------------------------|:--------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.web-proxy.address`   | `WebAppProxy` host:port for proxy to AM web apps. | `host:port` if this is the same as `yarn.resourcemanager.webapp.address` or it is not defined then the `ResourceManager` will run the proxy otherwise a standalone proxy server will need to be launched. |
| `yarn.web-proxy.keytab`    | `/etc/security/keytab/web-app.service.keytab`     | Kerberos keytab file for the WebAppProxy.                                                                                                                                                                 |
| `yarn.web-proxy.principal` | `wap/_HOST@REALM.TLD`                             | Kerberos principal name for the WebAppProxy.                                                                                                                                                              |

### LinuxContainerExecutor

A `ContainerExecutor` used by YARN framework which define how any *container* launched and controlled.

The following are the available in Hadoop YARN:

| ContainerExecutor          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|:---------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `DefaultContainerExecutor` | The default executor which YARN uses to manage container execution. The container process has the same Unix user as the NodeManager.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `LinuxContainerExecutor`   | Supported only on GNU/Linux, this executor runs the containers as either the YARN user who submitted the application (when full security is enabled) or as a dedicated user (defaults to nobody) when full security is not enabled. When full security is enabled, this executor requires all user accounts to be created on the cluster nodes where the containers are launched. It uses a `setuid` executable that is included in the Hadoop distribution. The NodeManager uses this executable to launch and kill containers. The setuid executable switches to the user who has submitted the application and launches or kills the containers. For maximum security, this executor sets up restricted permissions and user/group ownership of local files and directories used by the containers such as the shared objects, jars, intermediate files, log files etc. Particularly note that, because of this, except the application owner and NodeManager, no other user can access any of the local files/directories including those localized as part of the distributed cache. |

To build the LinuxContainerExecutor executable run:

     $ mvn package -Dcontainer-executor.conf.dir=/etc/hadoop/

The path passed in `-Dcontainer-executor.conf.dir` should be the path on the cluster nodes where a configuration file for the setuid executable should be located. The executable should be installed in `$HADOOP_YARN_HOME/bin`.

The executable must have specific permissions: 6050 or `--Sr-s---` permissions user-owned by `root` (super-user) and group-owned by a special group (e.g. `hadoop`) of which the NodeManager Unix user is the group member and no ordinary application user is. If any application user belongs to this special group, security will be compromised. This special group name should be specified for the configuration property `yarn.nodemanager.linux-container-executor.group` in both `conf/yarn-site.xml` and `conf/container-executor.cfg`.

For example, let's say that the NodeManager is run as user `yarn` who is part of the groups `users` and `hadoop`, any of them being the primary group. Let also be that `users` has both `yarn` and another user (application submitter) `alice` as its members, and `alice` does not belong to `hadoop`. Going by the above description, the setuid/setgid executable should be set 6050 or `--Sr-s---` with user-owner as `yarn` and group-owner as `hadoop` which has `yarn` as its member (and not `users` which has `alice` also as its member besides `yarn`).

The LinuxTaskController requires that paths including and leading up to the directories specified in `yarn.nodemanager.local-dirs` and `yarn.nodemanager.log-dirs` to be set 755 permissions as described above in the table on permissions on directories.

* `conf/container-executor.cfg`

The executable requires a configuration file called `container-executor.cfg` to be present in the configuration directory passed to the mvn target mentioned above.

The configuration file must be owned by the user running NodeManager (user `yarn` in the above example), group-owned by anyone and should have the permissions 0400 or `r--------` .

The executable requires following configuration items to be present in the `conf/container-executor.cfg` file. The items should be mentioned as simple key=value pairs, one per-line:

| Parameter                                         | Value                  | Notes                                                                                                                                                                                                                                                                             |
|:--------------------------------------------------|:-----------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.nodemanager.linux-container-executor.group` | `hadoop`               | Unix group of the NodeManager. The group owner of the `container-executor` binary should be this group. Should be same as the value with which the NodeManager is configured. This configuration is required for validating the secure access of the `container-executor` binary. |
| `banned.users`                                    | `hdfs,yarn,mapred,bin` | Banned users.                                                                                                                                                                                                                                                                     |
| `allowed.system.users`                            | `foo,bar`              | Allowed system users.                                                                                                                                                                                                                                                             |
| `min.user.id`                                     | `1000`                 | Prevent other super-users.                                                                                                                                                                                                                                                        |

To re-cap, here are the local file-sysytem permissions required for the various paths related to the `LinuxContainerExecutor`:

| Filesystem | Path                          | User:Group  | Permissions  |
|:-----------|:------------------------------|:------------|:-------------|
| local      | `container-executor`          | root:hadoop | `--Sr-s--*`  |
| local      | `conf/container-executor.cfg` | root:hadoop | `r-------*`  |
| local      | `yarn.nodemanager.local-dirs` | yarn:hadoop | `drwxr-xr-x` |
| local      | `yarn.nodemanager.log-dirs`   | yarn:hadoop | `drwxr-xr-x` |

### MapReduce JobHistory Server

| Parameter                        | Value                                     | Notes                                                        |
|:---------------------------------|:------------------------------------------|:-------------------------------------------------------------|
| `mapreduce.jobhistory.address`   | MapReduce JobHistory Server `host:port`   | Default port is 10020.                                       |
| `mapreduce.jobhistory.keytab`    | `/etc/security/keytab/jhs.service.keytab` | Kerberos keytab file for the MapReduce JobHistory Server.    |
| `mapreduce.jobhistory.principal` | `jhs/_HOST@REALM.TLD`                     | Kerberos principal name for the MapReduce JobHistory Server. |


Multihoming
-----------

Multihomed setups where each host has multiple hostnames in DNS (e.g. different hostnames corresponding to public and private network interfaces) may require additional configuration to get Kerberos authentication working. See [HDFS Support for Multihomed Networks](../hadoop-hdfs/HdfsMultihoming.html)

Troubleshooting
---------------

Kerberos is hard to set up —and harder to debug. Common problems are

1. Network and DNS configuration.
2. Kerberos configuration on hosts (`/etc/krb5.conf`).
3. Keytab creation and maintenance.
4. Environment setup: JVM, user login, system clocks, etc.

The fact that the error messages from the JVM are essentially meaningless
does not aid in diagnosing and fixing such problems.

Extra debugging information can be enabled for the client and for any service

Set the environment variable `HADOOP_JAAS_DEBUG` to `true`.

```bash
export HADOOP_JAAS_DEBUG=true
```

Edit the `log4j.properties` file to log Hadoop's security package at `DEBUG` level.

```
log4j.logger.org.apache.hadoop.security=DEBUG
```

Enable JVM-level debugging by setting some system properties.

```
export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true -Dsun.security.krb5.debug=true -Dsun.security.spnego.debug"
```

Troubleshooting with `KDiag`
---------------------------

Hadoop has a tool to aid validating setup: `KDiag`

It contains a series of probes for the JVM's configuration and the environment,
dumps out some system files (`/etc/krb5.conf`, `/etc/ntp.conf`), prints
out some system state and then attempts to log in to Kerberos as the current user,
or a specific principal in a named keytab.

The output of the command can be used for local diagnostics, or forwarded to
whoever supports the cluster.

The `KDiag` command has its own entry point; It is invoked by passing `kdiag` to
`bin/hadoop` command. Accordingly, it will display the kerberos client state
of the command used to invoke it.

```
hadoop kdiag
```

The command returns a status code of 0 for a successful diagnostics run.
This does not imply that Kerberos is working —merely that the KDiag command
did not identify any problem from its limited set of probes. In particular,
as it does not attempt to connect to any remote service, it does not verify
that the client is trusted by any service.

If unsuccessful, exit codes are

* -1: the command failed for an unknown reason
* 41: Unauthorized (== HTTP's 401). KDiag detected a condition which causes
Kerberos to not work. Examine the output to identify the issue.

### Usage

```
KDiag: Diagnose Kerberos Problems
  [-D key=value] : Define a configuration option.
  [--jaas] : Require a JAAS file to be defined in java.security.auth.login.config.
  [--keylen <keylen>] : Require a minimum size for encryption keys supported by the JVM. Default value : 256.
  [--keytab <keytab> --principal <principal>] : Login from a keytab as a specific principal.
  [--nofail] : Do not fail on the first problem.
  [--nologin] : Do not attempt to log in.
  [--out <file>] : Write output to a file.
  [--resource <resource>] : Load an XML configuration resource.
  [--secure] : Require the hadoop configuration to be secure.
  [--verifyshortname <principal>]: Verify the short name of the specific principal does not contain '@' or '/'
```

#### `--jaas`: Require a JAAS file to be defined in `java.security.auth.login.config`.

If `--jaas` is set, the Java system property `java.security.auth.login.config` must be
set to a JAAS file; this file must exist, be a simple file of non-zero bytes,
and readable by the current user. More detailed validation is not performed.

JAAS files are not needed by Hadoop itself, but some services (such as Zookeeper)
do require them for secure operation.

#### `--keylen <length>`: Require a minimum size for encryption keys supported by the JVM".

If the JVM does not support this length, the command will fail.

The default value is to 256, as needed for the `AES256` encryption scheme.
A JVM without the Java Cryptography Extensions installed does not support
such a key length. Kerberos will not work unless configured to use
an encryption scheme with a shorter key length.

#### `--keytab <keytab> --principal <principal>`: Log in from a keytab.

Log in from a keytab as the specific principal.

1. The file must contain the specific principal, including any named host.
That is, there is no mapping from `_HOST` to the current hostname.
1. KDiag will log out and attempt to log back in again. This catches
JVM compatibility problems which have existed in the past. (Hadoop's
Kerberos support requires use of/introspection into JVM-specific classes).

#### `--nofail` : Do not fail on the first problem

KDiag will make a best-effort attempt to diagnose all Kerberos problems,
rather than stop at the first one.

This is somewhat limited; checks are made in the order which problems
surface (e.g keylength is checked first), so an early failure can trigger
many more problems. But it does produce a more detailed report.

#### `--nologin`: Do not attempt to log in.

Skip trying to log in. This takes precedence over the `--keytab` option,
and also disables trying to log in to kerberos as the current kinited user.

This is useful when the KDiag command is being invoked within an application,
as it does not set up Hadoop's static security state —merely check for
some basic Kerberos preconditions.

#### `--out outfile`: Write output to file.

```
hadoop kdiag --out out.txt
```

Much of the diagnostics information comes from the JRE (to `stderr`) and
from Log4j (to `stdout`).
To get all the output, it is best to redirect both these output streams
to the same file, and omit the `--out` option.

```
hadoop kdiag --keytab zk.service.keytab --principal zookeeper/devix.example.org@REALM > out.txt 2>&1
```

Even there, the output of the two streams, emitted across multiple threads, can
be a bit confusing. It will get easier with practise. Looking at the thread
name in the Log4j output to distinguish background threads from the main thread
helps at the hadoop level, but doesn't assist in JVM-level logging.

#### `--resource <resource>` : XML configuration resource to load.
To load XML configuration files, this option can be used. As by default, the
`core-default` and `core-site` XML resources are only loaded. This will help,
when additional configuration files has any Kerberos related configurations.

```
hadoop kdiag --resource hbase-default.xml --resource hbase-site.xml
```

For extra logging during the operation, set the logging and `HADOOP_JAAS_DEBUG`
environment variable to the values listed in "Troubleshooting". The JVM
options are automatically set in KDiag.

#### `--secure`: Fail if the command is not executed on a secure cluster.

That is: if the authentication mechanism of the cluster is explicitly
or implicitly set to "simple":

```xml
<property>
  <name>hadoop.security.authentication</name>
  <value>simple</value>
</property>
```

Needless to say, an application so configured cannot talk to a secure Hadoop cluster.

#### `--verifyshortname <principal>`: validate the short name of a principal

This verifies that the short name of a principal contains neither the `"@"`
nor `"/"` characters.

### Example

```
hadoop kdiag \
  --nofail \
  --resource hdfs-site.xml --resource yarn-site.xml \
  --keylen 1024 \
  --keytab zk.service.keytab --principal zookeeper/devix.example.org@REALM
```

This attempts to to perform all diagnostics without failing early, load in
the HDFS and YARN XML resources, require a minimum key length of 1024 bytes,
and log in as the principal `zookeeper/devix.example.org@REALM`, whose key must be in
the keytab `zk.service.keytab`


References
----------

1. O'Malley O et al. [Hadoop Security Design](https://issues.apache.org/jira/secure/attachment/12428537/security-design.pdf)
1. O'Malley O, [Hadoop Security Architecture](http://www.slideshare.net/oom65/hadoop-security-architecture)
1. [Troubleshooting Kerberos on Java 7](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html)
1. [Troubleshooting Kerberos on Java 8](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html)
1. [Java 7 Kerberos Requirements](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html)
1. [Java 8 Kerberos Requirements](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html)
1. Loughran S., [Hadoop and Kerberos: The Madness beyond the Gate](https://steveloughran.gitbooks.io/kerberos_and_hadoop/content/)

