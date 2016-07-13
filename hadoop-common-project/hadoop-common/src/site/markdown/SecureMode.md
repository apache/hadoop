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

* [Hadoop in Secure Mode](#Hadoop_in_Secure_Mode)
    * [Introduction](#Introduction)
    * [Authentication](#Authentication)
        * [End User Accounts](#End_User_Accounts)
        * [User Accounts for Hadoop Daemons](#User_Accounts_for_Hadoop_Daemons)
        * [Kerberos principals for Hadoop Daemons](#Kerberos_principals_for_Hadoop_Daemons)
        * [Mapping from Kerberos principals to OS user accounts](#Mapping_from_Kerberos_principals_to_OS_user_accounts)
        * [Mapping from user to group](#Mapping_from_user_to_group)
        * [Proxy user](#Proxy_user)
        * [Secure DataNode](#Secure_DataNode)
    * [Data confidentiality](#Data_confidentiality)
        * [Data Encryption on RPC](#Data_Encryption_on_RPC)
        * [Data Encryption on Block data transfer.](#Data_Encryption_on_Block_data_transfer.)
        * [Data Encryption on HTTP](#Data_Encryption_on_HTTP)
    * [Configuration](#Configuration)
        * [Permissions for both HDFS and local fileSystem paths](#Permissions_for_both_HDFS_and_local_fileSystem_paths)
        * [Common Configurations](#Common_Configurations)
        * [NameNode](#NameNode)
        * [Secondary NameNode](#Secondary_NameNode)
        * [JournalNode](#JournalNode)
        * [DataNode](#DataNode)
        * [WebHDFS](#WebHDFS)
        * [ResourceManager](#ResourceManager)
        * [NodeManager](#NodeManager)
        * [Configuration for WebAppProxy](#Configuration_for_WebAppProxy)
        * [LinuxContainerExecutor](#LinuxContainerExecutor)
        * [MapReduce JobHistory Server](#MapReduce_JobHistory_Server)
    * [Multihoming](#Multihoming)
    * [References](#References)

Hadoop in Secure Mode
=====================

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
        RULE:[2:$1@$0](nn/.*@.*REALM.TLD)s/.*/hdfs/
        RULE:[2:$1@$0](jn/.*@.*REALM.TLD)s/.*/hdfs/
        RULE:[2:$1@$0](dn/.*@.*REALM.TLD)s/.*/hdfs/
        RULE:[2:$1@$0](nm/.*@.*REALM.TLD)s/.*/yarn/
        RULE:[2:$1@$0](rm/.*@.*REALM.TLD)s/.*/yarn/
        RULE:[2:$1@$0](jhs/.*@.*REALM.TLD)s/.*/mapred/
        DEFAULT
      </value>
    </property>

Custom rules can be tested using the `hadoop kerbname` command.  This command allows one to specify a principal and apply Hadoop's current `auth_to_local` ruleset.

### Mapping from user to group

The system user to system group mapping mechanism can be configured via `hadoop.security.group.mapping`. See [HDFS Permissions Guide](../hadoop-hdfs/HdfsPermissionsGuide.html#Group_Mapping) for details.

Practically you need to manage SSO environment using Kerberos with LDAP for Hadoop in secure mode.

### Proxy user

Some products such as Apache Oozie which access the services of Hadoop on behalf of end users need to be able to impersonate end users. See [the doc of proxy user](./Superusers.html) for details.

### Secure DataNode

Because the DataNode data transfer protocol does not use the Hadoop RPC framework, DataNodes must authenticate themselves using privileged ports which are specified by `dfs.datanode.address` and `dfs.datanode.http.address`. This authentication is based on the assumption that the attacker won't be able to get root privileges on DataNode hosts.

When you execute the `hdfs datanode` command as root, the server process binds privileged ports at first, then drops privilege and runs as the user account specified by `HADOOP_SECURE_DN_USER`. This startup process uses [the jsvc program](https://commons.apache.org/proper/commons-daemon/jsvc.html "Link to Apache Commons Jsvc") installed to `JSVC_HOME`. You must specify `HADOOP_SECURE_DN_USER` and `JSVC_HOME` as environment variables on start up (in `hadoop-env.sh`).

As of version 2.6.0, SASL can be used to authenticate the data transfer protocol. In this configuration, it is no longer required for secured clusters to start the DataNode as root using `jsvc` and bind to privileged ports. To enable SASL on data transfer protocol, set `dfs.data.transfer.protection` in hdfs-site.xml, set a non-privileged port for `dfs.datanode.address`, set `dfs.http.policy` to `HTTPS_ONLY` and make sure the `HADOOP_SECURE_DN_USER` environment variable is not defined. Note that it is not possible to use SASL on data transfer protocol if `dfs.datanode.address` is set to a privileged port. This is required for backwards-compatibility reasons.

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
| `dfs.namenode.https-address` | `nn_host_fqdn:50470`                            |                                                                                                                                                                                                                                                                                                                                                                                                    |
| `dfs.https.port`             | `50470`                                         |                                                                                                                                                                                                                                                                                                                                                                                                    |
| `dfs.https.enable`           | `true`                                          | This value is deprecated. `Use dfs.http.policy`                                                                                                                                                                                                                                                                                                                                                    |

### Secondary NameNode

| Parameter                                                   | Value                                    | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|:------------------------------------------------------------|:-----------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dfs.namenode.secondary.http-address`                       | `snn_host_fqdn:50090`                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `dfs.secondary.namenode.keytab.file`                        | `/etc/security/keytab/sn.service.keytab` | Kerberos keytab file for the Secondary NameNode.                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `dfs.secondary.namenode.kerberos.principal`                 | `sn/_HOST@REALM.TLD`                     | Kerberos principal name for the Secondary NameNode.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `dfs.secondary.namenode.kerberos.internal.spnego.principal` | `HTTP/_HOST@REALM.TLD`                   | The server principal used by the Secondary NameNode for web UI SPNEGO authentication. The SPNEGO server principal begins with the prefix `HTTP/` by convention. If the value is `'*'`, the web server will attempt to login with every principal specified in the keytab file `dfs.web.authentication.kerberos.keytab`. For most deployments this can be set to `${dfs.web.authentication.kerberos.principal}` i.e use the value of `dfs.web.authentication.kerberos.principal`. |
| `dfs.namenode.secondary.https-port`                         | `50470`                                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

### JournalNode

| Parameter                                            | Value                                        | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|:-----------------------------------------------------|:---------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dfs.journalnode.kerberos.principal`                 | `jn/_HOST@REALM.TLD`                         | Kerberos principal name for the JournalNode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `dfs.journalnode.keytab.file`                        | `/etc/security/keytab/jn.service.keytab`     | Kerberos keytab file for the JournalNode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `dfs.journalnode.kerberos.internal.spnego.principal` | `HTTP/_HOST@REALM.TLD`                       | The server principal used by the JournalNode for web UI SPNEGO authentication when Kerberos security is enabled. The SPNEGO server principal begins with the prefix `HTTP/` by convention. If the value is `'*'`, the web server will attempt to login with every principal specified in the keytab file `dfs.web.authentication.kerberos.keytab`. For most deployments this can be set to `${dfs.web.authentication.kerberos.principal}` i.e use the value of `dfs.web.authentication.kerberos.principal`. |
| `dfs.web.authentication.kerberos.keytab`             | `/etc/security/keytab/spnego.service.keytab` | SPNEGO keytab file for the JournalNode. In HA clusters this setting is shared with the Name Nodes.                                                                                                                                                                                                                                                                                                                                                                                                          |

### DataNode

| Parameter                                        | Value                                    | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|:-------------------------------------------------|:-----------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dfs.datanode.data.dir.perm`                     | `700`                                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `dfs.datanode.address`                           | `0.0.0.0:1004`                           | Secure DataNode must use privileged port in order to assure that the server was started securely. This means that the server must be started via jsvc. Alternatively, this must be set to a non-privileged port if using SASL to authenticate data transfer protocol. (See `dfs.data.transfer.protection`.)                                                                                                                                                                                                                  |
| `dfs.datanode.http.address`                      | `0.0.0.0:1006`                           | Secure DataNode must use privileged port in order to assure that the server was started securely. This means that the server must be started via jsvc.                                                                                                                                                                                                                                                                                                                                                                       |
| `dfs.datanode.https.address`                     | `0.0.0.0:50470`                          |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `dfs.datanode.kerberos.principal`                | `dn/_HOST@REALM.TLD`                     | Kerberos principal name for the DataNode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `dfs.datanode.keytab.file`                       | `/etc/security/keytab/dn.service.keytab` | Kerberos keytab file for the DataNode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `dfs.encrypt.data.transfer`                      | `false`                                  | set to `true` when using data encryption                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `dfs.encrypt.data.transfer.algorithm`            |                                          | optionally set to `3des` or `rc4` when using data encryption to control encryption algorithm                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `dfs.encrypt.data.transfer.cipher.suites`        |                                          | optionally set to `AES/CTR/NoPadding` to activate AES encryption when using data encryption                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `dfs.encrypt.data.transfer.cipher.key.bitlength` |                                          | optionally set to `128`, `192` or `256` to control key bit length when using AES with data encryption                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `dfs.data.transfer.protection`                   |                                          | `authentication` : authentication only; `integrity` : integrity check in addition to authentication; `privacy` : data encryption in addition to integrity This property is unspecified by default. Setting this property enables SASL for authentication of data transfer protocol. If this is enabled, then `dfs.datanode.address` must use a non-privileged port, `dfs.http.policy` must be set to `HTTPS_ONLY` and the `HADOOP_SECURE_DN_USER` environment variable must be undefined when starting the DataNode process. |

### WebHDFS

| Parameter                                   | Value                                      | Notes                                                                                                                                                                     |
|:--------------------------------------------|:-------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dfs.web.authentication.kerberos.principal` | `http/_HOST@REALM.TLD`                     | Kerberos principal name for the WebHDFS. In HA clusters this setting is commonly used by the JournalNodes for securing access to the JournalNode HTTP server with SPNEGO. |
| `dfs.web.authentication.kerberos.keytab`    | `/etc/security/keytab/http.service.keytab` | Kerberos keytab file for WebHDFS. In HA clusters this setting is commonly used the JournalNodes for securing access to the JournalNode HTTP server with SPNEGO.           |

### ResourceManager

| Parameter                        | Value                                    | Notes                                            |
|:---------------------------------|:-----------------------------------------|:-------------------------------------------------|
| `yarn.resourcemanager.principal` | `rm/_HOST@REALM.TLD`                     | Kerberos principal name for the ResourceManager. |
| `yarn.resourcemanager.keytab`    | `/etc/security/keytab/rm.service.keytab` | Kerberos keytab file for the ResourceManager.    |

### NodeManager

| Parameter                                         | Value                                                              | Notes                                                   |
|:--------------------------------------------------|:-------------------------------------------------------------------|:--------------------------------------------------------|
| `yarn.nodemanager.principal`                      | `nm/_HOST@REALM.TLD`                                               | Kerberos principal name for the NodeManager.            |
| `yarn.nodemanager.keytab`                         | `/etc/security/keytab/nm.service.keytab`                           | Kerberos keytab file for the NodeManager.               |
| `yarn.nodemanager.container-executor.class`       | `org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor` | Use LinuxContainerExecutor.                             |
| `yarn.nodemanager.linux-container-executor.group` | `hadoop`                                                           | Unix group of the NodeManager.                          |
| `yarn.nodemanager.linux-container-executor.path`  | `/path/to/bin/container-executor`                                  | The path to the executable of Linux container executor. |

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

References
----------

1. O'Malley O et al. [Hadoop Security Design](https://issues.apache.org/jira/secure/attachment/12428537/security-design.pdf)
1. O'Malley O, [Hadoop Security Architecture](http://www.slideshare.net/oom65/hadoop-security-architecture)
1. [Troubleshooting Kerberos on Java 7](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html)
1. [Troubleshooting Kerberos on Java 8](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html)
1. [Java 7 Kerberos Requirements](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html)
1. [Java 8 Kerberos Requirements](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html)
1. Loughran S., [Hadoop and Kerberos: The Madness beyond the Gate](https://steveloughran.gitbooks.io/kerberos_and_hadoop/content/)

