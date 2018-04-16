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

Hadoop Groups Mapping
===================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
--------
The groups of a user is determined by a group mapping service provider.
Hadoop supports various group mapping mechanisms, configured by the `hadoop.security.group.mapping` property. Some of them, such as `JniBasedUnixGroupsMappingWithFallback`, use operating
systems' group name resolution and requires no configuration. But Hadoop also supports special group mapping mechanisms through
LDAP and composition of LDAP and operating system group name resolution, which require additional configurations.
`hadoop.security.group.mapping` can be one of the following:

*    **org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback**

     The default implementation. It will determine if the Java Native Interface (JNI) is available. If JNI is available, the implementation will use the API within hadoop to resolve a list of groups for a user. If JNI is not available then the shell-based implementation, `ShellBasedUnixGroupsMapping`, is used.

*    **org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMappingWithFallback**

     Similar to `JniBasedUnixGroupsMappingWithFallback`. If JNI is available, it obtains netgroup membership using the Hadoop native API; otherwise uses `ShellBasedUnixGroupsNetgroupMapping`.

*    **org.apache.hadoop.security.ShellBasedUnixGroupsMapping**

     This implementation shells out with the `bash -c groups` command (for a Linux/Unix environment) or the `net group` command (for a Windows environment) to resolve a list of groups for a user.

*    **org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping**

     This implementation is similar to `ShellBasedUnixGroupsMapping`, except that it executes `getent netgroup` command to get netgroup membership.

*    **org.apache.hadoop.security.LdapGroupsMapping**

     An alternate implementation, which connects directly to an LDAP server to resolve the list of groups. However, this provider should only be used if the required groups reside exclusively in LDAP, and are not materialized on the Unix servers.
     LdapGroupsMapping supports SSL connection and POSIX group semantics. See section [LDAP Groups Mapping](#LDAP_Groups_Mapping) for details.

*    **org.apache.hadoop.security.CompositeGroupsMapping**

     This implementation composites other group mapping providers for determining group membership. This allows to combine existing provider implementations and composite a virtually new provider without customized development to deal with complex situation. See section [Composite Groups Mapping](#Composite_Groups_Mapping) for details.

For HDFS, the mapping of users to groups is performed on the NameNode. Thus, the host system configuration of the NameNode determines the group mappings for the users.

Note that HDFS stores the user and group of a file or directory as strings; there is no conversion from user and group identity numbers as is conventional in Unix.

Static Mapping
--------
It is possible to statically map users to groups by defining the mapping in `hadoop.user.group.static.mapping.overrides` in the format `user1=group1,group2;user2=;user3=group2`.
This property overrides any group mapping service provider. If a user's groups are defined in it, the groups are returned without more lookups; otherwise, the service provider defined in `hadoop.security.group.mapping` is used to look up the groups. By default, `dr.who=;` is defined, so the fake user dr.who will not have any groups.

Caching/Negative caching
--------
Since the group mapping resolution relies on external mechanisms, the NameNode performance may be impacted. To reduce the impact due to repeated lookups, Hadoop caches the groups returned by the service provider. The cache invalidate is configurable via `hadoop.security.groups.cache.secs`, and the default is 300 seconds.

With the default caching implementation, after `hadoop.security.groups.cache.secs` when the cache entry expires, the next thread to request group membership will query the group mapping service provider to lookup the current groups for the user. While this lookup is running, the thread that initiated it will block, while any other threads requesting groups for the same user will retrieve the previously cached values. If the refresh fails, the thread performing the refresh will throw an exception and the process will repeat for the next thread that requests a lookup for that value. If the lookup repeatedly fails, and the cache is not updated, after `hadoop.security.groups.cache.secs * 10` seconds the cached entry will be evicted and all threads will block until a successful reload is performed.

To avoid any threads blocking when the cached entry expires, set `hadoop.security.groups.cache.background.reload` to true. This enables a small thread pool of `hadoop.security.groups.cache.background.reload.threads` threads having 3 threads by default. With this setting, when the cache is queried for an expired entry, the expired result is returned immediately and a task is queued to refresh the cache in the background. If the background refresh fails a new refresh operation will be queued by the next request to the cache, until `hadoop.security.groups.cache.secs * 10` when the cached entry will be evicted and all threads will block for that user until a successful reload occurs.

To avoid spamming NameNode with unknown users, Hadoop employs negative caching so that if the result of the lookup is empty, return an empty group directly instead of performing more group mapping queries,
The cache invalidation is configurable via `hadoop.security.groups.negative-cache.secs`. The default is 30 seconds, so if group mapping service providers returns no group for a user, no lookup will be performed for the same user within 30 seconds.

LDAP Groups Mapping
--------
This provider supports LDAP with simple password authentication using JNDI API.
`hadoop.security.group.mapping.ldap.url` must be set. This refers to the URL of the LDAP server for resolving user groups.

`hadoop.security.group.mapping.ldap.base` configures the search base for the LDAP connection. This is a distinguished name, and will typically be the root of the LDAP directory.
Get groups for a given username first looks up the user and then looks up the groups for the user result. If the directory setup has different user and group search bases, use `hadoop.security.group.mapping.ldap.userbase` and `hadoop.security.group.mapping.ldap.groupbase` configs.

If the LDAP server does not support anonymous binds,
set the distinguished name of the user to bind in `hadoop.security.group.mapping.ldap.bind.user`.
The path to the file containing the bind user's password is specified in `hadoop.security.group.mapping.ldap.bind.password.file`.
This file should be readable only by the Unix user running the daemons.

It is possible to set a maximum time limit when searching and awaiting a result.
Set `hadoop.security.group.mapping.ldap.directory.search.timeout` to 0 if infinite wait period is desired. Default is 10,000 milliseconds (10 seconds).
This is the limit for each ldap query.  If `hadoop.security.group.mapping.ldap.search.group.hierarchy.levels` is set to a positive value, then the total latency will be bounded by max(Recur Depth in LDAP, `hadoop.security.group.mapping.ldap.search.group.hierarchy.levels` ) * `hadoop.security.group.mapping.ldap.directory.search.timeout`.

`hadoop.security.group.mapping.ldap.base` configures how far to walk up the groups hierarchy when resolving groups.
By default, with a limit of 0, in order to be considered a member of a group, the user must be an explicit member in LDAP.  Otherwise, it will traverse the group hierarchy `hadoop.security.group.mapping.ldap.search.group.hierarchy.levels` levels up.


### Active Directory ###
The default configuration supports LDAP group name resolution with an Active Directory server.

### POSIX Groups ###
If the LDAP server supports POSIX group semantics (RFC-2307), Hadoop can perform LDAP group resolution queries to the server by setting both
`hadoop.security.group.mapping.ldap.search.filter.user` to  `(&amp;(objectClass=posixAccount)(uid={0}))` and
`hadoop.security.group.mapping.ldap.search.filter.group` to `(objectClass=posixGroup)`.

### SSL ###
To secure the connection, the implementation supports LDAP over SSL (LDAPS). SSL is enable by setting `hadoop.security.group.mapping.ldap.ssl` to `true`.
In addition, specify the path to the keystore file for SSL connection in `hadoop.security.group.mapping.ldap.ssl.keystore` and keystore password in `hadoop.security.group.mapping.ldap.ssl.keystore.password`, at the same time, make sure `hadoop.security.credential.clear-text-fallback` is true.
Alternatively, store the keystore password in a file, and point `hadoop.security.group.mapping.ldap.ssl.keystore.password.file` to that file.
For security purposes, this file should be readable only by the Unix user running the daemons, and for preventing recursive dependency, this file should be a local file.
The first approach aka using `hadoop.security.group.mapping.ldap.ssl.keystore.password` is highly discouraged because it exposes the password in the configuration file.

### Low latency group mapping resolution ###
Typically, Hadoop resolves a user's group names by making two LDAP queries: the first query gets the user object, and the second query uses the user's Distinguished Name to find the groups.
For some LDAP servers, such as Active Directory, the user object returned in the first query also contains the DN of the user's groups in its `memberOf` attribute, and the name of a group is its Relative Distinguished Name.
Therefore, it is possible to infer the user's groups from the first query without sending the second one, and it may reduce group name resolution latency incurred by the second query. If it fails to get group names, it will fall back to the typical two-query scenario and send the second query to get group names.
To enable this feature, set `hadoop.security.group.mapping.ldap.search.attr.memberof` to `memberOf`, and Hadoop will resolve group names using this attribute in the user object.

If the LDAP server's certificate is not signed by a well known certificate authority, specify the path to the truststore in `hadoop.security.group.mapping.ldap.ssl.truststore`.
Similar to keystore, specify the truststore password file in `hadoop.security.group.mapping.ldap.ssl.truststore.password.file`.

Composite Groups Mapping
--------
`CompositeGroupsMapping` works by enumerating a list of service providers in `hadoop.security.group.mapping.providers`.
It get groups from each of the providers in the list one after the other. If `hadoop.security.group.mapping.providers.combined` is `true`, merge the groups returned by all providers; otherwise, return the groups in the first successful provider.
See the following section for a sample configuration.

### Multiple group mapping providers configuration sample ###
  This sample illustrates a typical use case for `CompositeGroupsMapping` where
Hadoop authentication uses MIT Kerberos which trusts an AD realm. In this case, service
principals such as hdfs, mapred, hbase, hive, oozie and etc can be placed in MIT Kerberos,
but end users are just from the trusted AD. For the service principals, `ShellBasedUnixGroupsMapping`
provider can be used to query their groups for efficiency, and for end users, `LdapGroupsMapping`
provider can be used. This avoids to add group entries in AD for service principals when only using
`LdapGroupsMapping` provider.
  In case multiple ADs are involved and trusted by the MIT Kerberos, `LdapGroupsMapping`
provider can be used multiple times with different AD specific configurations. This sample also shows how
to do that. Here are the necessary configurations.

```<property>
  <name>hadoop.security.group.mapping</name>
  <value>org.apache.hadoop.security.CompositeGroupsMapping</value>
  <description>
    Class for user to group mapping (get groups for a given user) for ACL, which
    makes use of other multiple providers to provide the service.
  </description>
</property>

<property>
  <name>hadoop.security.group.mapping.providers</name>
  <value>shell4services,ad4usersX,ad4usersY</value>
  <description>
    Comma separated of names of other providers to provide user to group mapping.
  </description>
</property>

<property>
  <name>hadoop.security.group.mapping.providers.combined</name>
  <value>true</value>
  <description>
    true or false to indicate whether groups from the providers are combined or not. The default value is true
    If true, then all the providers will be tried to get groups and all the groups are combined to return as
    the final results. Otherwise, providers are tried one by one in the configured list order, and if any
    groups are retrieved from any provider, then the groups will be returned without trying the left ones.
  </description>
</property>

<property>
  <name>hadoop.security.group.mapping.provider.shell4services</name>
  <value>org.apache.hadoop.security.ShellBasedUnixGroupsMapping</value>
  <description>
    Class for group mapping provider named by 'shell4services'. The name can then be referenced
    by hadoop.security.group.mapping.providers property.
  </description>
</property>

<property>
  <name>hadoop.security.group.mapping.provider.ad4usersX</name>
  <value>org.apache.hadoop.security.LdapGroupsMapping</value>
  <description>
    Class for group mapping provider named by 'ad4usersX'. The name can then be referenced
    by hadoop.security.group.mapping.providers property.
  </description>
</property>

<property>
  <name>hadoop.security.group.mapping.provider.ad4usersY</name>
  <value>org.apache.hadoop.security.LdapGroupsMapping</value>
  <description>
    Class for group mapping provider named by 'ad4usersY'. The name can then be referenced
    by hadoop.security.group.mapping.providers property.
  </description>
</property>

<property>
<name>hadoop.security.group.mapping.provider.ad4usersX.ldap.url</name>
<value>ldap://ad-host-for-users-X:389</value>
  <description>
    ldap url for the provider named by 'ad4usersX'. Note this property comes from
    'hadoop.security.group.mapping.ldap.url'.
  </description>
</property>

<property>
<name>hadoop.security.group.mapping.provider.ad4usersY.ldap.url</name>
<value>ldap://ad-host-for-users-Y:389</value>
  <description>
    ldap url for the provider named by 'ad4usersY'. Note this property comes from
    'hadoop.security.group.mapping.ldap.url'.
  </description>
</property>
```

You also need to configure other properties like
  hadoop.security.group.mapping.ldap.bind.password.file and etc.
for ldap providers in the same way as above does.
