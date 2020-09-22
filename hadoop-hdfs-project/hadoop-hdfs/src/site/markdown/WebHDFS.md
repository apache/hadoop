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

WebHDFS REST API
================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Document Conventions
--------------------

| `Monospaced` | Used for commands, HTTP request and responses and code blocks. |
|:---- |:---- |
| `<Monospaced>` | User entered values. |
| `[Monospaced]` | Optional values. When the value is not specified, the default value is used. |
| *Italics* | Important phrases and words. |

Introduction
------------

The HTTP REST API supports the complete [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html)/[FileContext](../../api/org/apache/hadoop/fs/FileContext.html) interface for HDFS. The operations and the corresponding FileSystem/FileContext methods are shown in the next section. The Section [HTTP Query Parameter Dictionary](#HTTP_Query_Parameter_Dictionary) specifies the parameter details such as the defaults and the valid values.

### Operations

*   HTTP GET
    * [`OPEN`](#Open_and_Read_a_File) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).open)
    * [`GETFILESTATUS`](#Status_of_a_FileDirectory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getFileStatus)
    * [`LISTSTATUS`](#List_a_Directory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).listStatus)
    * [`LISTSTATUS_BATCH`](#Iteratively_List_a_Directory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).listStatusIterator)
    * [`GETCONTENTSUMMARY`](#Get_Content_Summary_of_a_Directory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getContentSummary)
    * [`GETQUOTAUSAGE`](#Get_Quota_Usage_of_a_Directory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getQuotaUsage)
    * [`GETFILECHECKSUM`](#Get_File_Checksum) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getFileChecksum)
    * [`GETHOMEDIRECTORY`](#Get_Home_Directory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getHomeDirectory)
    * [`GETDELEGATIONTOKEN`](#Get_Delegation_Token) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getDelegationToken)
    * [`GETTRASHROOT`](#Get_Trash_Root) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getTrashRoot)
    * [`GETXATTRS`](#Get_an_XAttr) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getXAttr)
    * [`GETXATTRS`](#Get_multiple_XAttrs) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getXAttrs)
    * [`GETXATTRS`](#Get_all_XAttrs) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getXAttrs)
    * [`LISTXATTRS`](#List_all_XAttrs) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).listXAttrs)
    * [`CHECKACCESS`](#Check_access) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).access)
    * [`GETALLSTORAGEPOLICY`](#Get_all_Storage_Policies) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getAllStoragePolicies)
    * [`GETSTORAGEPOLICY`](#Get_Storage_Policy) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getStoragePolicy)
    * [`GETSNAPSHOTDIFF`](#Get_Snapshot_Diff)
    * [`GETSNAPSHOTTABLEDIRECTORYLIST`](#Get_Snapshottable_Directory_List)
    * [`GETFILEBLOCKLOCATIONS`](#Get_File_Block_Locations) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getFileBlockLocations)
    * [`GETECPOLICY`](#Get_EC_Policy) (see [HDFSErasureCoding](./HDFSErasureCoding.html#Administrative_commands).getErasureCodingPolicy)
*   HTTP PUT
    * [`CREATE`](#Create_and_Write_to_a_File) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).create)
    * [`MKDIRS`](#Make_a_Directory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).mkdirs)
    * [`CREATESYMLINK`](#Create_a_Symbolic_Link) (see [FileContext](../../api/org/apache/hadoop/fs/FileContext.html).createSymlink)
    * [`RENAME`](#Rename_a_FileDirectory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).rename)
    * [`SETREPLICATION`](#Set_Replication_Factor) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setReplication)
    * [`SETOWNER`](#Set_Owner) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setOwner)
    * [`SETPERMISSION`](#Set_Permission) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setPermission)
    * [`SETTIMES`](#Set_Access_or_Modification_Time) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setTimes)
    * [`RENEWDELEGATIONTOKEN`](#Renew_Delegation_Token) (see [DelegationTokenAuthenticator](../../api/org/apache/hadoop/security/token/delegation/web/DelegationTokenAuthenticator.html).renewDelegationToken)
    * [`CANCELDELEGATIONTOKEN`](#Cancel_Delegation_Token) (see [DelegationTokenAuthenticator](../../api/org/apache/hadoop/security/token/delegation/web/DelegationTokenAuthenticator.html).cancelDelegationToken)
    * [`ALLOWSNAPSHOT`](#Allow_Snapshot)
    * [`DISALLOWSNAPSHOT`](#Disallow_Snapshot)
    * [`CREATESNAPSHOT`](#Create_Snapshot) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).createSnapshot)
    * [`RENAMESNAPSHOT`](#Rename_Snapshot) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).renameSnapshot)
    * [`SETXATTR`](#Set_XAttr) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setXAttr)
    * [`REMOVEXATTR`](#Remove_XAttr) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).removeXAttr)
    * [`SETSTORAGEPOLICY`](#Set_Storage_Policy) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setStoragePolicy)
    * [`SATISFYSTORAGEPOLICY`](#Satisfy_Storage_Policy) (see [ArchivalStorage](./ArchivalStorage.html#Satisfy_Storage_Policy).satisfyStoragePolicy)
    * [`ENABLEECPOLICY`](#Enable_EC_Policy) (see [HDFSErasureCoding](./HDFSErasureCoding.html#Administrative_commands).enablePolicy)
    * [`DISABLEECPOLICY`](#Disable_EC_Policy) (see [HDFSErasureCoding](./HDFSErasureCoding.html#Administrative_commands).disablePolicy)
    * [`SETECPOLICY`](#Set_EC_Policy) (see [HDFSErasureCoding](./HDFSErasureCoding.html#Administrative_commands).setErasureCodingPolicy)
*   HTTP POST
    * [`APPEND`](#Append_to_a_File) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).append)
    * [`CONCAT`](#Concat_Files) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).concat)
    * [`TRUNCATE`](#Truncate_a_File) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).truncate)
    * [`UNSETSTORAGEPOLICY`](#Unset_Storage_Policy) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).unsetStoragePolicy)
    * [`UNSETECPOLICY`](#Unset_EC_Policy) (see [HDFSErasureCoding](./HDFSErasureCoding.html#Administrative_commands).unsetErasureCodingPolicy)
*   HTTP DELETE
    * [`DELETE`](#Delete_a_FileDirectory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).delete)
    * [`DELETESNAPSHOT`](#Delete_Snapshot) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).deleteSnapshot)

### FileSystem URIs vs HTTP URLs

The FileSystem scheme of WebHDFS is "`webhdfs://`". A WebHDFS FileSystem URI has the following format.

      webhdfs://<HOST>:<HTTP_PORT>/<PATH>

The above WebHDFS URI corresponds to the below HDFS URI.

      hdfs://<HOST>:<RPC_PORT>/<PATH>

In the REST API, the prefix "`/webhdfs/v1`" is inserted in the path and a query is appended at the end. Therefore, the corresponding HTTP URL has the following format.

      http://<HOST>:<HTTP_PORT>/webhdfs/v1/<PATH>?op=...

**Note** that if WebHDFS is secured with SSL, then the scheme should be "`swebhdfs://`".

      swebhdfs://<HOST>:<HTTP_PORT>/<PATH>

See also: [SSL Configurations for SWebHDFS](#SSL_Configurations_for_SWebHDFS)

### HDFS Configuration Options

Below are the HDFS configuration options for WebHDFS.

| Property Name | Description |
|:---- |:---- |
| `dfs.web.authentication.kerberos.principal` | The HTTP Kerberos principal used by Hadoop-Auth in the HTTP endpoint. The HTTP Kerberos principal MUST start with 'HTTP/' per Kerberos HTTP SPNEGO specification. A value of "\*" will use all HTTP principals found in the keytab. |
| `dfs.web.authentication.kerberos.keytab ` | The Kerberos keytab file with the credentials for the HTTP Kerberos principal used by Hadoop-Auth in the HTTP endpoint. |
| `dfs.webhdfs.socket.connect-timeout` | How long to wait for a connection to be established before failing.  Specified as a time duration, ie numerical value followed by a units symbol, eg 2m for two minutes. Defaults to 60s. |
| `dfs.webhdfs.socket.read-timeout` | How long to wait for data to arrive before failing.  Defaults to 60s. |

Authentication
--------------

When security is *off*, the authenticated user is the username specified in the `user.name` query parameter. If the `user.name` parameter is not set, the server may either set the authenticated user to a default web user, if there is any, or return an error response.

When security is *on*, authentication is performed by either Hadoop delegation token or Kerberos SPNEGO. If a token is set in the `delegation` query parameter, the authenticated user is the user encoded in the token. If the `delegation` parameter is not set, the user is authenticated by Kerberos SPNEGO.

Below are examples using the `curl` command tool.

1.  Authentication when security is off:

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?[user.name=<USER>&]op=..."

2.  Authentication using Kerberos SPNEGO when security is on:

        curl -i --negotiate -u : "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=..."

3.  Authentication using Hadoop delegation token when security is on:

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?delegation=<TOKEN>&op=..."

See also: [Authentication for Hadoop HTTP web-consoles](../hadoop-common/HttpAuthentication.html)

Additionally, WebHDFS supports OAuth2 on the client side. The Namenode and Datanodes do not currently support clients using OAuth2 but other backends that implement the WebHDFS REST interface may.

WebHDFS supports two type of OAuth2 code grants (user-provided refresh and access token or user provided credential) by default and provides a pluggable mechanism for implementing other OAuth2 authentications per the [OAuth2 RFC](https://tools.ietf.org/html/rfc6749), or custom authentications.  When using either of the provided code grant mechanisms, the WebHDFS client will refresh the access token as necessary.

OAuth2 should only be enabled for clients not running with Kerberos SPENGO.

| OAuth2 code grant mechanism | Description | Value of `dfs.webhdfs.oauth2.access.token.provider` that implements code grant |
|:---- |:---- |:----|
| Authorization Code Grant | The user provides an initial access token and refresh token, which are then used to authenticate WebHDFS requests and obtain replacement access tokens, respectively. | org.apache.hadoop.hdfs.web.oauth2.ConfRefreshTokenBasedAccessTokenProvider |
| Client Credentials Grant | The user provides a credential which is used to obtain access tokens, which are then used to authenticate WebHDFS requests. | org.apache.hadoop.hdfs.web.oauth2.ConfCredentialBasedAccessTokenProvider |


The following properties control OAuth2 authentication.

| OAuth2 related property | Description |
|:---- |:---- |
| `dfs.webhdfs.oauth2.enabled` | Boolean to enable/disable OAuth2 authentication |
| `dfs.webhdfs.oauth2.access.token.provider` | Class name of an implementation of `org.apache.hadoop.hdfs.web.oauth.AccessTokenProvider.`  Two are provided with the code, as described above, or the user may specify a user-provided implementation. The default value for this configuration key is the `ConfCredentialBasedAccessTokenProvider` implementation. |
| `dfs.webhdfs.oauth2.client.id` | Client id used to obtain access token with either credential or refresh token |
| `dfs.webhdfs.oauth2.refresh.url` | URL against which to post for obtaining bearer token with either credential or refresh token |
| `dfs.webhdfs.oauth2.access.token` | (required if using ConfRefreshTokenBasedAccessTokenProvider) Initial access token with which to authenticate |
| `dfs.webhdfs.oauth2.refresh.token` | (required if using ConfRefreshTokenBasedAccessTokenProvider) Initial refresh token to use to obtain new access tokens  |
| `dfs.webhdfs.oauth2.refresh.token.expires.ms.since.epoch` | (required if using ConfRefreshTokenBasedAccessTokenProvider) Access token expiration measured in milliseconds since Jan 1, 1970.  *Note this is a different value than provided by OAuth providers and has been munged as described in interface to be suitable for a client application*  |
| `dfs.webhdfs.oauth2.credential` | (required if using ConfCredentialBasedAccessTokenProvider).  Credential used to obtain initial and subsequent access tokens. |

SSL Configurations for SWebHDFS
-------------------------------------------------------

To use SWebHDFS FileSystem (i.e. using the swebhdfs protocol), a SSL configuration
file needs to be specified on the client side. This must specify 3 parameters:

| SSL property | Description |
|:---- |:---- |
| `ssl.client.truststore.location` | The local-filesystem location of the trust-store file, containing the certificate for the NameNode. |
| `ssl.client.truststore.type` | (Optional) The format of the trust-store file. |
| `ssl.client.truststore.password` | (Optional) Password for the trust-store file. |

The following is an example SSL configuration file (**ssl-client.xml**):

```xml
<configuration>
  <property>
    <name>ssl.client.truststore.location</name>
    <value>/work/keystore.jks</value>
    <description>Truststore to be used by clients. Must be specified.</description>
  </property>

  <property>
    <name>ssl.client.truststore.password</name>
    <value>changeme</value>
    <description>Optional. Default value is "".</description>
  </property>

  <property>
    <name>ssl.client.truststore.type</name>
    <value>jks</value>
    <description>Optional. Default value is "jks".</description>
  </property>
</configuration>
```

The SSL configuration file must be in the class-path of the client program and the filename needs to be specified in **core-site.xml**:

```xml
<property>
  <name>hadoop.ssl.client.conf</name>
  <value>ssl-client.xml</value>
  <description>
    Resource file from which ssl client keystore information will be extracted.
    This file is looked up in the classpath, typically it should be in Hadoop
    conf/ directory. Default value is "ssl-client.xml".
  </description>
</property>
```

Proxy Users
-----------

When the proxy user feature is enabled, a proxy user *P* may submit a request on behalf of another user *U*. The username of *U* must be specified in the `doas` query parameter unless a delegation token is presented in authentication. In such case, the information of both users *P* and *U* must be encoded in the delegation token.

1.  A proxy request when security is off:

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?[user.name=<USER>&]doas=<USER>&op=..."

2.  A proxy request using Kerberos SPNEGO when security is on:

        curl -i --negotiate -u : "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?doas=<USER>&op=..."

3.  A proxy request using Hadoop delegation token when security is on:

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?delegation=<TOKEN>&op=..."

Cross-Site Request Forgery Prevention
-------------------------------------

WebHDFS supports an optional, configurable mechanism for cross-site request
forgery (CSRF) prevention.  When enabled, WebHDFS HTTP requests to the NameNode
or DataNode must include a custom HTTP header.  Configuration properties allow
adjusting which specific HTTP methods are protected and the name of the HTTP
header.  The value sent in the header is not relevant.  Only the presence of a
header by that name is required.

Enabling CSRF prevention also sets up the `WebHdfsFileSystem` class to send the
required header.  This ensures that CLI commands like
[`hdfs dfs`](./HDFSCommands.html#dfs) and
[`hadoop distcp`](../../hadoop-distcp/DistCp.html) continue to work correctly
when used with `webhdfs:` URIs.

Enabling CSRF prevention also sets up the NameNode web UI to send the required
header.  After enabling CSRF prevention and restarting the NameNode, existing
users of the NameNode web UI need to refresh the browser to reload the page and
find the new configuration.

The following properties control CSRF prevention.

| Property | Description | Default Value |
|:---- |:---- |:----
| `dfs.webhdfs.rest-csrf.enabled` | If true, then enables WebHDFS protection against cross-site request forgery (CSRF).  The WebHDFS client also uses this property to determine whether or not it needs to send the custom CSRF prevention header in its HTTP requests. | `false` |
| `dfs.webhdfs.rest-csrf.custom-header` | The name of a custom header that HTTP requests must send when protection against cross-site request forgery (CSRF) is enabled for WebHDFS by setting dfs.webhdfs.rest-csrf.enabled to true.  The WebHDFS client also uses this property to determine whether or not it needs to send the custom CSRF prevention header in its HTTP requests. | `X-XSRF-HEADER` |
| `dfs.webhdfs.rest-csrf.methods-to-ignore` | A comma-separated list of HTTP methods that do not require HTTP requests to include a custom header when protection against cross-site request forgery (CSRF) is enabled for WebHDFS by setting dfs.webhdfs.rest-csrf.enabled to true.  The WebHDFS client also uses this property to determine whether or not it needs to send the custom CSRF prevention header in its HTTP requests. | `GET,OPTIONS,HEAD,TRACE` |
| `dfs.webhdfs.rest-csrf.browser-useragents-regex` | A comma-separated list of regular expressions used to match against an HTTP request's User-Agent header when protection against cross-site request forgery (CSRF) is enabled for WebHDFS by setting dfs.webhdfs.reset-csrf.enabled to true.  If the incoming User-Agent matches any of these regular expressions, then the request is considered to be sent by a browser, and therefore CSRF prevention is enforced.  If the request's User-Agent does not match any of these regular expressions, then the request is considered to be sent by something other than a browser, such as scripted automation.  In this case, CSRF is not a potential attack vector, so the prevention is not enforced.  This helps achieve backwards-compatibility with existing automation that has not been updated to send the CSRF prevention header. | `^Mozilla.*,^Opera.*` |
| `dfs.datanode.httpserver.filter.handlers` | Comma separated list of Netty servlet-style filter handlers to inject into the Datanode WebHDFS I/O path | `org.apache.hadoop.hdfs.server.datanode.web.RestCsrfPreventionFilterHandler` |

The following is an example `curl` call that uses the `-H` option to include the
custom header in the request.

        curl -i -L -X PUT -H 'X-XSRF-HEADER: ""' 'http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE'

WebHDFS Retry Policy
-------------------------------------

WebHDFS supports an optional, configurable retry policy for resilient copy of
large files that could timeout, or copy file between HA clusters that could failover during the copy.

The following properties control WebHDFS retry and failover policy.

| Property | Description | Default Value |
|:---- |:---- |:----
| `dfs.http.client.retry.policy.enabled` | If "true", enable the retry policy of WebHDFS client. If "false", retry policy is turned off. | `false` |
| `dfs.http.client.retry.policy.spec` | Specify a policy of multiple linear random retry for WebHDFS client, e.g. given pairs of number of retries and sleep time (n0, t0), (n1, t1), ..., the first n0 retries sleep t0 milliseconds on average, the following n1 retries sleep t1 milliseconds on average, and so on. | `10000,6,60000,10` |
| `dfs.http.client.failover.max.attempts` | Specify the max number of failover attempts for WebHDFS client in case of network exception. | `15` |
| `dfs.http.client.retry.max.attempts` | Specify the max number of retry attempts for WebHDFS client, if the difference between retried attempts and failovered attempts is larger than the max number of retry attempts, there will be no more retries. | `10` |
| `dfs.http.client.failover.sleep.base.millis` | Specify the base amount of time in milliseconds upon which the exponentially increased sleep time between retries or failovers is calculated for WebHDFS client. | `500` |
| `dfs.http.client.failover.sleep.max.millis` | Specify the upper bound of sleep time in milliseconds between retries or failovers for WebHDFS client. | `15000` |

WebHDFS Request Filtering
-------------------------------------
One may control directionality of data in the WebHDFS protocol allowing only writing data from insecure networks. To enable, one must ensure `dfs.datanode.httpserver.filter.handlers` includes `org.apache.hadoop.hdfs.server.datanode.web.HostRestrictingAuthorizationFilterHandler`.  Configuration of the `HostRestrictingAuthorizationFilter` is controlled via the following properties.

| Property | Description | Default Value |
|:---- |:---- |:----
| `dfs.datanode.httpserver.filter.handlers` | Comma separated list of Netty servlet-style filter handlers to inject into the Datanode WebHDFS I/O path | `org.apache.hadoop.hdfs.server.datanode.web.RestCsrfPreventionFilterHandler` |
| `dfs.web.authentication.host.allow.rules` | Rules allowing users to read files in the format of _user_,_network/bits_,_path glob_ newline or `|`-separated. Use `*` for a wildcard of all _users_ or _network/bits_. | nothing - defaults to no one may read via WebHDFS |

File and Directory Operations
-----------------------------

### Create and Write to a File

* Step 1: Submit a HTTP PUT request without automatically following redirects and without sending the file data.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
                            [&overwrite=<true |false>][&blocksize=<LONG>][&replication=<SHORT>]
                            [&permission=<OCTAL>][&buffersize=<INT>][&noredirect=<true|false>]"

    Usually the request is redirected to a datanode where the file data is to be written.

        HTTP/1.1 307 TEMPORARY_REDIRECT
        Location: http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=CREATE...
        Content-Length: 0

    However, if you do not want to be automatically redirected, you can set the noredirect flag.

        HTTP/1.1 200 OK
        Content-Type: application/json
        {"Location":"http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=CREATE..."}

* Step 2: Submit another HTTP PUT request using the URL in the `Location` header (or the returned response in case you specified noredirect) with the file data to be written.

        curl -i -X PUT -T <LOCAL_FILE> "http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=CREATE..."

    The client receives a `201 Created` response with zero content length and the WebHDFS URI of the file in the `Location` header:

        HTTP/1.1 201 Created
        Location: webhdfs://<HOST>:<PORT>/<PATH>
        Content-Length: 0

    If no permissions are specified, the newly created file will be assigned with default 644 permission. No umask mode will be applied from server side (so "fs.permissions.umask-mode" value configuration set on Namenode side will have no effect).

**Note** that the reason of having two-step create/append is for preventing clients to send out data before the redirect. This issue is addressed by the "`Expect: 100-continue`" header in HTTP/1.1; see [RFC 2616, Section 8.2.3](http://www.w3.org/Protocols/rfc2616/rfc2616-sec8.html#sec8.2.3). Unfortunately, there are software library bugs (e.g. Jetty 6 HTTP server and Java 6 HTTP client), which do not correctly implement "`Expect: 100-continue`". The two-step create/append is a temporary workaround for the software library bugs.

See also: [`overwrite`](#Overwrite), [`blocksize`](#Block_Size), [`replication`](#Replication), [`permission`](#Permission), [`buffersize`](#Buffer_Size), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).create

### Append to a File

* Step 1: Submit a HTTP POST request without automatically following redirects and without sending the file data.

        curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>][&noredirect=<true|false>]"

    Usually the request is redirected to a datanode where the file data is to be appended:

        HTTP/1.1 307 TEMPORARY_REDIRECT
        Location: http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=APPEND...
        Content-Length: 0

   However, if you do not want to be automatically redirected, you can set the noredirect flag.

        HTTP/1.1 200 OK
        Content-Type: application/json
        {"Location":"http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=APPEND..."}


* Step 2: Submit another HTTP POST request using the URL in the `Location` header (or the returned response in case you specified noredirect) with the file data to be appended.

        curl -i -X POST -T <LOCAL_FILE> "http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=APPEND..."

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See the note in the previous section for the description of why this operation requires two steps.

See also: [`buffersize`](#Buffer_Size), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).append

### Concat File(s)

* Submit a HTTP POST request.

        curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CONCAT&sources=<PATHS>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [`sources`](#Sources), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).concat

### Open and Read a File

* Submit a HTTP GET request with automatically following redirects.

        curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
                            [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>][&noredirect=<true|false>]"

    Usually the request is redirected to a datanode where the file data can be read:

        HTTP/1.1 307 TEMPORARY_REDIRECT
        Location: http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=OPEN...
        Content-Length: 0

    However if you do not want to be automatically redirected, you can set the noredirect flag.

        HTTP/1.1 200 OK
        Content-Type: application/json
        {"Location":"http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=OPEN..."}

    The client follows the redirect to the datanode and receives the file data:

        HTTP/1.1 200 OK
        Content-Type: application/octet-stream
        Content-Length: 22

        Hello, webhdfs user!

See also: [`offset`](#Offset), [`length`](#Length), [`buffersize`](#Buffer_Size), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).open

### Make a Directory

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MKDIRS[&permission=<OCTAL>]"

    The client receives a response with a [`boolean` JSON object](#Boolean_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"boolean": true}

    If no permissions are specified, the newly created directory will have 755 permission as default. No umask mode will be applied from server side (so "fs.permissions.umask-mode" value configuration set on Namenode side will have no effect).

See also: [`permission`](#Permission), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).mkdirs

### Create a Symbolic Link

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATESYMLINK
                                      &destination=<PATH>[&createParent=<true |false>]"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [`destination`](#Destination), [`createParent`](#Create_Parent), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).createSymlink

### Rename a File/Directory

* Submit a HTTP PUT request.

        curl -i -X PUT "<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>"

    The client receives a response with a [`boolean` JSON object](#Boolean_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"boolean": true}

See also: [`destination`](#Destination), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).rename

### Delete a File/Directory

* Submit a HTTP DELETE request.

        curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
                                      [&recursive=<true |false>]"

    The client receives a response with a [`boolean` JSON object](#Boolean_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"boolean": true}

See also: [`recursive`](#Recursive), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).delete

### Truncate a File

* Submit a HTTP POST request.

        curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=TRUNCATE&newlength=<LONG>"

    The client receives a response with a [`boolean` JSON object](#Boolean_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked
        
        {"boolean": true}

See also: [`newlength`](#New_Length), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).truncate

### Status of a File/Directory

* Submit a HTTP GET request.

        curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS"

    The client receives a response with a [`FileStatus` JSON object](#FileStatus_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
          "FileStatus":
          {
            "accessTime"      : 0,
            "blockSize"       : 0,
            "group"           : "supergroup",
            "length"          : 0,             //in bytes, zero for directories
            "modificationTime": 1320173277227,
            "owner"           : "webuser",
            "pathSuffix"      : "",
            "permission"      : "777",
            "replication"     : 0,
            "snapshotEnabled" : true
            "type"            : "DIRECTORY"    //enum {FILE, DIRECTORY, SYMLINK}
          }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getFileStatus

### List a Directory

* Submit a HTTP GET request.

        curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"

    The client receives a response with a [`FileStatuses` JSON object](#FileStatuses_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Content-Length: 427

        {
          "FileStatuses":
          {
            "FileStatus":
            [
              {
                "accessTime"      : 1320171722771,
                "blockSize"       : 33554432,
                "childrenNum"     : 0,
                "fileId"          : 16388,
                "group"           : "supergroup",
                "length"          : 24930,
                "modificationTime": 1320171722771,
                "owner"           : "webuser",
                "pathSuffix"      : "a.patch",
                "permission"      : "644",
                "replication"     : 1,
                "storagePolicy"   : 0,
                "type"            : "FILE"
              },
              {
                "accessTime"      : 0,
                "blockSize"       : 0,
                "childrenNum"     : 0,
                "fileId"          : 16389,
                "group"           : "supergroup",
                "length"          : 0,
                "modificationTime": 1320895981256,
                "owner"           : "username",
                "pathSuffix"      : "bar",
                "permission"      : "711",
                "replication"     : 0,
                "snapshotEnabled" : true
                "type"            : "DIRECTORY"
              },
              ...
            ]
          }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).listStatus

### List a File

* Submit a HTTP GET request.

        curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"

    The client receives a response with a [`FileStatuses` JSON object](#FileStatuses_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Content-Length: 427

        {
          "FileStatuses":
          {
            "FileStatus":
            [
              {
                "accessTime"      : 1320171722771,
                "blockSize"       : 33554432,
                "childrenNum"     : 0,
                "fileId"          : 16390,
                "group"           : "supergroup",
                "length"          : 1366,
                "modificationTime": 1501770633062,
                "owner"           : "webuser",
                "pathSuffix"      : "",
                "permission"      : "644",
                "replication"     : 1,
                "storagePolicy"   : 0,
                "type"            : "FILE"
              }
            ]
          }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).listStatus


### Iteratively List a Directory

* Submit a HTTP GET request.

        curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS_BATCH&startAfter=<CHILD>"

    The client receives a response with a [`DirectoryListing` JSON object](#DirectoryListing_JSON_Schema), which contains a [`FileStatuses` JSON object](#FileStatuses_JSON_Schema), as well as iteration information:

        HTTP/1.1 200 OK
        Cache-Control: no-cache
        Expires: Thu, 08 Sep 2016 03:40:38 GMT
        Date: Thu, 08 Sep 2016 03:40:38 GMT
        Pragma: no-cache
        Expires: Thu, 08 Sep 2016 03:40:38 GMT
        Date: Thu, 08 Sep 2016 03:40:38 GMT
        Pragma: no-cache
        Content-Type: application/json
        X-FRAME-OPTIONS: SAMEORIGIN
        Transfer-Encoding: chunked
        Server: Jetty(6.1.26)

        {
            "DirectoryListing": {
                "partialListing": {
                    "FileStatuses": {
                        "FileStatus": [
                            {
                                "accessTime": 0,
                                "blockSize": 0,
                                "childrenNum": 0,
                                "fileId": 16387,
                                "group": "supergroup",
                                "length": 0,
                                "modificationTime": 1473305882563,
                                "owner": "andrew",
                                "pathSuffix": "bardir",
                                "permission": "755",
                                "replication": 0,
                                "storagePolicy": 0,
                                "type": "DIRECTORY"
                            },
                            {
                                "accessTime": 1473305896945,
                                "blockSize": 1024,
                                "childrenNum": 0,
                                "fileId": 16388,
                                "group": "supergroup",
                                "length": 0,
                                "modificationTime": 1473305896965,
                                "owner": "andrew",
                                "pathSuffix": "bazfile",
                                "permission": "644",
                                "replication": 3,
                                "storagePolicy": 0,
                                "type": "FILE"
                            }
                        ]
                    }
                },
                "remainingEntries": 2
            }
        }

If `remainingEntries` is non-zero, there are additional entries in the directory.
To query the next batch, set the `startAfter` parameter to the `pathSuffix` of the last item returned in the current batch. For example:

        curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS_BATCH&startAfter=bazfile"

Which will return the next batch of directory entries:

        HTTP/1.1 200 OK
        Cache-Control: no-cache
        Expires: Thu, 08 Sep 2016 03:43:20 GMT
        Date: Thu, 08 Sep 2016 03:43:20 GMT
        Pragma: no-cache
        Expires: Thu, 08 Sep 2016 03:43:20 GMT
        Date: Thu, 08 Sep 2016 03:43:20 GMT
        Pragma: no-cache
        Content-Type: application/json
        X-FRAME-OPTIONS: SAMEORIGIN
        Transfer-Encoding: chunked
        Server: Jetty(6.1.26)

        {
            "DirectoryListing": {
                "partialListing": {
                    "FileStatuses": {
                        "FileStatus": [
                            {
                                "accessTime": 0,
                                "blockSize": 0,
                                "childrenNum": 0,
                                "fileId": 16386,
                                "group": "supergroup",
                                "length": 0,
                                "modificationTime": 1473305878951,
                                "owner": "andrew",
                                "pathSuffix": "foodir",
                                "permission": "755",
                                "replication": 0,
                                "storagePolicy": 0,
                                "type": "DIRECTORY"
                            },
                            {
                                "accessTime": 1473305902864,
                                "blockSize": 1024,
                                "childrenNum": 0,
                                "fileId": 16389,
                                "group": "supergroup",
                                "length": 0,
                                "modificationTime": 1473305902878,
                                "owner": "andrew",
                                "pathSuffix": "quxfile",
                                "permission": "644",
                                "replication": 3,
                                "storagePolicy": 0,
                                "type": "FILE"
                            }
                        ]
                    }
                },
                "remainingEntries": 0
            }
        }

Batch size is controlled by the `dfs.ls.limit` option on the NameNode.

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).listStatusIterator

Other File System Operations
----------------------------

### Get Content Summary of a Directory

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETCONTENTSUMMARY"

    The client receives a response with a [`ContentSummary` JSON object](#ContentSummary_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
          "ContentSummary":
          {
            "directoryCount": 2,
            "ecPolicy"      : "RS-6-3-1024k",
            "fileCount"     : 1,
            "length"        : 24930,
            "quota"         : -1,
            "spaceConsumed" : 24930,
            "spaceQuota"    : -1,
            "typeQuota":
            {
              "ARCHIVE":
              {
                "consumed": 500,
                "quota": 10000
              },
              "DISK":
              {
                "consumed": 500,
                "quota": 10000
              },
              "SSD":
              {
                "consumed": 500,
                "quota": 10000
              }
            }
          }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getContentSummary

### Get Quota Usage of a Directory

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETQUOTAUSAGE"

    The client receives a response with a [`QuotaUsage` JSON object](#QuotaUsage_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
          "QuotaUsage":
          {
            "fileAndDirectoryCount": 1,
            "quota"         : 100,
            "spaceConsumed" : 24930,
            "spaceQuota"    : 100000,
            "typeQuota":
            {
              "ARCHIVE":
              {
                "consumed": 500,
                "quota": 10000
              },
              "DISK":
              {
                "consumed": 500,
                "quota": 10000
              },
              "SSD":
              {
                "consumed": 500,
                "quota": 10000
              }
            }
          }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getQuotaUsage

### Set Quota

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETQUOTA
                                      &namespacequota=<QUOTA>[&storagespacequota=<QUOTA>]"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setQuota

### Set Quota By Storage Type

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETQUOTABYSTORAGETYPE
                                      &storagetype=<STORAGETYPE>&storagespacequota=<QUOTA>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setQuotaByStorageType

### Get File Checksum

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM"

    Usually the request is redirected to a datanode:

        HTTP/1.1 307 TEMPORARY_REDIRECT
        Location: http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM...
        Content-Length: 0

    However, if you do not want to be automatically redirected, you can set the noredirect flag.

        HTTP/1.1 200 OK
        Content-Type: application/json
        {"Location":"http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM..."}


    The client follows the redirect to the datanode and receives a [`FileChecksum` JSON object](#FileChecksum_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
          "FileChecksum":
          {
            "algorithm": "MD5-of-1MD5-of-512CRC32",
            "bytes"    : "eadb10de24aa315748930df6e185c0d ...",
            "length"   : 28
          }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getFileChecksum

### Get Home Directory

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/?op=GETHOMEDIRECTORY"

    The client receives a response with a [`Path` JSON object](#Path_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"Path": "/user/username"}

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getHomeDirectory

### Get Trash Root

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETTRASHROOT"

    The client receives a response with a [`Path` JSON object](#Path_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"Path": "/user/username/.Trash"}

    if the path is an encrypted zone path and user has permission of the path, the client receives a response like this:

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"Path": "/PATH/.Trash/username"}

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getTrashRoot

For more details about trash root in an encrypted zone, please refer to [Transparent Encryption Guide](./TransparentEncryption.html#Rename_and_Trash_considerations).

### Set Permission

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETPERMISSION
                                      [&permission=<OCTAL>]"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [`permission`](#Permission), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setPermission

### Set Owner

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETOWNER
                                      [&owner=<USER>][&group=<GROUP>]"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [`owner`](#Owner), [`group`](#Group), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setOwner

### Set Replication Factor

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETREPLICATION
                                      [&replication=<SHORT>]"

    The client receives a response with a [`boolean` JSON object](#Boolean_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"boolean": true}

See also: [`replication`](#Replication), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setReplication

### Set Access or Modification Time

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETTIMES
                                      [&modificationtime=<TIME>][&accesstime=<TIME>]"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [`modificationtime`](#Modification_Time), [`accesstime`](#Access_Time), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setTimes

### Modify ACL Entries

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MODIFYACLENTRIES
                                      &aclspec=<ACLSPEC>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).modifyAclEntries

### Remove ACL Entries

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=REMOVEACLENTRIES
                                      &aclspec=<ACLSPEC>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).removeAclEntries

### Remove Default ACL

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=REMOVEDEFAULTACL"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).removeDefaultAcl

### Remove ACL

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=REMOVEACL"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).removeAcl

### Set ACL

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETACL
                                      &aclspec=<ACLSPEC>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setAcl

### Get ACL Status

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETACLSTATUS"

    The client receives a response with a [`AclStatus` JSON object](#ACL_Status_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
            "AclStatus": {
                "entries": [
                    "user:carla:rw-", 
                    "group::r-x"
                ], 
                "group": "supergroup", 
                "owner": "hadoop", 
                "permission":"775",
                "stickyBit": false
            }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getAclStatus

### Check access

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CHECKACCESS
                                      &fsaction=<FSACTION>

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).access

Storage Policy Operations
-------------------------

### Get all Storage Policies

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1?op=GETALLSTORAGEPOLICY"

    The client receives a response with a [`BlockStoragePolicies` JSON object](#BlockStoragePolicies_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
            "BlockStoragePolicies": {
                "BlockStoragePolicy": [
                   {
                       "copyOnCreateFile": false,
                       "creationFallbacks": [],
                       "id": 2,
                       "name": "COLD",
                       "replicationFallbacks": [],
                       "storageTypes": ["ARCHIVE"]
                   },
                   {
                       "copyOnCreateFile": false,
                       "creationFallbacks": ["DISK","ARCHIVE"],
                       "id": 5,
                       "name": "WARM",
                       "replicationFallbacks": ["DISK","ARCHIVE"],
                       "storageTypes": ["DISK","ARCHIVE"]
                   },
                   {
                       "copyOnCreateFile": false,
                       "creationFallbacks": [],
                       "id": 7,
                       "name": "HOT",
                       "replicationFallbacks": ["ARCHIVE"],
                       "storageTypes": ["DISK"]
                   },
                   {
                       "copyOnCreateFile": false,
                       "creationFallbacks": ["SSD","DISK"],
                       "id": 10,"name": "ONE_SSD",
                       "replicationFallbacks": ["SSD","DISK"],
                       "storageTypes": ["SSD","DISK"]
                   },
                   {
                       "copyOnCreateFile": false,
                       "creationFallbacks": ["DISK"],
                       "id": 12,
                       "name": "ALL_SSD",
                       "replicationFallbacks": ["DISK"],
                       "storageTypes": ["SSD"]
                   },
                   {
                       "copyOnCreateFile": true,
                       "creationFallbacks": ["DISK"],
                       "id": 15,
                       "name": "LAZY_PERSIST",
                       "replicationFallbacks": ["DISK"],
                       "storageTypes": ["RAM_DISK","DISK"]
                   }
               ]
           }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getAllStoragePolicies

### Set Storage Policy

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETSTORAGEPOLICY
                                      &storagepolicy=<policy>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setStoragePolicy

### Unset Storage Policy

* Submit a HTTP POT request.

        curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=UNSETSTORAGEPOLICY"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).unsetStoragePolicy

### Get Storage Policy

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETSTORAGEPOLICY"

    The client receives a response with a [`BlockStoragePolicy` JSON object](#BlockStoragePolicy_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
            "BlockStoragePolicy": {
                "copyOnCreateFile": false,
               "creationFallbacks": [],
                "id":7,
                "name":"HOT",
                "replicationFallbacks":["ARCHIVE"],
                "storageTypes":["DISK"]
            }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getStoragePolicy

### Satisfy Storage Policy

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SATISFYSTORAGEPOLICY"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [ArchivalStorage](./ArchivalStorage.html#Satisfy_Storage_Policy).satisfyStoragePolicy

### Get File Block Locations

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILEBLOCKLOCATIONS

    The client receives a response with a [`BlockLocations` JSON Object](#Block_Locations_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
          "BlockLocations" :
          {
            "BlockLocation":
            [
              {
                "cachedHosts" : [],
                "corrupt" : false,
                "hosts" : ["host"],
                "length" : 134217728,                             // length of this block
                "names" : ["host:ip"],
                "offset" : 0,                                     // offset of the block in the file
                "storageTypes" : ["DISK"],                        // enum {RAM_DISK, SSD, DISK, ARCHIVE}
                "topologyPaths" : ["/default-rack/hostname:ip"]
              }, {
                "cachedHosts" : [],
                "corrupt" : false,
                "hosts" : ["host"],
                "length" : 62599364,
                "names" : ["host:ip"],
                "offset" : 134217728,
                "storageTypes" : ["DISK"],
                "topologyPaths" : ["/default-rack/hostname:ip"]
              },
              ...
            ]
          }
        }

See also: [`offset`](#Offset), [`length`](#Length), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getFileBlockLocations

Extended Attributes(XAttrs) Operations
--------------------------------------

### Set XAttr

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETXATTR
                                      &xattr.name=<XATTRNAME>&xattr.value=<XATTRVALUE>
                                      &flag=<FLAG>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setXAttr

### Remove XAttr

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=REMOVEXATTR
                                      &xattr.name=<XATTRNAME>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).removeXAttr

### Get an XAttr

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETXATTRS
                                      &xattr.name=<XATTRNAME>&encoding=<ENCODING>"

    The client receives a response with a [`XAttrs` JSON object](#XAttrs_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
            "XAttrs": [
                {
                    "name":"XATTRNAME",
                    "value":"XATTRVALUE"
                }
            ]
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getXAttr

### Get multiple XAttrs

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETXATTRS
                                      &xattr.name=<XATTRNAME1>&xattr.name=<XATTRNAME2>
                                      &encoding=<ENCODING>"

    The client receives a response with a [`XAttrs` JSON object](#XAttrs_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
            "XAttrs": [
                {
                    "name":"XATTRNAME1",
                    "value":"XATTRVALUE1"
                },
                {
                    "name":"XATTRNAME2",
                    "value":"XATTRVALUE2"
                }
            ]
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getXAttrs

### Get all XAttrs

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETXATTRS
                                      &encoding=<ENCODING>"

    The client receives a response with a [`XAttrs` JSON object](#XAttrs_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
            "XAttrs": [
                {
                    "name":"XATTRNAME1",
                    "value":"XATTRVALUE1"
                },
                {
                    "name":"XATTRNAME2",
                    "value":"XATTRVALUE2"
                },
                {
                    "name":"XATTRNAME3",
                    "value":"XATTRVALUE3"
                }
            ]
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getXAttrs

### List all XAttrs

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTXATTRS"

    The client receives a response with a [`XAttrNames` JSON object](#XAttrNames_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
            "XAttrNames":"[\"XATTRNAME1\",\"XATTRNAME2\",\"XATTRNAME3\"]"
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).listXAttrs

Erasure Coding Operations
-------------------------

### Enable EC Policy

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/?op=ENABLEECPOLICY
                                      &ecpolicy=<policy>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [HDFSErasureCoding](./HDFSErasureCoding.html#Administrative_commands).enablePolicy

### Disable EC Policy

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/?op=DISABLEECPOLICY
                                      &ecpolicy=<policy>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [HDFSErasureCoding](./HDFSErasureCoding.html#Administrative_commands).disablePolicy

### Set EC Policy

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETECPOLICY
                                      &ecpolicy=<policy>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [HDFSErasureCoding](./HDFSErasureCoding.html#Administrative_commands).setErasureCodingPolicy

### Get EC Policy

* Submit a HTTP GET request.

        curl -i -X GET "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETECPOLICY
                                     "

   The client receives a response with a [`ECPolicy` JSON object](#ECPolicy_JSON_Schema):


        {
            "name": "RS-10-4-1024k",
            "schema":
            {
            "codecName": "rs",
            "numDataUnits": 10,
            "numParityUnits": 4,
            "extraOptions": {}
            }
            "cellSize": 1048576,
            "id":5,
            "codecname":"rs",
            "numDataUnits": 10,
            "numParityUnits": 4,
            "replicationpolicy":false,
            "systemPolicy":true

        }



See also: [HDFSErasureCoding](./HDFSErasureCoding.html#Administrative_commands).getErasureCodingPolicy

### Unset EC Policy

* Submit a HTTP POST request.

        curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=UNSETECPOLICY
                                     "

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [HDFSErasureCoding](./HDFSErasureCoding.html#Administrative_commands).unsetErasureCodingPolicy

Snapshot Operations
-------------------

### Allow Snapshot

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=ALLOWSNAPSHOT"

    The client receives a response with zero content length on success:

        HTTP/1.1 200 OK
        Content-Length: 0

### Disallow Snapshot

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=DISALLOWSNAPSHOT"

    The client receives a response with zero content length on success:

        HTTP/1.1 200 OK
        Content-Length: 0

### Create Snapshot

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATESNAPSHOT[&snapshotname=<SNAPSHOTNAME>]"

    The client receives a response with a [`Path` JSON object](#Path_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"Path": "/user/username/.snapshot/s1"}

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).createSnapshot

### Delete Snapshot

* Submit a HTTP DELETE request.

        curl -i -X DELETE "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=DELETESNAPSHOT&snapshotname=<SNAPSHOTNAME>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).deleteSnapshot

### Rename Snapshot

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAMESNAPSHOT
                           &oldsnapshotname=<SNAPSHOTNAME>&snapshotname=<SNAPSHOTNAME>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).renameSnapshot

### Get Snapshot Diff

* Submit a HTTP GET request.

        curl -i GET "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETSNAPSHOTDIFF
                           &oldsnapshotname=<SNAPSHOTNAME>&snapshotname=<SNAPSHOTNAME>"

    The client receives a response with a [`SnapshotDiffReport` JSON object](#SnapshotDiffReport_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"SnapshotDiffReport":{"diffList":[],"fromSnapshot":"s3","snapshotRoot":"/foo","toSnapshot":"s4"}}

### Get Snapshottable Directory List

* Submit a HTTP GET request.

        curl -i GET "http://<HOST>:<PORT>/webhdfs/v1/?user.name=<USER>&op=GETSNAPSHOTTABLEDIRECTORYLIST"

    If the USER is not the hdfs super user, the call lists only the snapshottable directories owned by the user. If the USER is the hdfs super user, the call lists all the snapshottable directories. The client receives a response with a [`SnapshottableDirectoryList` JSON object](#SnapshottableDirectoryList_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
            "SnapshottableDirectoryList":
            [
                {
                  "dirStatus":
                    {
                        "accessTime":0,
                        "blockSize":0,
                        "childrenNum":0,
                        "fileId":16386,
                        "group":"hadoop",
                        "length":0,
                        "modificationTime":1520761889225,
                        "owner":"random",
                        "pathSuffix":"bar",
                        "permission":"755",
                        "replication":0,
                        "storagePolicy":0,
                        "type":"DIRECTORY"
                    },
                  "parentFullPath":"/",
                  "snapshotNumber":0,
                  "snapshotQuota":65536
                }
            ]
        }

Delegation Token Operations
---------------------------

### Get Delegation Token

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/?op=GETDELEGATIONTOKEN
                    [&renewer=<USER>][&service=<SERVICE>][&kind=<KIND>]"

    The client receives a response with a [`Token` JSON object](#Token_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
          "Token":
          {
            "urlString": "JQAIaG9y..."
          }
        }

See also: [`renewer`](#Renewer), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getDelegationToken, [`kind`](#Token_Kind), [`service`](#Token_Service)

### Renew Delegation Token

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/?op=RENEWDELEGATIONTOKEN&token=<TOKEN>"

    The client receives a response with a [`long` JSON object](#Long_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"long": 1320962673997}           //the new expiration time

See also: [`token`](#Token), [DelegationTokenAuthenticator](../../api/org/apache/hadoop/security/token/delegation/web/DelegationTokenAuthenticator.html).renewDelegationToken

### Cancel Delegation Token

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/?op=CANCELDELEGATIONTOKEN&token=<TOKEN>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [`token`](#Token), [DelegationTokenAuthenticator](../../api/org/apache/hadoop/security/token/delegation/web/DelegationTokenAuthenticator.html).cancelDelegationToken

Error Responses
---------------

When an operation fails, the server may throw an exception. The JSON schema of error responses is defined in [RemoteException JSON Schema](#RemoteException_JSON_Schema). The table below shows the mapping from exceptions to HTTP response codes.

### HTTP Response Codes

| Exceptions | HTTP Response Codes |
|:---- |:---- |
| `IllegalArgumentException ` | `400 Bad Request ` |
| `UnsupportedOperationException` | `400 Bad Request ` |
| `SecurityException ` | `401 Unauthorized ` |
| `IOException ` | `403 Forbidden ` |
| `FileNotFoundException ` | `404 Not Found ` |
| `RuntimeException ` | `500 Internal Server Error` |

Below are examples of exception responses.

#### Illegal Argument Exception

    HTTP/1.1 400 Bad Request
    Content-Type: application/json
    Transfer-Encoding: chunked

    {
      "RemoteException":
      {
        "exception"    : "IllegalArgumentException",
        "javaClassName": "java.lang.IllegalArgumentException",
        "message"      : "Invalid value for webhdfs parameter \"permission\": ..."
      }
    }

#### Security Exception

    HTTP/1.1 401 Unauthorized
    Content-Type: application/json
    Transfer-Encoding: chunked

    {
      "RemoteException":
      {
        "exception"    : "SecurityException",
        "javaClassName": "java.lang.SecurityException",
        "message"      : "Failed to obtain user group information: ..."
      }
    }

#### Access Control Exception

    HTTP/1.1 403 Forbidden
    Content-Type: application/json
    Transfer-Encoding: chunked

    {
      "RemoteException":
      {
        "exception"    : "AccessControlException",
        "javaClassName": "org.apache.hadoop.security.AccessControlException",
        "message"      : "Permission denied: ..."
      }
    }

#### File Not Found Exception

    HTTP/1.1 404 Not Found
    Content-Type: application/json
    Transfer-Encoding: chunked

    {
      "RemoteException":
      {
        "exception"    : "FileNotFoundException",
        "javaClassName": "java.io.FileNotFoundException",
        "message"      : "File does not exist: /foo/a.patch"
      }
    }

JSON Schemas
------------

All operations, except for [`OPEN`](#Open_and_Read_a_File), either return a zero-length response or a JSON response. For [`OPEN`](#Open_and_Read_a_File), the response is an octet-stream. The JSON schemas are shown below. See [draft-zyp-json-schema-03](http://tools.ietf.org/id/draft-zyp-json-schema-03.html) for the syntax definitions of the JSON schemas.

**Note** that the default value of [`additionalProperties`](http://tools.ietf.org/id/draft-zyp-json-schema-03.html#additionalProperties) is an empty schema which allows any value for additional properties. Therefore, all WebHDFS JSON responses allow any additional property. However, if additional properties are included in the responses, they are considered as optional properties in order to maintain compatibility.

### ACL Status JSON Schema

```json
{
  "name"      : "AclStatus",
  "properties":
  {
    "AclStatus":
    {
      "type"      : "object",
      "properties":
      {
        "entries":
        {
          "type": "array",
          "items":
          {
            "description": "ACL entry.",
            "type": "string"
          }
        },
        "group":
        {
          "description": "The group owner.",
          "type"       : "string",
          "required"   : true
        },
        "owner":
        {
          "description": "The user who is the owner.",
          "type"       : "string",
          "required"   : true
        },
        "stickyBit":
        {
          "description": "True if the sticky bit is on.",
          "type"       : "boolean",
          "required"   : true
        }
      }
    }
  }
}
```

### XAttrs JSON Schema

```json
{
  "name"      : "XAttrs",
  "properties":
  {
    "XAttrs":
    {
      "type"      : "array",
      "items":
      {
        "type"    : "object",
        "properties":
        {
          "name":
          {
            "description": "XAttr name.",
            "type"       : "string",
            "required"   : true
          },
          "value":
          {
            "description": "XAttr value.",
            "type"       : "string"
          }
        }
      }
    }
  }
}
```

### XAttrNames JSON Schema

```json
{
  "name"      : "XAttrNames",
  "properties":
  {
    "XAttrNames":
    {
      "description": "XAttr names.",
      "type"       : "string",
      "required"   : true
    }
  }
}
```

### Boolean JSON Schema

```json
{
  "name"      : "boolean",
  "properties":
  {
    "boolean":
    {
      "description": "A boolean value",
      "type"       : "boolean",
      "required"   : true
    }
  }
}
```

See also: [`MKDIRS`](#Make_a_Directory), [`RENAME`](#Rename_a_FileDirectory), [`DELETE`](#Delete_a_FileDirectory), [`SETREPLICATION`](#Set_Replication_Factor)

### ContentSummary JSON Schema

```json
{
  "name"      : "ContentSummary",
  "properties":
  {
    "ContentSummary":
    {
      "type"      : "object",
      "properties":
      {
        "directoryCount":
        {
          "description": "The number of directories.",
          "type"       : "integer",
          "required"   : true
        },
        "fileCount":
        {
          "description": "The number of files.",
          "type"       : "integer",
          "required"   : true
        },
        "length":
        {
          "description": "The number of bytes used by the content.",
          "type"       : "integer",
          "required"   : true
        },
        "quota":
        {
          "description": "The namespace quota of this directory.",
          "type"       : "integer",
          "required"   : true
        },
        "spaceConsumed":
        {
          "description": "The disk space consumed by the content.",
          "type"       : "integer",
          "required"   : true
        },
        "spaceQuota":
        {
          "description": "The disk space quota.",
          "type"       : "integer",
          "required"   : true
        },
        "typeQuota":
        {
          "type"      : "object",
          "properties":
          {
            "ARCHIVE":
            {
              "type"      : "object",
              "properties":
              {
                "consumed":
                {
                  "description": "The storage type space consumed.",
                  "type"       : "integer",
                  "required"   : true
                },
                "quota":
                {
                  "description": "The storage type quota.",
                  "type"       : "integer",
                  "required"   : true
                }
              }
            },
            "DISK":
            {
              "type"      : "object",
              "properties":
              {
                "consumed":
                {
                  "description": "The storage type space consumed.",
                  "type"       : "integer",
                  "required"   : true
                },
                "quota":
                {
                  "description": "The storage type quota.",
                  "type"       : "integer",
                  "required"   : true
                }
              }
            },
            "SSD":
            {
              "type"      : "object",
              "properties":
              {
                "consumed":
                {
                  "description": "The storage type space consumed.",
                  "type"       : "integer",
                  "required"   : true
                },
                "quota":
                {
                  "description": "The storage type quota.",
                  "type"       : "integer",
                  "required"   : true
                }
              }
            }
          }
        }
      }
    }
  }
}
```

See also: [`GETCONTENTSUMMARY`](#Get_Content_Summary_of_a_Directory)

### QuotaUsage JSON Schema

```json
{
  "name"      : "QuotaUsage",
  "properties":
  {
    "QuotaUsage":
    {
      "type"      : "object",
      "properties":
      {
        "fileAndDirectoryCount":
        {
          "description": "The number of files and directories.",
          "type"       : "integer",
          "required"   : true
        },
        "quota":
        {
          "description": "The namespace quota of this directory.",
          "type"       : "integer",
          "required"   : true
        },
        "spaceConsumed":
        {
          "description": "The disk space consumed by the content.",
          "type"       : "integer",
          "required"   : true
        },
        "spaceQuota":
        {
          "description": "The disk space quota.",
          "type"       : "integer",
          "required"   : true
        },
        "typeQuota":
        {
          "type"      : "object",
          "properties":
          {
            "ARCHIVE":
            {
              "type"      : "object",
              "properties":
              {
                "consumed":
                {
                  "description": "The storage type space consumed.",
                  "type"       : "integer",
                  "required"   : true
                },
                "quota":
                {
                  "description": "The storage type quota.",
                  "type"       : "integer",
                  "required"   : true
                }
              }
            },
            "DISK":
            {
              "type"      : "object",
              "properties":
              {
                "consumed":
                {
                  "description": "The storage type space consumed.",
                  "type"       : "integer",
                  "required"   : true
                },
                "quota":
                {
                  "description": "The storage type quota.",
                  "type"       : "integer",
                  "required"   : true
                }
              }
            },
            "SSD":
            {
              "type"      : "object",
              "properties":
              {
                "consumed":
                {
                  "description": "The storage type space consumed.",
                  "type"       : "integer",
                  "required"   : true
                },
                "quota":
                {
                  "description": "The storage type quota.",
                  "type"       : "integer",
                  "required"   : true
                }
              }
            }
          }
        }
      }
    }
  }
}
```

See also: [`GETQUOTAUSAGE`](#Get_Quota_Usage_of_a_Directory)

### FileChecksum JSON Schema


```json
{
  "name"      : "FileChecksum",
  "properties":
  {
    "FileChecksum":
    {
      "type"      : "object",
      "properties":
      {
        "algorithm":
        {
          "description": "The name of the checksum algorithm.",
          "type"       : "string",
          "required"   : true
        },
        "bytes":
        {
          "description": "The byte sequence of the checksum in hexadecimal.",
          "type"       : "string",
          "required"   : true
        },
        "length":
        {
          "description": "The length of the bytes (not the length of the string).",
          "type"       : "integer",
          "required"   : true
        }
      }
    }
  }
}
```

### FileStatus JSON Schema

```json
{
  "name"      : "FileStatus",
  "properties":
  {
    "FileStatus": fileStatusProperties      //See FileStatus Properties
  }
}
```

See also: [`FileStatus` Properties](#FileStatus_Properties), [`GETFILESTATUS`](#Status_of_a_FileDirectory), [FileStatus](../../api/org/apache/hadoop/fs/FileStatus.html)

#### FileStatus Properties

JavaScript syntax is used to define `fileStatusProperties` so that it can be referred in both `FileStatus` and `FileStatuses` JSON schemas.

```javascript
var fileStatusProperties =
{
  "type"      : "object",
  "properties":
  {
    "accessTime":
    {
      "description": "The access time.",
      "type"       : "integer",
      "required"   : true
    },
    "blockSize":
    {
      "description": "The block size of a file.",
      "type"       : "integer",
      "required"   : true
    },
    "group":
    {
      "description": "The group owner.",
      "type"       : "string",
      "required"   : true
    },
    "length":
    {
      "description": "The number of bytes in a file.",
      "type"       : "integer",
      "required"   : true
    },
    "modificationTime":
    {
      "description": "The modification time.",
      "type"       : "integer",
      "required"   : true
    },
    "owner":
    {
      "description": "The user who is the owner.",
      "type"       : "string",
      "required"   : true
    },
    "pathSuffix":
    {
      "description": "The path suffix.",
      "type"       : "string",
      "required"   : true
    },
    "permission":
    {
      "description": "The permission represented as a octal string.",
      "type"       : "string",
      "required"   : true
    },
    "replication":
    {
      "description": "The number of replication of a file.",
      "type"       : "integer",
      "required"   : true
    },
   "symlink":                                         //an optional property
    {
      "description": "The link target of a symlink.",
      "type"       : "string"
    },
   "type":
    {
      "description": "The type of the path object.",
      "enum"       : ["FILE", "DIRECTORY", "SYMLINK"],
      "required"   : true
    }
  }
};
```

### FileStatuses JSON Schema

A `FileStatuses` JSON object represents an array of `FileStatus` JSON objects.

```json
{
  "name"      : "FileStatuses",
  "properties":
  {
    "FileStatuses":
    {
      "type"      : "object",
      "properties":
      {
        "FileStatus":
        {
          "description": "An array of FileStatus",
          "type"       : "array",
          "items"      : fileStatusProperties      //See FileStatus Properties
        }
      }
    }
  }
}
```

See also: [`FileStatus` Properties](#FileStatus_Properties), [`LISTSTATUS`](#List_a_Directory), [FileStatus](../../api/org/apache/hadoop/fs/FileStatus.html)

### DirectoryListing JSON Schema

A `DirectoryListing` JSON object represents a batch of directory entries while iteratively listing a directory. It contains a `FileStatuses` JSON object as well as iteration information.

```json
{
  "name"      : "DirectoryListing",
  "properties":
  {
    "DirectoryListing":
    {
      "type"      : "object",
      "properties":
      {
        "partialListing":
        {
          "description": "A partial directory listing",
          "type"       : "object", // A FileStatuses object
          "required"   : true
        },
        "remainingEntries":
        {
          "description": "Number of remaining entries",
          "type"       : "integer",
          "required"   : true
        }
      }
    }
  }

}
```

See also: [`FileStatuses` JSON Schema](#FileStatuses_JSON_Schema), [`LISTSTATUS_BATCH`](#Iteratively_List_a_Directory), [FileStatus](../../api/org/apache/hadoop/fs/FileStatus.html)

### Long JSON Schema

```json
{
  "name"      : "long",
  "properties":
  {
    "long":
    {
      "description": "A long integer value",
      "type"       : "integer",
      "required"   : true
    }
  }
}
```

See also: [`RENEWDELEGATIONTOKEN`](#Renew_Delegation_Token),

### Path JSON Schema

```json
{
  "name"      : "Path",
  "properties":
  {
    "Path":
    {
      "description": "The string representation a Path.",
      "type"       : "string",
      "required"   : true
    }
  }
}
```

See also: [`GETHOMEDIRECTORY`](#Get_Home_Directory), [Path](../../api/org/apache/hadoop/fs/Path.html)

### RemoteException JSON Schema

```json
{
  "name"      : "RemoteException",
  "properties":
  {
    "RemoteException":
    {
      "type"      : "object",
      "properties":
      {
        "exception":
        {
          "description": "Name of the exception",
          "type"       : "string",
          "required"   : true
        },
        "message":
        {
          "description": "Exception message",
          "type"       : "string",
          "required"   : true
        },
        "javaClassName":                                     //an optional property
        {
          "description": "Java class name of the exception",
          "type"       : "string"
        }
      }
    }
  }
}
```
See also: [Error Responses](#Error_Responses)

### Token JSON Schema

```json
{
  "name"      : "Token",
  "properties":
  {
    "Token": tokenProperties      //See Token Properties
  }
}
```

See also: [`Token` Properties](#Token_Properties), [`GETDELEGATIONTOKEN`](#Get_Delegation_Token), the note in [Delegation](#Delegation).

#### Token Properties

JavaScript syntax is used to define `tokenProperties` so that it can be referred in `Token` JSON schema.

```json
var tokenProperties =
{
  "type"      : "object",
  "properties":
  {
    "urlString":
    {
      "description": "A delegation token encoded as a URL safe string.",
      "type"       : "string",
      "required"   : true
    }
  }
}
```

See also: [`Token` Properties](#Token_Properties), the note in [Delegation](#Delegation).
### BlockStoragePolicy JSON Schema

```json
{
  "name"      : "BlockStoragePolicy",
  "properties":
  {
    "BlockStoragePolicy": blockStoragePolicyProperties      //See BlockStoragePolicy Properties
  }
}
```

See also: [`BlockStoragePolicy` Properties](#BlockStoragePolicy_Properties), [`GETSTORAGEPOLICY`](#Get_Storage_Policy)

#### BlockStoragePolicy Properties

JavaScript syntax is used to define `blockStoragePolicyProperties` so that it can be referred in both `BlockStoragePolicy` and `BlockStoragePolicies` JSON schemas.

```javascript
var blockStoragePolicyProperties =
{
  "type"      : "object",
  "properties":
  {
    "id":
    {
      "description": "Policy ID.",
      "type"       : "integer",
      "required"   : true
    },
    "name":
    {
      "description": "Policy name.",
      "type"       : "string",
      "required"   : true
    },
    "storageTypes":
    {
      "description": "An array of storage types for block placement.",
      "type"       : "array",
      "required"   : true
      "items"      :
      {
        "type": "string"
      }
    },
    "replicationFallbacks":
    {
      "description": "An array of fallback storage types for replication.",
      "type"       : "array",
      "required"   : true
      "items"      :
      {
        "type": "string"
      }
    },
    "creationFallbacks":
    {
      "description": "An array of fallback storage types for file creation.",
      "type"       : "array",
      "required"   : true
      "items"      :
      {
       "type": "string"
      }
    },
    "copyOnCreateFile":
    {
      "description": "If set then the policy cannot be changed after file creation.",
      "type"       : "boolean",
      "required"   : true
    }
  }
};
```
### ECPolicy JSON Schema

```json
{
  "name": "RS-10-4-1024k",
  schema {
           "codecName": "rs",
           "numDataUnits": 10,
           "numParityUnits": 4,
           "extraOptions": {}
          }
  "cellSize": 1048576,
  "id":5,
  "codecname":"rs",
  "numDataUnits": 10,
  "numParityUnits": 4,
  "replicationpolicy":false,
  "systemPolicy":true
}
```

### BlockStoragePolicies JSON Schema

A `BlockStoragePolicies` JSON object represents an array of `BlockStoragePolicy` JSON objects.

```json
{
  "name"      : "BlockStoragePolicies",
  "properties":
  {
    "BlockStoragePolicies":
    {
      "type"      : "object",
      "properties":
      {
        "BlockStoragePolicy":
        {
          "description": "An array of BlockStoragePolicy",
          "type"       : "array",
          "items"      : blockStoragePolicyProperties      //See BlockStoragePolicy Properties
        }
      }
    }
  }
}
```

### SnapshotDiffReport JSON Schema

```json
{
  "name": "SnapshotDiffReport",
  "type": "object",
  "properties":
  {
    "SnapshotDiffReport":
    {
      "type"        : "object",
      "properties"  :
      {
        "diffList":
        {
          "description": "An array of DiffReportEntry",
          "type"        : "array",
          "items"       : diffReportEntries,
          "required"    : true
        },
        "fromSnapshot":
        {
          "description": "Source snapshot",
          "type"        : "string",
          "required"    : true
        },
        "snapshotRoot":
        {
          "description" : "String representation of snapshot root path",
          "type"        : "string",
          "required"    : true
        },
        "toSnapshot":
        {
          "description" : "Destination snapshot",
          "type"        : "string",
          "required"    : true
        }
      }
    }
  }
}
```


#### DiffReport Entries

JavaScript syntax is used to define `diffReportEntries` so that it can be referred in `SnapshotDiffReport` JSON schema.

```javascript
var diffReportEntries =
{
  "type": "object",
  "properties":
  {
    "sourcePath":
    {
      "description" : "Source path name relative to snapshot root",
      "type"        : "string",
      "required"    : true
    },
    "targetPath":
    {
      "description" : "Target path relative to snapshot root used for renames",
      "type"        : "string",
      "required"    : true
    },
    "type":
    {
      "description" : "Type of diff report entry",
      "enum"        : ["CREATE", "MODIFY", "DELETE", "RENAME"],
      "required"    : true
    }
  }
}
```

### SnapshottableDirectoryList JSON Schema

```json
{
  "name": "SnapshottableDirectoryList",
  "type": "object",
  "properties":
  {
    "SnapshottableDirectoryList":
    {
      "description": "An array of SnapshottableDirectoryStatus",
      "type"        : "array",
      "items"       : snapshottableDirectoryStatus,
      "required"    : true
    }
  }
}
```

#### SnapshottableDirectoryStatus

JavaScript syntax is used to define `snapshottableDirectoryStatus` so that it can be referred in `SnapshottableDirectoryList` JSON schema.

```javascript
var snapshottableDirectoryStatus =
{
  "type": "object",
  "properties":
  {
    "dirStatus": fileStatusProperties,
    "parentFullPath":
    {
      "description" : "Full path of the parent of snapshottable directory",
      "type"        : "string",
      "required"    : true
    },
    "snapshotNumber":
    {
      "description" : "Number of snapshots created on the snapshottable directory",
      "type"        : "integer",
      "required"    : true
    },
    "snapshotQuota":
    {
      "description" : "Total number of snapshots allowed on the snapshottable directory",
      "type"        : "integer",
      "required"    : true
    }
  }
}
```

### BlockLocations JSON Schema

A `BlockLocations` JSON object represents an array of `BlockLocation` JSON objects.

```json
{
  "name"      : "BlockLocations",
  "properties":
  {
    "BlockLocations":
    {
      "type"      : "object",
      "properties":
      {
        "BlockLocation":
        {
          "description": "An array of BlockLocation",
          "type"       : "array",
          "items"      : blockLocationProperties      //See BlockLocation Properties
        }
      }
    }
  }
}
```

See also [`BlockLocation` Properties](#BlockLocation_Properties), [`GETFILEBLOCKLOCATIONS`](#Get_File_Block_Locations), [BlockLocation](../../api/org/apache/hadoop/fs/BlockLocation.html)

### BlockLocation JSON Schema

```json
{
  "name"      : "BlockLocation",
  "properties":
  {
    "BlockLocation": blockLocationProperties      //See BlockLocation Properties
  }
}
```

See also [`BlockLocation` Properties](#BlockLocation_Properties), [`GETFILEBLOCKLOCATIONS`](#Get_File_Block_Locations), [BlockLocation](../../api/org/apache/hadoop/fs/BlockLocation.html)

#### BlockLocation Properties

JavaScript syntax is used to define `blockLocationProperties` so that it can be referred in both `BlockLocation` and `BlockLocations` JSON schemas.

```javascript
var blockLocationProperties =
{
  "type"      : "object",
  "properties":
  {
    "cachedHosts":
    {
      "description": "Datanode hostnames with a cached replica",
      "type"       : "array",
      "required"   : "true",
      "items"      :
      {
        "description": "A datanode hostname",
        "type"       : "string"
      }
    },
    "corrupt":
    {
      "description": "True if the block is corrupted",
      "type"       : "boolean",
      "required"   : "true"
    },
    "hosts":
    {
      "description": "Datanode hostnames store the block",
      "type"       : "array",
      "required"   : "true",
      "items"      :
      {
        "description": "A datanode hostname",
        "type"       : "string"
      }
    },
    "length":
    {
      "description": "Length of the block",
      "type"       : "integer",
      "required"   : "true"
    },
    "names":
    {
      "description": "Datanode IP:xferPort for accessing the block",
      "type"       : "array",
      "required"   : "true",
      "items"      :
      {
        "description": "DatanodeIP:xferPort",
        "type"       : "string"
      }
    },
    "offset":
    {
      "description": "Offset of the block in the file",
      "type"       : "integer",
      "required"   : "true"
    },
    "storageTypes":
    {
      "description": "Storage type of each replica",
      "type"       : "array",
      "required"   : "true",
      "items"      :
      {
        "description": "Storage type",
        "enum"       : ["RAM_DISK", "SSD", "DISK", "ARCHIVE"]
      }
    },
    "topologyPaths":
    {
      "description": "Datanode addresses in network topology",
      "type"       : "array",
      "required"   : "true",
      "items"      :
      {
        "description": "/rack/host:ip",
        "type"       : "string"
      }
    }
  }
};
```

HTTP Query Parameter Dictionary
-------------------------------

### ACL Spec

| Name | `aclspec` |
|:---- |:---- |
| Description | The ACL spec included in ACL modification operations. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | See [Permissions and HDFS](./HdfsPermissionsGuide.html). |
| Syntax | See [Permissions and HDFS](./HdfsPermissionsGuide.html). |

### XAttr Name

| Name | `xattr.name` |
|:---- |:---- |
| Description | The XAttr name of a file/directory. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | Any string prefixed with user./trusted./system./security.. |
| Syntax | Any string prefixed with user./trusted./system./security.. |

### XAttr Value

| Name | `xattr.value` |
|:---- |:---- |
| Description | The XAttr value of a file/directory. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | An encoded value. |
| Syntax | Enclosed in double quotes or prefixed with 0x or 0s. |

See also: [Extended Attributes](./ExtendedAttributes.html)

### XAttr set flag

| Name | `flag` |
|:---- |:---- |
| Description | The XAttr set flag. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | CREATE,REPLACE. |
| Syntax | CREATE,REPLACE. |

See also: [Extended Attributes](./ExtendedAttributes.html)

### XAttr value encoding

| Name | `encoding` |
|:---- |:---- |
| Description | The XAttr value encoding. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | text | hex | base64 |
| Syntax | text | hex | base64 |

See also: [Extended Attributes](./ExtendedAttributes.html)

### Access Time

| Name | `accesstime` |
|:---- |:---- |
| Description | The access time of a file/directory. |
| Type | long |
| Default Value | -1 (means keeping it unchanged) |
| Valid Values | -1 or a timestamp |
| Syntax | Any integer. |

See also: [`SETTIMES`](#Set_Access_or_Modification_Time)

### Block Size

| Name | `blocksize` |
|:---- |:---- |
| Description | The block size of a file. |
| Type | long |
| Default Value | Specified in the configuration. |
| Valid Values | \> 0 |
| Syntax | Any integer. |

See also: [`CREATE`](#Create_and_Write_to_a_File)

### Buffer Size

| Name | `buffersize` |
|:---- |:---- |
| Description | The size of the buffer used in transferring data. |
| Type | int |
| Default Value | Specified in the configuration. |
| Valid Values | \> 0 |
| Syntax | Any integer. |

See also: [`CREATE`](#Create_and_Write_to_a_File), [`APPEND`](#Append_to_a_File), [`OPEN`](#Open_and_Read_a_File)

### Create Flag

| Name | `createflag` |
|:---- |:---- |
| Description | Enum of possible flags to process while creating a file |
| Type | enumerated strings |
| Default Value | \<empty\> |
| Valid Values | Legal combinations of create, overwrite, append and sync_block |
| Syntax | See note below |

The following combinations are not valid:
* append,create
* create,append,overwrite

See also: [`CREATE`](#Create_and_Write_to_a_File)

### Create Parent

| Name | `createparent` |
|:---- |:---- |
| Description | If the parent directories do not exist, should they be created? |
| Type | boolean |
| Default Value | true |
| Valid Values | true, false |
| Syntax | true |

See also: [`CREATESYMLINK`](#Create_a_Symbolic_Link)

### Delegation

| Name | `delegation` |
|:---- |:---- |
| Description | The delegation token used for authentication. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | An encoded token. |
| Syntax | See the note below. |

**Note** that delegation tokens are encoded as a URL safe string; see `encodeToUrlString()` and `decodeFromUrlString(String)` in `org.apache.hadoop.security.token.Token` for the details of the encoding.

See also: [Authentication](#Authentication)

### Destination

| Name | `destination` |
|:---- |:---- |
| Description | The destination path. |
| Type | Path |
| Default Value | \<empty\> (an invalid path) |
| Valid Values | An absolute FileSystem path without scheme and authority. |
| Syntax | Any path. |

See also: [`CREATESYMLINK`](#Create_a_Symbolic_Link), [`RENAME`](#Rename_a_FileDirectory)

### Do As

| Name | `doas` |
|:---- |:---- |
| Description | Allowing a proxy user to do as another user. |
| Type | String |
| Default Value | null |
| Valid Values | Any valid username. |
| Syntax | Any string. |

See also: [Proxy Users](#Proxy_Users)

### Fs Action

| Name | `fsaction` |
|:---- |:---- |
| Description | File system operation read/write/execute |
| Type | String |
| Default Value | null (an invalid value) |
| Valid Values | Strings matching regex pattern  "[r-][w-][x-] " |
| Syntax |  "[r-][w-][x-] " |

See also: [`CHECKACCESS`](#Check_access),

### Group

| Name | `group` |
|:---- |:---- |
| Description | The name of a group. |
| Type | String |
| Default Value | \<empty\> (means keeping it unchanged) |
| Valid Values | Any valid group name. |
| Syntax | Any string. |

See also: [`SETOWNER`](#Set_Owner)

### Length

| Name | `length` |
|:---- |:---- |
| Description | The number of bytes to be processed. |
| Type | long |
| Default Value | null (means the entire file) |
| Valid Values | \>= 0 or null |
| Syntax | Any integer. |

See also: [`OPEN`](#Open_and_Read_a_File)

### Modification Time

| Name | `modificationtime` |
|:---- |:---- |
| Description | The modification time of a file/directory. |
| Type | long |
| Default Value | -1 (means keeping it unchanged) |
| Valid Values | -1 or a timestamp |
| Syntax | Any integer. |

See also: [`SETTIMES`](#Set_Access_or_Modification_Time)

### New Length

| Name | `newlength` |
|:---- |:---- |
| Description | The size the file is to be truncated to. |
| Type | long |
| Valid Values | \>= 0 |
| Syntax | Any long. |

### Offset

| Name | `offset` |
|:---- |:---- |
| Description | The starting byte position. |
| Type | long |
| Default Value | 0 |
| Valid Values | \>= 0 |
| Syntax | Any integer. |

See also: [`OPEN`](#Open_and_Read_a_File)

### Old Snapshot Name

| Name | `oldsnapshotname` |
|:---- |:---- |
| Description | The old name of the snapshot to be renamed. |
| Type | String |
| Default Value | null |
| Valid Values | An existing snapshot name. |
| Syntax | Any string. |

See also: [`RENAMESNAPSHOT`](#Rename_Snapshot)

### Op

| Name | `op` |
|:---- |:---- |
| Description | The name of the operation to be executed. |
| Type | enum |
| Default Value | null (an invalid value) |
| Valid Values | Any valid operation name. |
| Syntax | Any string. |

See also: [Operations](#Operations)

### Overwrite

| Name | `overwrite` |
|:---- |:---- |
| Description | If a file already exists, should it be overwritten? |
| Type | boolean |
| Default Value | false |
| Valid Values | true |
| Syntax | true |

See also: [`CREATE`](#Create_and_Write_to_a_File)

### Owner

| Name | `owner` |
|:---- |:---- |
| Description | The username who is the owner of a file/directory. |
| Type | String |
| Default Value | \<empty\> (means keeping it unchanged) |
| Valid Values | Any valid username. |
| Syntax | Any string. |

See also: [`SETOWNER`](#Set_Owner)

### Permission

| Name | `permission` |
|:---- |:---- |
| Description | The permission of a file/directory. |
| Type | Octal |
| Default Value | 644 for files, 755 for directories |
| Valid Values | 0 - 1777 |
| Syntax | Any radix-8 integer (leading zeros may be omitted.) |

See also: [`CREATE`](#Create_and_Write_to_a_File), [`MKDIRS`](#Make_a_Directory), [`SETPERMISSION`](#Set_Permission)

### Recursive

| Name | `recursive` |
|:---- |:---- |
| Description | Should the operation act on the content in the subdirectories? |
| Type | boolean |
| Default Value | false |
| Valid Values | true |
| Syntax | true |

See also: [`RENAME`](#Rename_a_FileDirectory)

### Renewer

| Name | `renewer` |
|:---- |:---- |
| Description | The username of the renewer of a delegation token. |
| Type | String |
| Default Value | \<empty\> (means the current user) |
| Valid Values | Any valid username. |
| Syntax | Any string. |

See also: [`GETDELEGATIONTOKEN`](#Get_Delegation_Token)

### Replication

| Name | `replication` |
|:---- |:---- |
| Description | The number of replications of a file. |
| Type | short |
| Default Value | Specified in the configuration. |
| Valid Values | \> 0 |
| Syntax | Any integer. |

See also: [`CREATE`](#Create_and_Write_to_a_File), [`SETREPLICATION`](#Set_Replication_Factor)

### Snapshot Name

| Name | `snapshotname` |
|:---- |:---- |
| Description | The name of the snapshot to be created/deleted. Or the new name for snapshot rename. |
| Type | String |
| Default Value | null |
| Valid Values | Any valid snapshot name. |
| Syntax | Any string. |

See also: [`CREATESNAPSHOT`](#Create_Snapshot), [`DELETESNAPSHOT`](#Delete_Snapshot), [`RENAMESNAPSHOT`](#Rename_Snapshot)

### Sources

| Name | `sources` |
|:---- |:---- |
| Description | A list of source paths. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | A list of comma separated absolute FileSystem paths without scheme and authority. |
| Syntax | Any string. |

See also: [`CONCAT`](#Concat_Files)

### Token

| Name | `token` |
|:---- |:---- |
| Description | The delegation token used for the operation. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | An encoded token. |
| Syntax | See the note in [Delegation](#Delegation). |

See also: [`RENEWDELEGATIONTOKEN`](#Renew_Delegation_Token), [`CANCELDELEGATIONTOKEN`](#Cancel_Delegation_Token)

### Token Kind

| Name | `kind` |
|:---- |:---- |
| Description | The kind of the delegation token requested |
| Type | String |
| Default Value | \<empty\> (Server sets the default kind for the service) |
| Valid Values | A string that represents token kind e.g "HDFS\_DELEGATION\_TOKEN" or "WEBHDFS delegation" |
| Syntax | Any string. |

See also: [`GETDELEGATIONTOKEN`](#Get_Delegation_Token)

### Token Service

| Name | `service` |
|:---- |:---- |
| Description | The name of the service where the token is supposed to be used, e.g. ip:port of the namenode |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | ip:port in string format or logical name of the service |
| Syntax | Any string. |

See also: [`GETDELEGATIONTOKEN`](#Get_Delegation_Token)

### Username

| Name | `user.name` |
|:---- |:---- |
| Description | The authenticated user; see [Authentication](#Authentication). |
| Type | String |
| Default Value | null |
| Valid Values | Any valid username. |
| Syntax | Any string. |

See also: [Authentication](#Authentication)

### NoRedirect

| Name | `noredirect` |
|:---- |:---- |
| Description | Whether the response should return an HTTP 307 redirect or HTTP 200 OK. See [Create and Write to a File](#Create_and_Write_to_a_File). |
| Type | boolean |
| Default Value | false |
| Valid Values | true |
| Syntax | true |

See also: [Create and Write to a File](#Create_and_Write_to_a_File)

### Namespace Quota

| Name | `namespacequota` |
|:---- |:---- |
| Description | Limit on the namespace usage, i.e., number of files/directories, under a directory. |
| Type | String |
| Default Value | Long.MAX_VALUE |
| Valid Values | \> 0. |
| Syntax | Any integer. |

See also: [`SETQUOTA`](#Set_Quota)

### Storage Space Quota

| Name | `storagespacequota` |
|:---- |:---- |
| Description | Limit on storage space usage (in bytes, including replication) under a directory. |
| Type | String |
| Default Value | Long.MAX_VALUE |
| Valid Values | \> 0. |
| Syntax | Any integer. |

See also: [`SETQUOTA`](#Set_Quota), [`SETQUOTABYSTORAGETYPE`](#Set_Quota_By_Storage_Type)

### Storage Type

| Name | `storagetype` |
|:---- |:---- |
| Description | Storage type of the specific storage type quota to be modified. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | Any valid storage type. |
| Syntax | Any string. |

See also: [`SETQUOTABYSTORAGETYPE`](#Set_Quota_By_Storage_Type)

### Storage Policy

| Name | `storagepolicy` |
|:---- |:---- |
| Description | The name of the storage policy. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | Any valid storage policy name; see [GETALLSTORAGEPOLICY](#Get_all_Storage_Policies).  |
| Syntax | Any string. |

See also: [`SETSTORAGEPOLICY`](#Set_Storage_Policy)

### Erasure Coding Policy

| Name | `ecpolicy` |
|:---- |:---- |
| Description | The name of the erasure coding policy. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | Any valid erasure coding policy name;  |
| Syntax | Any string. |

See also: [`ENABLEECPOLICY`](#Enable_EC_Policy) or [`DISABLEECPOLICY`](#Disable_EC_Policy)

### Start After

| Name | `startAfter` |
|:---- |:---- |
| Description | The last item returned in the liststatus batch. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | Any valid file/directory name. |
| Syntax | Any string. |

See also: [`LISTSTATUS_BATCH`](#Iteratively_List_a_Directory)
