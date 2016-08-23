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

CredentialProvider API Guide
=====================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
--------

  The CredentialProvider API is an SPI framework for plugging in extensible
  credential providers. Credential providers are used to separate the use of
  sensitive tokens, secrets and passwords from the details of their storage and
  management. The ability to choose various storage mechanisms for protecting
  these credentials allows us to keep such sensitive assets out of clear text,
  away from prying eyes and potentially to be managed by third party solutions.

  This document aims to describe the design of the CredentialProvider API, the
  out of the box implementations, where they are used and how to adopt their use.

Usage
-----
### Usage Overview
Let's provide a quick overview of the use of the credential provider framework for protecting passwords or other sensitive tokens in hadoop.

##### Why is it used?
There are certain deployments that are very sensitive to how sensitive tokens like passwords are stored and managed within the cluster. For instance, there may be security best practices and policies in place that require such things to never be stored in clear text, for example. Enterprise deployments may be required to use a preferred solution for managing credentials and we need a way to plug in integrations for them.

##### General Usage Pattern
There are numerous places within the Hadoop project and ecosystem that can leverage the credential provider API today and the number continues to grow. In general, the usage pattern consists of the same requirements and flow.

1. Provision credentials within provider specific stores. This provisioning may be accomplished through the hadoop credential command or possibly through provider specific management tools.
2. Configure the credential provider path property. The provider path property `hadoop.security.credential.provider.path` is a comma separated list of one or more credential provider URIs that is traversed while trying to resolve a credential alias.
    - This property may be configured within core-site.xml or a component specific configuration file that is merged with core-site.xml.
    - For command line interfaces, such as that for DistCp, the property can be added with a hadoop system property ("-D *property=value*") and dynamically added to the Configuration.
3. Features or components that leverage the new [Configuration.getPassword](../../api/org/apache/hadoop/conf/Configuration.html#getPassword-java.lang.String-) method to resolve their credentials will automatically pick up support for the credential provider API.
    - By using the same property names as are used for existing clear text passwords, this mechanism allows for the migration to credential providers while providing backward compatibility for clear text.
    - The entire credential provider path is interrogated before falling back to clear text passwords in config.
4. Features or components that do not use the hadoop Configuration class for config or have other internal uses for the credential providers may choose to write to the CredentialProvider API itself. An example of its use will be included in this document but may also be found within [Configuration.getPassword](../../api/org/apache/hadoop/conf/Configuration.html#getPassword-java.lang.String-) and within the unit tests of features that have added support and need to provision credentials for the tests.

##### Provision Credentials
Example: ssl.server.keystore.password

```
    hadoop credential create ssl.server.keystore.password -value 123
      -provider localjceks://file/home/lmccay/aws.jceks
```

Note that the alias names are the same as the configuration properties that were used to get the
credentials from the Configuration.get method. Reusing these names allows for intuitive
migration to the use of credential providers and fall back logic for backward compatibility.

##### Configuring the Provider Path
Now, we need to make sure that this provisioned credential store is known at runtime by the
[Configuration.getPassword](../../api/org/apache/hadoop/conf/Configuration.html#getPassword-java.lang.String-) method. If there is no credential provider path configuration then
getPassword will skip the credential provider API interrogation. So, it is important that the
following be configured within core-site.xml or your component's equivalent.

```
    <property>
      <name>hadoop.security.credential.provider.path</name>
      <value>localjceks://file/home/lmccay/aws.jceks</value>
      <description>Path to interrogate for protected credentials.</description>
    </property>
```

A couple additional things to note about the provider path:

1. The scheme is used to indicate the type of provider in the above case the
 localjceks provider does not have a dependency on the Hadoop fs abstraction
 and is needed sometimes to avoid a recursive dependency. Another provider
 represented by jceks, does use the Hadoop fs abstraction and therefore has
 support for keystores provisioned within HDFS. A third provider type is the
 user type. This provider can manage credentials stored within the Credentials
 file for a process.
2. The path configuration accepts a comma separated path of providers or
 credential stores. The [Configuration.getPassword](../../api/org/apache/hadoop/conf/Configuration.html#getPassword-java.lang.String-) method will walk through
 all of the providers until it resolves the alias or exhausts the list.
 Depending on the runtime needs for credentials, we may need to configure
 a chain of providers to check.

In summary, first, provision the credentials into a provider then configure the provider for use by a feature or component and it will often just be picked up through the use of the [Configuration.getPassword](../../api/org/apache/hadoop/conf/Configuration.html#getPassword-java.lang.String-) method.

##### Supported Features
| Feature\Component | Description | Link |
|:---- |:---- |:---|
|LDAPGroupsMapping    |LDAPGroupsMapping is used to look up the groups for a given user in LDAP. The CredentialProvider API is used to protect the LDAP bind password and those needed for SSL.|[LDAP Groups Mapping](GroupsMapping.html#LDAP_Groups_Mapping)|
|SSL Passwords        |FileBasedKeyStoresFactory leverages the credential provider API in order to resolve the SSL related passwords.|TODO|
|HDFS                 |DFSUtil leverages Configuration.getPassword method to use the credential provider API and/or fallback to the clear text value stored in ssl-server.xml.|TODO|
|YARN                 |WebAppUtils uptakes the use of the credential provider API through the new method on Configuration called getPassword. This provides an alternative to storing the passwords in clear text within the ssl-server.xml file while maintaining backward compatibility.|TODO|
|AWS <br/> S3/S3A     |Uses Configuration.getPassword to get the S3 credentials. They may be resolved through the credential provider API or from the config for backward compatibility.|[AWS S3/S3A Usage](../../hadoop-aws/tools/hadoop-aws/index.html)|
|Azure <br/> WASB     |Uses Configuration.getPassword to get the WASB credentials. They may be resolved through the credential provider API or from the config for backward compatibility.|[Azure WASB Usage](../../hadoop-azure/index.html)|
|Apache <br/> Accumulo|The trace.password property is used by the Tracer to authenticate with Accumulo and persist the traces in the trace table. The credential provider API is used to acquire the trace.password from a provider or from configuration for backward compatibility.|TODO|
|Apache <br/> Slider  |A capability has been added to Slider to prompt the user for needed passwords and store them using CredentialProvider so they can be retrieved by an app later.|TODO|
|Apache <br/> Hive    |Protection of the metastore password, SSL related passwords and JDO string password has been added through the use of the Credential Provider API|TODO|
|Apache <br/> HBase   |The HBase RESTServer is using the new Configuration.getPassword method so that the credential provider API will be checked first then fall back to clear text - when allowed.|TODO|
|Apache <br/> Oozie   |Protects SSL, email and JDBC passwords using the credential provider API.|TODO|
|Apache <br/> Ranger  |Protects database, trust and keystore passwords using the credential provider API.|TODO|

### Credential Management

#### The hadoop credential Command

Usage: `hadoop credential <subcommand> [options]`

See the command options detail in the [Commands Manual](CommandsManual.html#credential)

Utilizing the credential command will often be for provisioning a password or secret to a particular credential store provider. In order to explicitly indicate which provider store to use the `-provider` option should be used.

Example: `hadoop credential create ssl.server.keystore.password -provider jceks://file/tmp/test.jceks`

In order to indicate a particular provider type and location, the user must provide the `hadoop.security.credential.provider.path` configuration element in core-site.xml or use the command line option `-provider` on each of the credential management commands. This provider path is a comma-separated list of URLs that indicates the type and location of a list of providers that should be consulted. For example, the following path: `user:///,jceks://file/tmp/test.jceks,jceks://hdfs@nn1.example.com/my/path/test.jceks` indicates that the current user's credentials file should be consulted through the User Provider, that the local file located at `/tmp/test.jceks` is a Java Keystore Provider and that the file located within HDFS at `nn1.example.com/my/path/test.jceks` is also a store for a Java Keystore Provider.

#### Provider Types

1. The `UserProvider`, which is representd by the provider URI `user:///`, is used to retrieve credentials from a user's Credentials file. This file is used to store various tokens, secrets and passwords that are needed by executing jobs and applications.
2. The `JavaKeyStoreProvider`, which is represented by the provider URI `jceks://file|hdfs/path-to-keystore`, is used to retrieve credentials from a Java keystore. The underlying use of the Hadoop filesystem abstraction allows credentials to be stored on the local filesystem or within HDFS.
3. The `LocalJavaKeyStoreProvider`, which is represented by the provider URI `localjceks://file/path-to-keystore`, is used to access credentials from a Java keystore that is must be stored on the local filesystem. This is needed for credentials that would result in a recursive dependency on accessing HDFS. Anytime that your credential is required to gain access to HDFS we can't depend on getting a credential out of HDFS to do so.

#### Keystore Passwords

Keystores in Java are generally protected by passwords. The primary method of protection of the keystore-based credential providers are OS level file permissions and any other policy based access protection that may exist for the target filesystem. While the password is not a primary source of protection, it is very important to understand the mechanics required and options available for managing these passwords. It is also very important to understand all the parties that will need access to the password used to protect the keystores in order to consume them at runtime.

##### Options
| Option | Description | Notes |
|:---- |:---- |:---|
|Default password |This is a harcoded password of "none". |This is a hardcoded password in an open source project and as such has obvious disadvantages. However, the mechanics section will show that it is simpler and consequently nearly as secure as the other more complex options.|
|Environment variable|`HADOOP_CREDSTORE_PASSWORD`|This option uses an environment variable to communicate the password that should be used when interrogating all of the keystores that are configured in the `hadoop.security.credential.provider.path` configuration property. All of the keystore based providers in the path will need to be protected by the same password.|
|Password-file|`hadoop.security.credstore.java-keystore-provider.password-file`|This option uses a "side file" that has its location configured in the `hadoop.security.credstore.java-keystore-provider.password-file` configuration property to communicate the password that should be used when interrogating all of the keystores that are configured in the `hadoop.security.credential.provider.path` configuration property.|

##### Mechanics
Extremely important to consider that *all* of the runtime consumers of the credential being protected (mapreduce jobs/applications) will need to have access to the password used to protect the keystore providers. Communicating this password can be done a number of ways and they are described in the Options section above.

|Keystore Password| Description|Sync Required|Clear Text|File Permissions|
|:---- |:---- |:---|:---|:---|
|Default Password|Hardcoded password is the default. Essentially, when using the default password for all keystore-based credential stores, we are leveraging the file permissions to protect the credential store and the keystore password is just a formality of persisting the keystore.|No|Yes|No (documented)|
|Environment Variable|`HADOOP_CREDSTORE_PASSWORD` Environment variable must be set to the custom password for all keystores that may be configured in the provider path of any process that needs to access credentials from a keystore-based credential provider. There is only one env variable for the entire path of comma separated providers. It is difficult to know the passwords required for each keystore and it is suggested that the same be used for all keystore-based credential providers to avoid this issue. Setting the environment variable will likely require it to be set from a script or some other clear text storage mechanism. Environment variables for running processes are available from various unix commands.|Yes|Yes|No|
|Password File|`hadoop.security.credstore.java-keystore-provider.password-file` configuration property must be set to the location of the "side file" that contains the custom password for all keystores that may be configured in the provider path. Any process that needs to access credentials from a keystore-based credential provider will need to have this configuration property set to the appropriate file location. There is only one password-file for the entire path of comma separated providers. It is difficult to know the passwords required for each keystore and it is therefore suggested that the same be used for all keystore-based credential providers to avoid this issue. Password-files are additional files that need to be managed, store the password in clear text and need file permissions to be set such that only those that need access to them have it. If file permissions are set inappropriately the password to access the keystores is available in clear text.|Yes|Yes|Yes|

The use of the default password means that no additional communication/synchronization to runtime consumers needs to be done. The default password is known but file permissions are the primary protection of the keystore.

When file permissions are thwarted, unlike "side files", there are no standard tools that can expose the protected credentials - even with the password known. Keytool requires a password that is six characters or more and doesn't know how to retrieve general secrets from a keystore. It is also limited to PKI keypairs. Editors will not review the secrets stored within the keystore, nor will `cat`, `more` or any other standard tools. This is why the keystore providers are better than "side file" storage of credentials.

That said, it is trivial for someone to write code to access the credentials stored within a keystore-based credential provider using the API. Again, when using the default password, the password is merely a formality of persisting the keystore. The *only* protection is file  permissions and OS level access policy.

Users may decide to use a password "side file" to store the password for the keystores themselves and this is supported. It is just really important to be aware of the mechanics required for this level of correctness.

