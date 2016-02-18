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

---

 - [Overview](#Overview)
 - [Usage](#Usage)
     - [Usage Overview](#Usage_Overview)
     - [Supported Features](#Supported_Features)
 - [Credential Management](#Credential_Management)
    - [The hadoop credential Command](#The_hadoop_credential_Command)

---

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
|LDAPGroupsMapping    |LDAPGroupsMapping is used to look up the groups for a given user in LDAP. The CredentialProvider API is used to protect the LDAP bind password and those needed for SSL.|TODO|
|SSL Passwords        |FileBasedKeyStoresFactory leverages the credential provider API in order to resolve the SSL related passwords.|TODO|
|HDFS                 |DFSUtil leverages Configuration.getPassword method to use the credential provider API and/or fallback to the clear text value stored in ssl-server.xml.|TODO|
|YARN                 |WebAppUtils uptakes the use of the credential provider API through the new method on Configuration called getPassword. This provides an alternative to storing the passwords in clear text within the ssl-server.xml file while maintaining backward compatibility.|TODO|
|AWS <br/> S3/S3A     |Uses Configuration.getPassword to get the S3 credentials. They may be resolved through the credential provider API or from the config for backward compatibility.|[AWS S3/S3A Usage](../../hadoop-aws/tools/hadoop-aws/index.html)|
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

Example: `hadoop credential create ssl.server.keystore.password jceks://file/tmp/test.jceks`

In order to indicate a particular provider type and location, the user must provide the `hadoop.security.credential.provider.path` configuration element in core-site.xml or use the command line option `-provider` on each of the credential management commands. This provider path is a comma-separated list of URLs that indicates the type and location of a list of providers that should be consulted. For example, the following path: `user:///,jceks://file/tmp/test.jceks,jceks://hdfs@nn1.example.com/my/path/test.jceks` indicates that the current user's credentials file should be consulted through the User Provider, that the local file located at `/tmp/test.jceks` is a Java Keystore Provider and that the file located within HDFS at `nn1.example.com/my/path/test.jceks` is also a store for a Java Keystore Provider.
