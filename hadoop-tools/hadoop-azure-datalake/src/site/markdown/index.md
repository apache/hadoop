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

# Hadoop Azure Data Lake Support

<!-- MACRO{toc|fromDepth=1|toDepth=3} -->

## Introduction

The `hadoop-azure-datalake` module provides support for integration with the
[Azure Data Lake Store](https://azure.microsoft.com/en-in/documentation/services/data-lake-store/).
This support comes via the JAR file `azure-datalake-store.jar`.

## Features

* Read and write data stored in an Azure Data Lake Storage account.
* Reference file system paths using URLs using the `adl` scheme for Secure Webhdfs i.e. SSL
  encrypted access.
* Can act as a source of data in a MapReduce job, or a sink.
* Tested on both Linux and Windows.
* Tested for scale.
* API `setOwner()`, `setAcl`, `removeAclEntries()`, `modifyAclEntries()` accepts UPN or OID
  (Object ID) as user and group names.

## Limitations

Partial or no support for the following operations :

* Operation on Symbolic Links
* Proxy Users
* File Truncate
* File Checksum
* File replication factor
* Home directory the active user on Hadoop cluster.
* Extended Attributes(XAttrs) Operations
* Snapshot Operations
* Delegation Token Operations
* User and group information returned as `listStatus()` and `getFileStatus()` is
in the form of the GUID associated in Azure Active Directory.

## Usage

### Concepts
Azure Data Lake Storage access path syntax is:

```
adl://<Account Name>.azuredatalakestore.net/
```

For details on using the store, see
[**Get started with Azure Data Lake Store using the Azure Portal**](https://azure.microsoft.com/en-in/documentation/articles/data-lake-store-get-started-portal/)

#### OAuth2 Support

Usage of Azure Data Lake Storage requires an OAuth2 bearer token to be present as
part of the HTTPS header as per the OAuth2 specification.
A valid OAuth2 bearer token must be obtained from the Azure Active Directory service
for those valid users who have access to Azure Data Lake Storage Account.

Azure Active Directory (Azure AD) is Microsoft's multi-tenant cloud based directory
and identity management service. See [*What is ActiveDirectory*](https://azure.microsoft.com/en-in/documentation/articles/active-directory-whatis/).

Following sections describes theOAuth2 configuration in `core-site.xml`.

### Configuring Credentials and FileSystem
Credentials can be configured using either a refresh token (associated with a user),
or a client credential (analogous to a service principal).

#### Using Refresh Tokens

Add the following properties to the cluster's `core-site.xml`

```xml
<property>
  <name>dfs.adls.oauth2.access.token.provider.type</name>
  <value>RefreshToken</value>
</property>
```

Applications must set the Client id and OAuth2 refresh token from the Azure Active Directory
service associated with the client id. See [*Active Directory Library For Java*](https://github.com/AzureAD/azure-activedirectory-library-for-java).

**Do not share client id and refresh token, it must be kept secret.**

```xml
<property>
  <name>dfs.adls.oauth2.client.id</name>
  <value></value>
</property>

<property>
  <name>dfs.adls.oauth2.refresh.token</name>
  <value></value>
</property>
```


#### Using Client Keys

##### Generating the Service Principal

1.  Go to [the portal](https://portal.azure.com)
2.  Under "Browse", look for Active Directory and click on it.
3.  Create "Web Application". Remember the name you create here - that is what you will add to your ADL account as authorized user.
4.  Go through the wizard
5.  Once app is created, Go to app configuration, and find the section on "keys"
6.  Select a key duration and hit save. Save the generated keys.
7. Note down the properties you will need to auth:
    -  The client ID
    -  The key you just generated above
    -  The token endpoint (select "View endpoints" at the bottom of the page and copy/paste the OAuth2 .0 Token Endpoint value)
    -  Resource: Always https://management.core.windows.net/ , for all customers

##### Adding the service principal to your ADL Account
1.  Go to the portal again, and open your ADL account
2.  Select Users under Settings
3.  Add your user name you created in Step 6 above (note that it does not show up in the list, but will be found if you searched for the name)
4.  Add "Owner" role

##### Configure core-site.xml
Add the following properties to your `core-site.xml`

```xml
<property>
  <name>dfs.adls.oauth2.refresh.url</name>
  <value>TOKEN ENDPOINT FROM STEP 7 ABOVE</value>
</property>

<property>
  <name>dfs.adls.oauth2.client.id</name>
  <value>CLIENT ID FROM STEP 7 ABOVE</value>
</property>

<property>
  <name>dfs.adls.oauth2.credential</name>
  <value>PASSWORD FROM STEP 7 ABOVE</value>
</property>
```

#### Protecting the Credentials with Credential Providers

In many Hadoop clusters, the `core-site.xml` file is world-readable. To protect
these credentials, it is recommended that you use the
credential provider framework to securely store them and access them.

All ADLS credential properties can be protected by credential providers.
For additional reading on the credential provider API, see
[Credential Provider API](../hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

##### Provisioning

```bash
hadoop credential create dfs.adls.oauth2.refresh.token -value 123
    -provider localjceks://file/home/foo/adls.jceks
hadoop credential create dfs.adls.oauth2.credential -value 123
    -provider localjceks://file/home/foo/adls.jceks
```

##### Configuring core-site.xml or command line property

```xml
<property>
  <name>hadoop.security.credential.provider.path</name>
  <value>localjceks://file/home/foo/adls.jceks</value>
  <description>Path to interrogate for protected credentials.</description>
</property>
```

##### Running DistCp

```bash
hadoop distcp
    [-D hadoop.security.credential.provider.path=localjceks://file/home/user/adls.jceks]
    hdfs://<NameNode Hostname>:9001/user/foo/007020615
    adl://<Account Name>.azuredatalakestore.net/testDir/
```

NOTE: You may optionally add the provider path property to the `distcp` command
line instead of added job specific configuration to a generic `core-site.xml`.
The square brackets above illustrate this capability.`

### Accessing adl URLs

After credentials are configured in `core-site.xml`, any Hadoop component may
reference files in that Azure Data Lake Storage account by using URLs of the following
format:

```
adl://<Account Name>.azuredatalakestore.net/<path>
```

The schemes `adl` identifies a URL on a Hadoop-compatible file system backed by Azure
Data Lake Storage.  `adl` utilizes encrypted HTTPS access for all interaction with
the Azure Data Lake Storage API.

For example, the following
[FileSystem Shell](../hadoop-project-dist/hadoop-common/FileSystemShell.html)
commands demonstrate access to a storage account named `youraccount`.


```bash
hadoop fs -mkdir adl://yourcontainer.azuredatalakestore.net/testDir

hadoop fs -put testFile adl://yourcontainer.azuredatalakestore.net/testDir/testFile

hadoop fs -cat adl://yourcontainer.azuredatalakestore.net/testDir/testFile
test file content
```
### User/Group Representation

The `hadoop-azure-datalake` module provides support for configuring how
User/Group information is represented during
`getFileStatus()`, `listStatus()`,  and `getAclStatus()` calls..

Add the following properties to `core-site.xml`

```xml
<property>
  <name>adl.feature.ownerandgroup.enableupn</name>
  <value>true</value>
  <description>
    When true : User and Group in FileStatus/AclStatus response is
    represented as user friendly name as per Azure AD profile.

    When false (default) : User and Group in FileStatus/AclStatus
    response is represented by the unique identifier from Azure AD
    profile (Object ID as GUID).

    For performance optimization, Recommended default value.
  </description>
</property>
```
## Testing the azure-datalake-store Module
The `hadoop-azure` module includes a full suite of unit tests.
Most of the tests will run without additional configuration by running mvn test.
This includes tests against mocked storage, which is an in-memory emulation of Azure Data Lake Storage.

A selection of tests can run against the Azure Data Lake Storage. To run these
tests, please create `src/test/resources/auth-keys.xml` with Adl account
information mentioned in the above sections and the following properties.

```xml
<property>
    <name>dfs.adl.test.contract.enable</name>
    <value>true</value>
</property>

<property>
    <name>test.fs.adl.name</name>
    <value>adl://yourcontainer.azuredatalakestore.net</value>
</property>
```
