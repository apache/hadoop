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

# Hadoop Azure Support: ABFS  — Azure Data Lake Storage Gen2

<!-- MACRO{toc|fromDepth=1|toDepth=3} -->

## <a name="introduction"></a> Introduction

The `hadoop-azure` module provides support for the Azure Data Lake Storage Gen2
storage layer through the "abfs" connector

To make it part of Apache Hadoop's default classpath, make sure that
`HADOOP_OPTIONAL_TOOLS` environment variable has `hadoop-azure` in the list,
*on every machine in the cluster*

```bash
export HADOOP_OPTIONAL_TOOLS=hadoop-azure
```

You can set this locally in your `.profile`/`.bashrc`, but note it won't
propagate to jobs running in-cluster.


## <a name="features"></a> Features of the ABFS connector.

* Supports reading and writing data stored in an Azure Blob Storage account.
* *Fully Consistent* view of the storage across all clients.
* Can read data written through the `wasb:` connector.
* Presents a hierarchical file system view by implementing the standard Hadoop
  [`FileSystem`](../api/org/apache/hadoop/fs/FileSystem.html) interface.
* Supports configuration of multiple Azure Blob Storage accounts.
* Can act as a source or destination of data in Hadoop MapReduce, Apache Hive, Apache Spark.
* Tested at scale on both Linux and Windows by Microsoft themselves.
* Can be used as a replacement for HDFS on Hadoop clusters deployed in Azure infrastructure.

For details on ABFS, consult the following documents:

* [A closer look at Azure Data Lake Storage Gen2](https://azure.microsoft.com/en-gb/blog/a-closer-look-at-azure-data-lake-storage-gen2/);
MSDN Article from June 28, 2018.
* [Storage Tiers](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers)

## Getting started

### Concepts

The Azure Storage data model presents 3 core concepts:

* **Storage Account**: All access is done through a storage account.
* **Container**: A container is a grouping of multiple blobs.  A storage account
  may have multiple containers.  In Hadoop, an entire file system hierarchy is
  stored in a single container.
* **Blob**: A file of any type and size stored with the existing wasb connector

The ABFS connector connects to classic containers, or those created
with Hierarchical Namespaces.

## <a name="namespaces"></a> Hierarchical Namespaces (and WASB Compatibility)

A key aspect of ADLS Gen 2 is its support for
[hierachical namespaces](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace)
These are effectively directories and offer high performance rename and delete operations
—something which makes a significant improvement in performance in query engines
writing data to, including MapReduce, Spark, Hive, as well as DistCp.

This feature is only available if the container was created with "namespace"
support.

You enable namespace support when creating a new Storage Account,
by checking the "Hierarchical Namespace" option in the Portal UI, or, when
creating through the command line, using the option `--hierarchical-namespace true`

_You cannot enable Hierarchical Namespaces on an existing storage account_

Containers in a storage account with Hierarchical Namespaces are
not (currently) readable through the `wasb:` connector.

Some of the `az storage` command line commands fail too, for example:

```bash
$ az storage container list --account-name abfswales1
Blob API is not yet supported for hierarchical namespace accounts. ErrorCode: BlobApiNotYetSupportedForHierarchicalNamespaceAccounts
```

### <a name="creating"></a> Creating an Azure Storage Account

The best documentation on getting started with Azure Datalake Gen2 with the
abfs connector is [Using Azure Data Lake Storage Gen2 with Azure HDInsight clusters](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-use-hdi-cluster)

It includes instructions to create it from [the Azure command line tool](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest),
which can be installed on Windows, MacOS (via Homebrew) and Linux (apt or yum).

The [az storage](https://docs.microsoft.com/en-us/cli/azure/storage?view=azure-cli-latest) subcommand
handles all storage commands, [`az storage account create`](https://docs.microsoft.com/en-us/cli/azure/storage/account?view=azure-cli-latest#az-storage-account-create)
does the creation.

Until the ADLS gen2 API support is finalized, you need to add an extension
to the ADLS command.
```bash
az extension add --name storage-preview
```

Check that all is well by verifying that the usage command includes `--hierarchical-namespace`:
```
$  az storage account
usage: az storage account create [-h] [--verbose] [--debug]
     [--output {json,jsonc,table,tsv,yaml,none}]
     [--query JMESPATH] --resource-group
     RESOURCE_GROUP_NAME --name ACCOUNT_NAME
     [--sku {Standard_LRS,Standard_GRS,Standard_RAGRS,Standard_ZRS,Premium_LRS,Premium_ZRS}]
     [--location LOCATION]
     [--kind {Storage,StorageV2,BlobStorage,FileStorage,BlockBlobStorage}]
     [--tags [TAGS [TAGS ...]]]
     [--custom-domain CUSTOM_DOMAIN]
     [--encryption-services {blob,file,table,queue} [{blob,file,table,queue} ...]]
     [--access-tier {Hot,Cool}]
     [--https-only [{true,false}]]
     [--file-aad [{true,false}]]
     [--hierarchical-namespace [{true,false}]]
     [--bypass {None,Logging,Metrics,AzureServices} [{None,Logging,Metrics,AzureServices} ...]]
     [--default-action {Allow,Deny}]
     [--assign-identity]
     [--subscription _SUBSCRIPTION]
```

You can list locations from `az account list-locations`, which lists the
name to refer to in the `--location` argument:
```
$ az account list-locations -o table

DisplayName          Latitude    Longitude    Name
-------------------  ----------  -----------  ------------------
East Asia            22.267      114.188      eastasia
Southeast Asia       1.283       103.833      southeastasia
Central US           41.5908     -93.6208     centralus
East US              37.3719     -79.8164     eastus
East US 2            36.6681     -78.3889     eastus2
West US              37.783      -122.417     westus
North Central US     41.8819     -87.6278     northcentralus
South Central US     29.4167     -98.5        southcentralus
North Europe         53.3478     -6.2597      northeurope
West Europe          52.3667     4.9          westeurope
Japan West           34.6939     135.5022     japanwest
Japan East           35.68       139.77       japaneast
Brazil South         -23.55      -46.633      brazilsouth
Australia East       -33.86      151.2094     australiaeast
Australia Southeast  -37.8136    144.9631     australiasoutheast
South India          12.9822     80.1636      southindia
Central India        18.5822     73.9197      centralindia
West India           19.088      72.868       westindia
Canada Central       43.653      -79.383      canadacentral
Canada East          46.817      -71.217      canadaeast
UK South             50.941      -0.799       uksouth
UK West              53.427      -3.084       ukwest
West Central US      40.890      -110.234     westcentralus
West US 2            47.233      -119.852     westus2
Korea Central        37.5665     126.9780     koreacentral
Korea South          35.1796     129.0756     koreasouth
France Central       46.3772     2.3730       francecentral
France South         43.8345     2.1972       francesouth
Australia Central    -35.3075    149.1244     australiacentral
Australia Central 2  -35.3075    149.1244     australiacentral2
```

Once a location has been chosen, create the account
```bash

az storage account create --verbose \
    --name abfswales1 \
    --resource-group devteam2 \
    --kind StorageV2 \
    --hierarchical-namespace true \
    --location ukwest \
    --sku Standard_LRS \
    --https-only true \
    --encryption-services blob \
    --access-tier Hot \
    --tags owner=engineering \
    --assign-identity \
    --output jsonc
```

The output of the command is a JSON file, whose `primaryEndpoints` command
includes the name of the store endpoint:
```json
{
  "primaryEndpoints": {
    "blob": "https://abfswales1.blob.core.windows.net/",
    "dfs": "https://abfswales1.dfs.core.windows.net/",
    "file": "https://abfswales1.file.core.windows.net/",
    "queue": "https://abfswales1.queue.core.windows.net/",
    "table": "https://abfswales1.table.core.windows.net/",
    "web": "https://abfswales1.z35.web.core.windows.net/"
  }
}
```

The `abfswales1.dfs.core.windows.net` account is the name by which the
storage account will be referred to.

Now ask for the connection string to the store, which contains the account key
```bash
az storage account  show-connection-string --name abfswales1
{
  "connectionString": "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=abfswales1;AccountKey=ZGlkIHlvdSByZWFsbHkgdGhpbmsgSSB3YXMgZ29pbmcgdG8gcHV0IGEga2V5IGluIGhlcmU/IA=="
}
```

You then need to add the access key to your `core-site.xml`, JCEKs file or
use your cluster management tool to set it the option `fs.azure.account.key.STORAGE-ACCOUNT`
to this value.
```XML
<property>
  <name>fs.azure.account.key.abfswales1.dfs.core.windows.net</name>
  <value>ZGlkIHlvdSByZWFsbHkgdGhpbmsgSSB3YXMgZ29pbmcgdG8gcHV0IGEga2V5IGluIGhlcmU/IA==</value>
</property>
```

#### Creation through the Azure Portal

Creation through the portal is covered in [Quickstart: Create an Azure Data Lake Storage Gen2 storage account](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-quickstart-create-account)

Key Steps

1. Create a new Storage Account in a location which suits you.
1. "Basics" Tab: select "StorageV2".
1. "Advanced" Tab: enable "Hierarchical Namespace".

You have now created your storage account. Next, get the key for authentication
for using the default "Shared Key" authentication.

1. Go to the Azure Portal.
1. Select "Storage Accounts"
1. Select the newly created storage account.
1. In the list of settings, locate "Access Keys" and select that.
1. Copy one of the access keys to the clipboard, add to the XML option,
set in cluster management tools, Hadoop JCEKS file or KMS store.

### <a name="new_container"></a> Creating a new container

An Azure storage account can have multiple containers, each with the container
name as the userinfo field of the URI used to reference it.

For example, the container "container1" in the storage account just created
will have the URL `abfs://container1@abfswales1.dfs.core.windows.net/`


You can create a new container through the ABFS connector, by setting the option
 `fs.azure.createRemoteFileSystemDuringInitialization` to `true`.

If the container does not exist, an attempt to list it with `hadoop fs -ls`
will fail

```
$ hadoop fs -ls abfs://container1@abfswales1.dfs.core.windows.net/

ls: `abfs://container1@abfswales1.dfs.core.windows.net/': No such file or directory
```

Enable remote FS creation and the second attempt succeeds, creating the container as it does so:

```
$ hadoop fs -D fs.azure.createRemoteFileSystemDuringInitialization=true \
 -ls abfs://container1@abfswales1.dfs.core.windows.net/
```

This is useful for creating accounts on the command line, especially before
the `az storage` command supports hierarchical namespaces completely.


### Listing and examining containers of a Storage Account.

You can use the [Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/)

## <a name="configuring"></a> Configuring ABFS

Any configuration can be specified generally (or as the default when accessing all accounts)
or can be tied to a specific account.
For example, an OAuth identity can be configured for use regardless of which
account is accessed with the property `fs.azure.account.oauth2.client.id`
or you can configure an identity to be used only for a specific storage account with
`fs.azure.account.oauth2.client.id.<account_name>.dfs.core.windows.net`.

This is shown in the Authentication section.

## <a name="authentication"></a> Authentication

Authentication for ABFS is ultimately granted by [Azure Active Directory](https://docs.microsoft.com/en-us/azure/active-directory/develop/authentication-scenarios).

The concepts covered there are beyond the scope of this document to cover;
developers are expected to have read and understood the concepts therein
to take advantage of the different authentication mechanisms.

What is covered here, briefly, is how to configure the ABFS client to authenticate
in different deployment situations.

The ABFS client can be deployed in different ways, with its authentication needs
driven by them.

1. With the storage account's authentication secret in the configuration:
"Shared Key".
1. Using OAuth 2.0 tokens of one form or another.
1. Deployed in-Azure with the Azure VMs providing OAuth 2.0 tokens to the application,
 "Managed Instance".

What can be changed is what secrets/credentials are used to authenticate the caller.

The authentication mechanism is set in `fs.azure.account.auth.type` (or the account specific variant),
and, for the various OAuth options `fs.azure.account.oauth.provider.type`

All secrets can be stored in JCEKS files. These are encrypted and password
protected —use them or a compatible Hadoop Key Management Store wherever
possible

### <a name="shared-key-auth"></a> Default: Shared Key

This is the simplest authentication mechanism of account + password.

The account name is inferred from the URL;
the password, "key", retrieved from the XML/JCECKs configuration files.

```xml
<property>
  <name>fs.azure.account.auth.type.abfswales1.dfs.core.windows.net</name>
  <value>SharedKey</value>
  <description>
  </description>
</property>
<property>
  <name>fs.azure.account.key.abfswales1.dfs.core.windows.net</name>
  <value>ZGlkIHlvdSByZWFsbHkgdGhpbmsgSSB3YXMgZ29pbmcgdG8gcHV0IGEga2V5IGluIGhlcmU/IA==</value>
  <description>
  The secret password. Never share these.
  </description>
</property>
```

*Note*: The source of the account key can be changed through a custom key provider;
one exists to execute a shell script to retrieve it.

### <a name="oauth-client-credentials"></a> OAuth 2.0 Client Credentials

OAuth 2.0 credentials of (client id, client secret, endpoint) are provided in the configuration/JCEKS file.

The specifics of this process is covered
in [hadoop-azure-datalake](../hadoop-azure-datalake/index.html#Configuring_Credentials_and_FileSystem);
the key names are slightly different here.

```xml
<property>
  <name>fs.azure.account.auth.type</name>
  <value>OAuth</value>
  <description>
  Use OAuth authentication
  </description>
</property>
<property>
  <name>fs.azure.account.oauth.provider.type</name>
  <value>org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider</value>
  <description>
  Use client credentials
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.endpoint</name>
  <value></value>
  <description>
  URL of OAuth endpoint
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.id</name>
  <value></value>
  <description>
  Client ID
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.secret</name>
  <value></value>
  <description>
  Secret
  </description>
</property>
```

### <a name="oauth-user-and-passwd"></a> OAuth 2.0: Username and Password

An OAuth 2.0 endpoint, username and password are provided in the configuration/JCEKS file.

```xml
<property>
  <name>fs.azure.account.auth.type</name>
  <value>OAuth</value>
  <description>
  Use OAuth authentication
  </description>
</property>
<property>
  <name>fs.azure.account.oauth.provider.type</name>
  <value>org.apache.hadoop.fs.azurebfs.oauth2.UserPasswordTokenProvider</value>
  <description>
  Use user and password
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.endpoint</name>
  <value></value>
  <description>
  URL of OAuth 2.0 endpoint
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.user.name</name>
  <value></value>
  <description>
  username
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.user.password</name>
  <value></value>
  <description>
  password for account
  </description>
</property>
```

### <a name="oauth-refresh-token"></a> OAuth 2.0: Refresh Token

With an existing Oauth 2.0 token, make a request of the Active Directory endpoint
`https://login.microsoftonline.com/Common/oauth2/token` for this token to be refreshed.

```xml
<property>
  <name>fs.azure.account.auth.type</name>
  <value>OAuth</value>
  <description>
  Use OAuth 2.0 authentication
  </description>
</property>
<property>
  <name>fs.azure.account.oauth.provider.type</name>
  <value>org.apache.hadoop.fs.azurebfs.oauth2.RefreshTokenBasedTokenProvider</value>
  <description>
  Use the Refresh Token Provider
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.refresh.token</name>
  <value></value>
  <description>
  Refresh token
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.id</name>
  <value></value>
  <description>
  Optional Client ID
  </description>
</property>
```

### <a name="managed-identity"></a> Azure Managed Identity

[Azure Managed Identities](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview), formerly "Managed Service Identities".

OAuth 2.0 tokens are issued by a special endpoint only accessible
from the executing VM (`http://169.254.169.254/metadata/identity/oauth2/token`).
The issued credentials can be used to authenticate.

The Azure Portal/CLI is used to create the service identity.

```xml
<property>
  <name>fs.azure.account.auth.type</name>
  <value>OAuth</value>
  <description>
  Use OAuth authentication
  </description>
</property>
<property>
  <name>fs.azure.account.oauth.provider.type</name>
  <value>org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider</value>
  <description>
  Use MSI for issuing OAuth tokens
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.msi.tenant</name>
  <value></value>
  <description>
  Optional MSI Tenant ID
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.id</name>
  <value></value>
  <description>
  Optional Client ID
  </description>
</property>
```

### Custom OAuth 2.0 Token Provider

A Custom OAuth 2.0 token provider supplies the ABFS connector with an OAuth 2.0
token when its `getAccessToken()` method is invoked.

```xml
<property>
  <name>fs.azure.account.auth.type</name>
  <value>Custom</value>
  <description>
  Custom Authentication
  </description>
</property>
<property>
  <name>fs.azure.account.oauth.provider.type</name>
  <value></value>
  <description>
  classname of Custom Authentication Provider
  </description>
</property>
```

The declared class must implement `org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee`
and optionally `org.apache.hadoop.fs.azurebfs.extensions.BoundDTExtension`.

## <a name="technical"></a> Technical notes

### <a name="proxy"></a> Proxy setup

The connector uses the JVM proxy settings to control its proxy setup.

See The [Oracle Java documentation](https://docs.oracle.com/javase/8/docs/technotes/guides/net/proxies.html) for the options to set.

As the connector uses HTTPS by default, the `https.proxyHost` and `https.proxyPort`
options are those which must be configured.

In MapReduce jobs, including distcp, the proxy options must be set in both the
`mapreduce.map.java.opts` and `mapreduce.reduce.java.opts`.

```bash
# this variable is only here to avoid typing the same values twice.
# It's name is not important.
export DISTCP_PROXY_OPTS="-Dhttps.proxyHost=web-proxy.example.com -Dhttps.proxyPort=80"

hadoop distcp \
  -D mapreduce.map.java.opts="$DISTCP_PROXY_OPTS" \
  -D mapreduce.reduce.java.opts="$DISTCP_PROXY_OPTS" \
  -update -skipcrccheck -numListstatusThreads 40 \
  hdfs://namenode:8020/users/alice abfs://backups@account.dfs.core.windows.net/users/alice
```

Without these settings, even though access to ADLS may work from the command line,
`distcp` access can fail with network errors.

### <a name="security"></a> Security

As with other object stores, login secrets are valuable pieces of information.
Organizations should have a process for safely sharing them.

### <a name="limitations"></a> Limitations of the ABFS connector

* File last access time is not tracked.
* Extended attributes are not supported.
* File Checksums are not supported.
* The `Syncable` interfaces `hsync()` and `hflush()` operations are supported if
`fs.azure.enable.flush` is set to true (default=true). With the Wasb connector,
this limited the number of times either call could be made to 50,000
[HADOOP-15478](https://issues.apache.org/jira/browse/HADOOP-15478).
If abfs has the a similar limit, then excessive use of sync/flush may
cause problems.

### <a name="consistency"></a> Consistency and Concurrency

As with all Azure storage services, the Azure Datalake Gen 2 store offers
a fully consistent view of the store, with complete
Create, Read, Update, and Delete consistency for data and metadata.
(Compare and contrast with S3 which only offers Create consistency;
S3Guard adds CRUD to metadata, but not the underlying data).

### <a name="performance"></a> Performance and Scalability

For containers with hierarchical namespaces,
the scalability numbers are, in Big-O-notation, as follows:

| Operation | Scalability |
|-----------|-------------|
| File Rename | `O(1)` |
| File Delete | `O(1)` |
| Directory Rename:| `O(1)` |
| Directory Delete | `O(1)` |

For non-namespace stores, the scalability becomes:

| Operation | Scalability |
|-----------|-------------|
| File Rename | `O(1)` |
| File Delete | `O(1)` |
| Directory Rename:| `O(files)` |
| Directory Delete | `O(files)` |

That is: the more files there are, the slower directory operations get.


Further reading: [Azure Storage Scalability Targets](https://docs.microsoft.com/en-us/azure/storage/common/storage-scalability-targets?toc=%2fazure%2fstorage%2fqueues%2ftoc.json)

### <a name="extensibility"></a> Extensibility

The ABFS connector supports a number of limited-private/unstable extension
points for third-parties to integrate their authentication and authorization
services into the ABFS client.

* `CustomDelegationTokenManager` : adds ability to issue Hadoop Delegation Tokens.
* `AbfsAuthorizer` permits client-side authorization of file operations.
* `CustomTokenProviderAdaptee`: allows for custom provision of
Azure OAuth tokens.
* `KeyProvider`.

Consult the source in `org.apache.hadoop.fs.azurebfs.extensions`
and all associated tests to see how to make use of these extension points.

_Warning_ These extension points are unstable.

## <a href="options"></a> Other configuration options

Consult the javadocs for `org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys`,
`org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations` and
`org.apache.hadoop.fs.azurebfs.AbfsConfiguration` for the full list
of configuration options and their default values.

### <a name="flushconfigoptions"></a> Flush Options

#### <a name="abfsflushconfigoptions"></a> 1. Azure Blob File System Flush Options
Config `fs.azure.enable.flush` provides an option to render ABFS flush APIs -
 HFlush() and HSync() to be no-op. By default, this
config will be set to true.

Both the APIs will ensure that data is persisted.

#### <a name="outputstreamflushconfigoptions"></a> 2. OutputStream Flush Options
Config `fs.azure.disable.outputstream.flush` provides an option to render
OutputStream Flush() API to be a no-op in AbfsOutputStream. By default, this
config will be set to true.

Hflush() being the only documented API that can provide persistent data
transfer, Flush() also attempting to persist buffered data will lead to
performance issues.

## <a name="troubleshooting"></a> Troubleshooting

The problems associated with the connector usually come down to, in order

1. Classpath.
1. Network setup (proxy etc.).
1. Authentication and Authorization.
1. Anything else.

If you log `org.apache.hadoop.fs.azurebfs.services` at `DEBUG` then you will
see more details about any request which is failing.

One useful tool for debugging connectivity is the [cloudstore storediag utility](https://github.com/steveloughran/cloudstore/releases).

This validates the classpath, the settings, then tries to work with the filesystem.

```bash
bin/hadoop jar cloudstore-0.1-SNAPSHOT.jar storediag abfs://container@account.dfs.core.windows.net/
```

1. If the `storediag` command cannot work with an abfs store, nothing else is likely to.
1. If the `storediag` store does successfully work, that does not guarantee that the classpath
or configuration on the rest of the cluster is also going to work, especially
in distributed applications. But it is at least a start.

### `ClassNotFoundException: org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem`

The `hadoop-azure` JAR is not on the classpah.

```
java.lang.RuntimeException: java.lang.ClassNotFoundException:
    Class org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem not found
  at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:2625)
  at org.apache.hadoop.fs.FileSystem.getFileSystemClass(FileSystem.java:3290)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3322)
  at org.apache.hadoop.fs.FileSystem.access$200(FileSystem.java:136)
  at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:3373)
  at org.apache.hadoop.fs.FileSystem$Cache.get(FileSystem.java:3341)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:491)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
Caused by: java.lang.ClassNotFoundException:
    Class org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem not found
  at org.apache.hadoop.conf.Configuration.getClassByName(Configuration.java:2529)
  at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:2623)
  ... 16 more
```

Tip: if this is happening on the command line, you can turn on debug logging
of the hadoop scripts:

```bash
export HADOOP_SHELL_SCRIPT_DEBUG=true
```

If this is happening on an application running within the cluster, it means
the cluster (somehow) needs to be configured so that the `hadoop-azure`
module and dependencies are on the classpath of deployed applications.

### `ClassNotFoundException: com.microsoft.azure.storage.StorageErrorCode`

The `azure-storage` JAR is not on the classpath.

### `Server failed to authenticate the request`

The request wasn't authenticated while using the default shared-key
authentication mechanism.

```
Operation failed: "Server failed to authenticate the request.
 Make sure the value of Authorization header is formed correctly including the signature.",
 403, HEAD, https://account.dfs.core.windows.net/container2?resource=filesystem&timeout=90
  at org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation.execute(AbfsRestOperation.java:135)
  at org.apache.hadoop.fs.azurebfs.services.AbfsClient.getFilesystemProperties(AbfsClient.java:209)
  at org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.getFilesystemProperties(AzureBlobFileSystemStore.java:259)
  at org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem.fileSystemExists(AzureBlobFileSystem.java:859)
  at org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem.initialize(AzureBlobFileSystem.java:110)
```

Causes include:

* Your credentials are incorrect.
* Your shared secret has expired. in Azure, this happens automatically
* Your shared secret has been revoked.
* host/VM clock drift means that your client's clock is out of sync with the
Azure servers —the call is being rejected as it is either out of date (considered a replay)
or from the future. Fix: Check your clocks, etc.

### `Configuration property _something_.dfs.core.windows.net not found`

There's no `fs.azure.account.key.` entry in your cluster configuration declaring the
access key for the specific account, or you are using the wrong URL

```
$ hadoop fs -ls abfs://container@abfswales2.dfs.core.windows.net/

ls: Configuration property abfswales2.dfs.core.windows.net not found.
```

* Make sure that the URL is correct
* Add the missing account key.


### `No such file or directory when trying to list a container`

There is no container of the given name. Either it has been mistyped
or the container needs to be created.

```
$ hadoop fs -ls abfs://container@abfswales1.dfs.core.windows.net/

ls: `abfs://container@abfswales1.dfs.core.windows.net/': No such file or directory
```

* Make sure that the URL is correct
* Create the container if needed

### "HTTP connection to https://login.microsoftonline.com/_something_ failed for getting token from AzureAD. Http response: 200 OK"

+ it has a content-type `text/html`, `text/plain`, `application/xml`

The OAuth authentication page didn't fail with an HTTP error code, but it didn't return JSON either

```
$ bin/hadoop fs -ls abfs://container@abfswales1.dfs.core.windows.net/

 ...

ls: HTTP Error 200;
  url='https://login.microsoftonline.com/02a07549-0a5f-4c91-9d76-53d172a638a2/oauth2/authorize'
  AADToken: HTTP connection to
  https://login.microsoftonline.com/02a07549-0a5f-4c91-9d76-53d172a638a2/oauth2/authorize
  failed for getting token from AzureAD.
  Unexpected response.
  Check configuration, URLs and proxy settings.
  proxies=none;
  requestId='dd9d526c-8b3d-4b3f-a193-0cf021938600';
  contentType='text/html; charset=utf-8';
```

Likely causes are configuration and networking:

1. Authentication is failing, the caller is being served up the Azure Active Directory
signon page for humans, even though it is a machine calling.
1. The URL is wrong —it is pointing at a web page unrelated to OAuth2.0
1. There's a proxy server in the way trying to return helpful instructions.

## <a name="testing"></a> Testing ABFS

See the relevant section in [Testing Azure](testing_azure.html).
