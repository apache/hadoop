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

# Hadoop Azure Support: ABFS  - Azure Data Lake Storage Gen2

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

See also:
* [FNS (non-HNS)](./fns_blob.html)
* [Legacy-Deprecated-WASB](./wasb.html)
* [Testing](./testing_azure.html)

## <a name="features"></a> Features of the ABFS connector.

* Supports reading and writing data stored in an Azure Blob Storage account.
* *Fully Consistent* view of the storage across all clients.
* Can read data written through the ` deprecated wasb:` connector.
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
* **Blob**:  A file of any type and size. In Hadoop, files are stored in blobs.
  The internal implementation also uses blobs to persist the file system
  hierarchy and other metadata.

The ABFS connector connects to classic containers, or those created
with Hierarchical Namespaces.

## <a name="namespaces"></a> Hierarchical Namespaces (and WASB Compatibility)

A key aspect of ADLS Gen 2 is its support for
[hierarchical namespaces](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace)
These are effectively directories and offer high performance rename and delete operations
— something which makes a significant improvement in performance in query engines
writing data to, including MapReduce, Spark, Hive, as well as DistCp.

This feature is only available if the container was created with "namespace"
support.

You enable namespace support when creating a new Storage Account,
by checking the "Hierarchical Namespace" option in the Portal UI, or, when
creating through the command line, using the option `--hierarchical-namespace true`

_You cannot enable Hierarchical Namespaces on an existing storage account_

_**Containers in a storage account with Hierarchical Namespaces are
not (currently) readable through the `deprecated wasb:` connector.**_

Some of the `az storage` command line commands fail too, for example:

```bash
$ az storage container list --account-name abfswales1
```
Output:
```
Blob API is not yet supported for hierarchical namespace accounts. ErrorCode: BlobApiNotYetSupportedForHierarchicalNamespaceAccounts
```

### <a name="creating"></a> Creating an Azure Storage Account

The best documentation on getting started with Azure Datalake Gen2 with the
abfs connector is [Using Azure Data Lake Storage Gen2 with Azure HDInsight clusters](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-use-hdi-cluster)

It includes instructions to create it from [the Azure command line tool](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest),
which can be installed on Windows, MacOS (via Homebrew) and Linux (apt or yum).

The [az storage](https://docs.microsoft.com/en-us/cli/azure/storage?view=azure-cli-latest) subcommand
handles all storage commands, [az storage account create](https://docs.microsoft.com/en-us/cli/azure/storage/account?view=azure-cli-latest#az-storage-account-create)
does the creation.

Until the ADLS gen2 API support is finalized, you need to add an extension
to the ADLS command.
```bash
az extension add --name storage-preview
```

Check that all is well by verifying that the usage command includes `--hierarchical-namespace`:
```bash
$  az storage account
```
Output:
```
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
     **[--hierarchical-namespace [{true,false}]]**
     [--bypass {None,Logging,Metrics,AzureServices} [{None,Logging,Metrics,AzureServices} ...]]
     [--default-action {Allow,Deny}]
     [--assign-identity]
     [--subscription _SUBSCRIPTION]
```

You can list locations from `az account list-locations`, which lists the
name to refer to in the `--location` argument:
```bash
$ az account list-locations -o table
```
It would list locations in a table format.

Sample output:
```
DisplayName          Latitude    Longitude    Name
-------------------  ----------  -----------  ------------------
East Asia            22.267      114.188      eastasia
Southeast Asia       1.283       103.833      southeastasia
....                  ....        ......         .......
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
  "connectionString": "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=abfswales1;AccountKey=ACCOUNT_KEY_VALUE"
}
```

You then need to add the access key to your `core-site.xml`, JCEKs file or
use your cluster management tool to set it the option `fs.azure.account.key.STORAGE-ACCOUNT`
to this value.
```XML
<property>
  <name>fs.azure.account.key.abfswales1.dfs.core.windows.net</name>
  <value>ACCOUNT_KEY_VALUE</value>
</property>
```

#### Creation through the Azure Portal

Creation through the portal is covered in [Quickstart: Create an Azure Data Lake Storage Gen2 storage account](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-quickstart-create-account)

**Key Steps**

1. Create a new Storage Account in a location which suits you.
1. "Basics" Tab: select "StorageV2".
1. "Advanced" Tab: enable "Hierarchical Namespace".

You have now created your storage account. Next, get the key for "Shared Key" authentication (default authentication type).

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
 `fs.azure.createRemoteFileSystemDuringInitialization` to `true`. Though the
  same is not supported when AuthType is SAS.

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

1. With the storage account's authentication secret in the configuration: "Shared Key".
2. Using OAuth 2.0 tokens of one form or another.
3. Deployed in-Azure with the Azure VMs providing OAuth 2.0 tokens to the application, "Managed Instance".
4. Using Shared Access Signature (SAS) tokens provided by a custom implementation of the SASTokenProvider interface.
5. By directly configuring a fixed Shared Access Signature (SAS) token in the account configuration settings files.

Note: SAS Based Authentication should be used only with HNS Enabled accounts.

What can be changed is what secrets/credentials are used to authenticate the caller.

The authentication mechanism is set in `fs.azure.account.auth.type` (or the
account specific variant). The possible values are SharedKey, OAuth, Custom
and SAS. For the various OAuth options use the config `fs.azure.account.oauth.provider.type`. Following are the implementations supported
ClientCredsTokenProvider, UserPasswordTokenProvider, MsiTokenProvider,
RefreshTokenBasedTokenProvider and WorkloadIdentityTokenProvider. An IllegalArgumentException is thrown if
the specified provider type is not one of the supported.

All secrets can be stored in JCEKS files. These are encrypted and password
protected —use them or a compatible Hadoop Key Management Store wherever
possible

### <a name="aad-token-fetch-retry-logic"></a> AAD Token fetch retries

The exponential retry policy used for the AAD token fetch retries can be tuned
with the following configurations.
* `fs.azure.oauth.token.fetch.retry.max.retries`: Sets the maximum number of
 retries. Default value is 5.
* `fs.azure.oauth.token.fetch.retry.min.backoff.interval`: Minimum back-off
  interval. Added to the retry interval computed from delta backoff. By
   default this is set as 0. Set the interval in milli seconds.
* `fs.azure.oauth.token.fetch.retry.max.backoff.interval`: Maximum back-off
interval. Default value is 60000 (sixty seconds). Set the interval in milli
seconds.
* `fs.azure.oauth.token.fetch.retry.delta.backoff`: Back-off interval between
retries. Multiples of this timespan are used for subsequent retry attempts
 . The default value is 2.

### <a name="shared-key-auth"></a> Default: Shared Key

This is the simplest authentication mechanism of account + password.

The account name is inferred from the URL;
the password, "key", retrieved from the XML/JCECKs configuration files.

```xml
<property>
  <name>fs.azure.account.auth.type.ACCOUNT_NAME.dfs.core.windows.net</name>
  <value>SharedKey</value>
  <description>
  </description>
</property>
<property>
  <name>fs.azure.account.key.ACCOUNT_NAME.dfs.core.windows.net</name>
  <value>ACCOUNT_KEY</value>
  <description>
  The secret password. Never share these.
  </description>
</property>
```

*Note*: The source of the account key can be changed through a custom key provider;
one needs to execute a shell script to retrieve it.

A custom key provider class can be provided with the config
`fs.azure.account.keyprovider`. If a key provider class is specified the same
will be used to get account key. Otherwise the Simple key provider will be used
which will use the key specified for the config `fs.azure.account.key`.

To retrieve using shell script, specify the path to the script for the config
`fs.azure.shellkeyprovider.script`. ShellDecryptionKeyProvider class use the
script specified to retrieve the key.

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
  <value>TOKEN_ENDPOINT</value>
  <description>
  URL of OAuth endpoint
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.id</name>
  <value>CLIENT_ID</value>
  <description>
  Client ID
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.secret</name>
  <value>CLIENT_SECRET</value>
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
  <value>TOKEN_ENDPOINT</value>
  <description>
  URL of OAuth 2.0 endpoint
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.user.name</name>
  <value>USERNAME_VALUE</value>
  <description>
  username
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.user.password</name>
  <value>USER_PASSWORD</value>
  <description>
  password for account
  </description>
</property>
```

### <a name="oauth-refresh-token"></a> OAuth 2.0: Refresh Token

With an existing Oauth 2.0 token, make a request to the Active Directory endpoint
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
  <value>REFRESH_TOKEN</value>
  <description>
  Refresh token
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.refresh.endpoint</name>
  <value>REFRESH_ENDPOINT</value>
  <description>
  Refresh token endpoint
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.id</name>
  <value>CLIENT_ID</value>
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
  <value>MSI_TENANT_VALUE</value>
  <description>
  Optional MSI Tenant ID
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.msi.endpoint</name>
  <value>TOKEN_ENDPOINT</value>
  <description>
   MSI endpoint
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.id</name>
  <value>CLIENT_ID</value>
  <description>
  Optional Client ID
  </description>
</property>
```

### <a name="workload-identity"></a> Azure Workload Identity

[Azure Workload Identities](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview), formerly "Azure AD pod identity".

OAuth 2.0 tokens are written to a file that is only accessible
from the executing pod (`/var/run/secrets/azure/tokens/azure-identity-token`).
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
  <value>org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider</value>
  <description>
  Use Workload Identity for issuing OAuth tokens
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.msi.tenant</name>
  <value>${env.AZURE_TENANT_ID}</value>
  <description>
  Optional MSI Tenant ID
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.id</name>
  <value>${env.AZURE_CLIENT_ID}</value>
  <description>
  Optional Client ID
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.token.file</name>
  <value>${env.AZURE_FEDERATED_TOKEN_FILE}</value>
  <description>
  Token file path
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
  <value>PROVIDER_TYPE</value>
  <description>
  classname of Custom Authentication Provider
  </description>
</property>
```

The declared class must implement `org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee`
and optionally `org.apache.hadoop.fs.azurebfs.extensions.BoundDTExtension`.

The declared class also holds responsibility to implement retry logic while fetching access tokens.

### <a name="delegationtokensupportconfigoptions"></a> Delegation Token Provider

A delegation token provider supplies the ABFS connector with delegation tokens,
helps renew and cancel the tokens by implementing the
CustomDelegationTokenManager interface.

```xml
<property>
  <name>fs.azure.enable.delegation.token</name>
  <value>true</value>
  <description>Make this true to use delegation token provider</description>
</property>
<property>
  <name>fs.azure.delegation.token.provider.type</name>
  <value>{fully-qualified-class-name-for-implementation-of-CustomDelegationTokenManager-interface}</value>
</property>
```
In case delegation token is enabled, and the config `fs.azure.delegation.token
.provider.type` is not provided then an IlleagalArgumentException is thrown.

### Shared Access Signature (SAS) Token Provider

A shared access signature (SAS) provides secure delegated access to resources in
your storage account. With a SAS, you have granular control over how a client can access your data.
To know more about how SAS Authentication works refer to
[Grant limited access to Azure Storage resources using shared access signatures (SAS)](https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview)

There are three types of SAS supported by Azure Storage:
- [User Delegation SAS](https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas): Recommended for use with ABFS Driver with HNS Enabled ADLS Gen2 accounts. It is Identity based SAS that works at blob/directory level)
- [Service SAS](https://learn.microsoft.com/en-us/rest/api/storageservices/create-service-sas): Global and works at container level.
- [Account SAS](https://learn.microsoft.com/en-us/rest/api/storageservices/create-account-sas): Global and works at account level.

#### Known Issues With SAS
- SAS Based Authentication works only with HNS Enabled ADLS Gen2 Accounts which
is a recommended account type to be used with ABFS.
- Certain root level operations are known to fail with SAS Based Authentication.

#### Using User Delegation SAS with ABFS

- **Description**: ABFS allows you to implement your custom SAS Token Provider
that uses your identity to create a user delegation key which then can be used to
create SAS instead of storage account key. The declared class must implement
`org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider`.

- **Configuration**: To use this method with ABFS Driver, specify the following properties in your `core-site.xml` file:
    1. Authentication Type:
        ```xml
        <property>
          <name>fs.azure.account.auth.type</name>
          <value>SAS</value>
        </property>
        ```

    1. Custom SAS Token Provider Class:
        ```xml
        <property>
          <name>fs.azure.sas.token.provider.type</name>
          <value>CUSTOM_SAS_TOKEN_PROVIDER_CLASS</value>
        </property>
        ```

    Replace `CUSTOM_SAS_TOKEN_PROVIDER_CLASS` with fully qualified class name of
your custom token provider implementation. Depending upon the implementation you
might need to specify additional configurations that are required by your custom
implementation.

- **Example**: ABFS Hadoop Driver provides a [MockDelegationSASTokenProvider](https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-azure/src/test/java/org/apache/hadoop/fs/azurebfs/extensions/MockDelegationSASTokenProvider.java)
implementation that can be used as an example on how to implement your own custom
SASTokenProvider. This requires the Application credentials to be specifed using
the following configurations apart from above two:

    1. App Service Principle Tenant Id:
        ```xml
        <property>
          <name>fs.azure.test.app.service.principal.tenant.id</name>
          <value>TENANT_ID</value>
        </property>
        ```
    1. App Service Principle Object Id:
        ```xml
        <property>
          <name>fs.azure.test.app.service.principal.object.id</name>
          <value>OBJECT_ID</value>
        </property>
        ```
    1. App Id:
        ```xml
        <property>
          <name>fs.azure.test.app.id</name>
          <value>APPLICATION_ID</value>
        </property>
        ```
    1. App Secret:
        ```xml
        <property>
          <name>fs.azure.test.app.secret</name>
          <value>APPLICATION_SECRET</value>
        </property>
        ```

- **Security**: More secure than Shared Key and allows granting limited access
to data without exposing the access key. Recommended to be used only with HNS Enabled,
ADLS Gen 2 storage accounts.

#### Using Account/Service SAS with ABFS

- **Description**: ABFS allows user to use Account/Service SAS for authenticating
requests. User can specify them as fixed SAS Token to be used across all the requests.

- **Configuration**: To use this method with ABFS Driver, specify the following properties in your `core-site.xml` file:

    1. Authentication Type:
        ```xml
        <property>
          <name>fs.azure.account.auth.type</name>
          <value>SAS</value>
        </property>
        ```

  2. Account SAS (Fixed SAS Token at Account Level):
        ```xml
        <property>
          <name>fs.azure.sas.fixed.token.ACCOUNT_NAME</name>
          <value>FIXED_ACCOUNT_SAS_TOKEN</value>
        </property>
        ```

    - Replace `FIXED_ACCOUNT_SAS_TOKEN` with fixed Account/Service SAS. You can also
      generate SAS from Azure portal. Account -> Security + Networking -> Shared Access Signature

    3. Service  SAS (Fixed SAS Token at Container Level):
        ```xml
           <property>
             <name>fs.azure.sas.fixed.token.CONTAINER_NAME.ACCOUNT_NAME</name>
             <value>FIXED_SAS_TOKEN</value>
           </property>
           ```

    - Replace `FIXED_SERVICE_SAS_TOKEN` with fixed Service SAS. You can also
      generate SAS from Azure portal. Account -> Data storage -> Containers ->
      right click on your container and select generate SAS ->
      Give valid permissions and expiry time -> Click on generate SAS and copy
      the SAS token.


- **Security**: Account/Service SAS requires account keys to be used which makes
  them less secure. There is no scope of having delegated access to different users.

*Note:*
- Preference order for SAS will be:
    - fs.azure.sas.token.provider.type
    - fs.azure.sas.fixed.token.CONTAINER_NAME.ACCOUNT_NAME
    - fs.azure.sas.fixed.token.ACCOUNT_NAME
    - fs.azure.sas.fixed.token

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
* `SASTokenProvider`: allows for custom provision of Azure Storage Shared Access Signature (SAS) tokens.
* `CustomTokenProviderAdaptee`: allows for custom provision of
Azure OAuth tokens.
* `KeyProvider`.

Consult the source in `org.apache.hadoop.fs.azurebfs.extensions`
and all associated tests to see how to make use of these extension points.

_Warning_ These extension points are unstable.

### <a href="networking"></a>Networking Layer:

ABFS Driver can use the following networking libraries:
- ApacheHttpClient:
  -  <a href = "https://hc.apache.org/httpcomponents-client-4.5.x/index.html">Library Documentation</a>.
- JDK networking library:
  - <a href="https://docs.oracle.com/javase/8/docs/api/java/net/HttpURLConnection.html">Library documentation</a>.
  - Default networking library.

The networking library can be configured using the configuration `fs.azure.networking.library`
while initializing the filesystem.
Following are the supported values:
- `JDK_HTTP_URL_CONNECTION` : Use JDK networking library [Default]
- `APACHE_HTTP_CLIENT` : Use Apache HttpClient

#### <a href="ahc_networking_conf"></a>ApacheHttpClient networking layer configuration Options:

Following are the configuration options for ApacheHttpClient networking layer that
can be provided at the initialization of the filesystem:
1. `fs.azure.apache.http.client.idle.connection.ttl`:
   1. Maximum idle time in milliseconds for a connection to be kept alive in the connection pool.
      If the connection is not reused within the time limit, the connection shall be closed.
   2. Default value: 5000 milliseconds.
2. `fs.azure.apache.http.client.max.cache.connection.size`:
   1. Maximum number of connections that can be cached in the connection pool for
      a filesystem instance. Total number of concurrent connections has no limit.
   2. Default value: 5.
3. `fs.azure.apache.http.client.max.io.exception.retries`:
   1. Maximum number of times the client will retry on IOExceptions for a single request
      with ApacheHttpClient networking-layer. Breach of this limit would turn off
      the future uses of the ApacheHttpClient library in the current JVM instance.
   2. Default value: 3.

#### <a href="ahc_classpath"></a> ApacheHttpClient classpath requirements:

ApacheHttpClient is a `compile` maven dependency in hadoop-azure and would be
included in the hadoop-azure jar. For using hadoop-azure with ApacheHttpClient no
additional information is required in the classpath.

## <a href="options"></a> Other configuration options

Consult the javadocs for `org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys`,
`org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations` and
`org.apache.hadoop.fs.azurebfs.AbfsConfiguration` for the full list
of configuration options and their default values.

### <a name="clientcorrelationoptions"></a> Client Correlation Options

#### <a name="clientcorrelationid"></a> 1. Client CorrelationId Option

Config `fs.azure.client.correlationid` provides an option to correlate client
requests using this client-provided identifier. This Id will be visible in Azure
Storage Analytics logs in the `request-id-header` field.
Reference: [Storage Analytics log format](https://docs.microsoft.com/en-us/rest/api/storageservices/storage-analytics-log-format)

This config accepts a string which can be maximum of 72 characters and should
contain alphanumeric characters and/or hyphens only. Defaults to empty string if
input is invalid.

#### <a name="tracingcontextformat"></a> 1. Correlation IDs Display Options

Config `fs.azure.tracingcontext.format` provides an option to select the format
of IDs included in the `request-id-header`. This config accepts a String value
corresponding to the following enum options.
  `SINGLE_ID_FORMAT` : clientRequestId
  `ALL_ID_FORMAT` : all IDs (default)
  `TWO_ID_FORMAT` : clientCorrelationId:clientRequestId

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

### <a name="100continueconfigoptions"></a> Hundred Continue Options

`fs.azure.account.expect.header.enabled`: This configuration parameter is used
to specify whether you wish to send a expect 100 continue header with each
append request or not. It is configured to true by default. This flag configures
the client to check with the Azure store before uploading a block of data from
an output stream. This allows the client to throttle back gracefully -before
actually attempting to upload the block. In experiments this provides
significant throughput improvements under heavy load. For more information :
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Expect


### <a name="accountlevelthrottlingoptions"></a> Account level throttling Options

`fs.azure.account.operation.idle.timeout`: This value specifies the time after which the timer for the analyzer (read or
write) should be paused until no new request is made again. The default value for the same is 60 seconds.

### <a name="hnscheckconfigoptions"></a> HNS Check Options
Config `fs.azure.account.hns.enabled` provides an option to specify whether
 the storage account is HNS enabled or not. In case the config is not provided,
  a server call is made to check the same.

### <a name="flushconfigoptions"></a> Access Options
Config `fs.azure.enable.check.access` needs to be set true to enable
 the AzureBlobFileSystem.access().

### <a name="idempotency"></a> Operation Idempotency

Requests failing due to server timeouts and network failures will be retried.
PUT/POST operations are idempotent and need no specific handling.

Delete is considered to be idempotent by default if the target does not exist on
retry.

### <a name="featureconfigoptions"></a> Primary User Group Options
The group name which is part of FileStatus and AclStatus will be set the same as
the username if the following config is set to true
`fs.azure.skipUserGroupMetadataDuringInitialization`.

### <a name="ioconfigoptions"></a> IO Options
The following configs are related to read and write operations.

`fs.azure.io.retry.max.retries`: Sets the number of retries for IO operations.
Currently this is used only for the server call retry logic. Used within
`AbfsClient` class as part of the ExponentialRetryPolicy. The value should be
greater than or equal to 0.

`fs.azure.io.retry.min.backoff.interval`: Sets the minimum backoff interval for
retries of IO operations. Currently this is used only for the server call retry
logic. Used within `AbfsClient` class as part of the ExponentialRetryPolicy. This
value indicates the smallest interval (in milliseconds) to wait before retrying
an IO operation. The default value is 500 milliseconds.

`fs.azure.io.retry.max.backoff.interval`: Sets the maximum backoff interval for
retries of IO operations. Currently this is used only for the server call retry
logic. Used within `AbfsClient` class as part of the ExponentialRetryPolicy. This
value indicates the largest interval (in milliseconds) to wait before retrying
an IO operation. The default value is 25000 (25 seconds).

`fs.azure.io.retry.backoff.interval`: Sets the default backoff interval for
retries of IO operations. Currently this is used only for the server call retry
logic. Used within `AbfsClient` class as part of the ExponentialRetryPolicy. This
value is used to compute a random delta between 80% and 120% of the specified
value. This random delta is then multiplied by an exponent of the current IO
retry number (i.e., the default is multiplied by `2^(retryNum - 1)`) and then
contstrained within the range of [`fs.azure.io.retry.min.backoff.interval`,
`fs.azure.io.retry.max.backoff.interval`] to determine the amount of time to
wait before the next IO retry attempt. The default value is 500 milliseconds.

`fs.azure.write.request.size`: To set the write buffer size. Specify the value
in bytes. The value should be between 16384 to 104857600 both inclusive (16 KB
to 100 MB). The default value will be 8388608 (8 MB).

`fs.azure.read.request.size`: To set the read buffer size.Specify the value in
bytes. The value should be between 16384 to 104857600 both inclusive (16 KB to
100 MB). The default value will be 4194304 (4 MB).

`fs.azure.read.alwaysReadBufferSize`: Read request size configured by
`fs.azure.read.request.size` will be honoured only when the reads done are in
sequential pattern. When the read pattern is detected to be random, read size
will be same as the buffer length provided by the calling process.
This config when set to true will force random reads to also read in same
request sizes as sequential reads. This is a means to have same read patterns
as of ADLS Gen1, as it does not differentiate read patterns and always reads by
the configured read request size. The default value for this config will be
false, where reads for the provided buffer length is done when random read
pattern is detected.

`fs.azure.readaheadqueue.depth`: Sets the readahead queue depth in
AbfsInputStream. In case the set value is negative the read ahead queue depth
will be set as Runtime.getRuntime().availableProcessors(). By default the value
will be 2. To disable readaheads, set this value to 0. If your workload is
 doing only random reads (non-sequential) or you are seeing throttling, you
  may try setting this value to 0.

`fs.azure.read.readahead.blocksize`: To set the read buffer size for the read
aheads. Specify the value in bytes. The value should be between 16384 to
104857600 both inclusive (16 KB to 100 MB). The default value will be
4194304 (4 MB).

`fs.azure.buffered.pread.disable`: By default the positional read API will do a
seek and read on input stream. This read will fill the buffer cache in
AbfsInputStream and update the cursor positions. If this optimization is true
it will skip usage of buffer and do a lock free REST call for reading from blob.
This optimization is very much helpful for HBase kind of short random read over
a shared AbfsInputStream instance.
Note: This is not a config which can be set at cluster level. It can be used as
an option on FutureDataInputStreamBuilder.
See FileSystem#openFile(Path path)

To run under limited memory situations configure the following. Especially
when there are too many writes from the same process.

`fs.azure.write.max.concurrent.requests`: To set the maximum concurrent
 write requests from an AbfsOutputStream instance  to server at any point of
 time. Effectively this will be the threadpool size within the
 AbfsOutputStream instance. Set the value in between 1 to 8 both inclusive.

`fs.azure.write.max.requests.to.queue`: To set the maximum write requests
 that can be queued. Memory consumption of AbfsOutputStream instance can be
 tuned with this config considering each queued request holds a buffer. Set
 the value 3 or 4 times the value set for s.azure.write.max.concurrent.requests.

`fs.azure.analysis.period`: The time after which sleep duration is recomputed after analyzing metrics. The default value
for the same is 10 seconds.

### <a name="securityconfigoptions"></a> Security Options
`fs.azure.always.use.https`: Enforces to use HTTPS instead of HTTP when the flag
is made true. Irrespective of the flag, `AbfsClient` will use HTTPS if the secure
scheme (ABFSS) is used or OAuth is used for authentication. By default this will
be set to true.

`fs.azure.ssl.channel.mode`: Initializing DelegatingSSLSocketFactory with the
specified SSL channel mode. Value should be of the enum
DelegatingSSLSocketFactory.SSLChannelMode. The default value will be
DelegatingSSLSocketFactory.SSLChannelMode.Default.

### <a name="encryptionconfigoptions"></a> Encryption Options
Only one of the following two options can be configured. If config values of
both types are set, ABFS driver will throw an exception. If using the global
key type, ensure both pre-computed values are provided.

#### <a name="globalcpkconfigoptions"></a> Customer-Provided Global Key
A global encryption key can be configured by providing the following
pre-computed values. The key will be applied to any new files created post
setting the configuration, and will be required in the requests to read ro
modify the contents of the files.

`fs.azure.encryption.encoded.client-provided-key`: The Base64 encoded version
of the 256-bit encryption key.

`fs.azure.encryption.encoded.client-provided-key-sha`: The Base64 encoded
version of the SHA256 has of the 256-bit encryption key.

#### <a name="encryptioncontextconfigoptions"></a> Encryption Context Provider

ABFS driver supports an interface called `EncryptionContextProvider` that
can be used as a plugin for clients to provide custom implementations for
the encryption framework. This framework allows for an `encryptionContext`
and an `encryptionKey` to be generated by the EncryptionContextProvider for
a file to be created. The server keeps track of the encryptionContext for
each file. To perform subsequent operations such as read on the encrypted file,
ABFS driver will fetch the corresponding encryption key from the
EncryptionContextProvider implementation by providing the encryptionContext
string retrieved from a GetFileStatus request to the server.

`fs.azure.encryption.context.provider.type`: The canonical name of the class
implementing EncryptionContextProvider.

### <a name="serverconfigoptions"></a> Server Options
`fs.azure.io.read.tolerate.concurrent.append`: When the config is made true, the
If-Match header sent to the server for read calls will be set as * otherwise the
same will be set with ETag. This is basically a mechanism in place to handle the
reads with optimistic concurrency.
Please refer the following links for further information.
1. https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/read
2. https://azure.microsoft.com/de-de/blog/managing-concurrency-in-microsoft-azure-storage-2/

`fs.azure.list.max.results`: listStatus API fetches the FileStatus information
from server in a page by page manner. The config is used to set the maxResults URI
param which sets the page size(maximum results per call). The value should
be >  0. By default, this will be 5000. Server has a maximum value for this
parameter as 5000. So even if the config is above 5000 the response will only
contain 5000 entries. Please refer the following link for further information.
https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list

`fs.azure.enable.checksum.validation`: When the config is set to true, Content-MD5
headers are sent to the server for read and append calls. This provides a way
to verify the integrity of data during transport. This will have performance
impact due to MD5 Hash re-computation on Client and Server side. Please refer
to the Azure documentation for
[Read](https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/read)
and [Append](https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update)
APIs for more details

### <a name="throttlingconfigoptions"></a> Throttling Options
ABFS driver has the capability to throttle read and write operations to achieve
maximum throughput by minimizing errors. The errors occur when the account
ingress or egress limits are exceeded and, the server-side throttles requests.
Server-side throttling causes the retry policy to be used, but the retry policy
sleeps for long periods of time causing the total ingress or egress throughput
to be as much as 35% lower than optimal. The retry policy is also after the
fact, in that it applies after a request fails. On the other hand, the
client-side throttling implemented here happens before requests are made and
sleeps just enough to minimize errors, allowing optimal ingress and/or egress
throughput. By default the throttling mechanism is enabled in the driver. The
same can be disabled by setting the config `fs.azure.enable.autothrottling`
to false.

### <a name="renameconfigoptions"></a> Rename Options
`fs.azure.atomic.rename.key`: Directories for atomic rename support can be
specified comma separated in this config. The driver prints the following
warning log if the source of the rename belongs to one of the configured
directories. "The atomic rename feature is not supported by the ABFS scheme
; however, rename, create and delete operations are atomic if Namespace is
enabled for your Azure Storage account."
The directories can be specified as comma separated values. By default the value
is "/hbase"

### <a name="infiniteleaseoptions"></a> Infinite Lease Options
`fs.azure.infinite-lease.directories`: Directories for infinite lease support
can be specified comma separated in this config. By default, multiple
clients will be able to write to the same file simultaneously. When writing
to files contained within the directories specified in this config, the
client will obtain a lease on the file that will prevent any other clients
from writing to the file. When the output stream is closed, the lease will be
released. To revoke a client's write access for a file, the
AzureBlobFilesystem breakLease method may be called. If the client dies
before the file can be closed and the lease released, breakLease will need to
be called before another client will be able to write to the file.

`fs.azure.lease.threads`: This is the size of the thread pool that will be
used for lease operations for infinite lease directories. By default the value
is 0, so it must be set to at least 1 to support infinite lease directories.

### <a name="perfoptions"></a> Perf Options

#### <a name="abfstracklatencyoptions"></a> 1. HTTP Request Tracking Options
If you set `fs.azure.abfs.latency.track` to `true`, the module starts tracking the
performance metrics of ABFS HTTP traffic. To obtain these numbers on your machine
or cluster, you will also need to enable debug logging for the `AbfsPerfTracker`
class in your `log4j` config. A typical perf log line appears like:

```
h=KARMA t=2019-10-25T20:21:14.518Z a=abfstest01.dfs.core.windows.net
c=abfs-testcontainer-84828169-6488-4a62-a875-1e674275a29f cr=delete ce=deletePath
r=Succeeded l=32 ls=32 lc=1 s=200 e= ci=95121dae-70a8-4187-b067-614091034558
ri=97effdcf-201f-0097-2d71-8bae00000000 ct=0 st=0 rt=0 bs=0 br=0 m=DELETE
u=https%3A%2F%2Fabfstest01.dfs.core.windows.net%2Ftestcontainer%2Ftest%3Ftimeout%3D90%26recursive%3Dtrue
```

The fields have the following definitions:

`h`: host name
`t`: time when this request was logged
`a`: Azure storage account name
`c`: container name
`cr`: name of the caller method
`ce`: name of the callee method
`r`: result (Succeeded/Failed)
`l`: latency (time spent in callee)
`ls`: latency sum (aggregate time spent in caller; logged when there are multiple
callees; logged with the last callee)
`lc`: latency count (number of callees; logged when there are multiple callees;
logged with the last callee)
`s`: HTTP Status code
`e`: Error code
`ci`: client request ID
`ri`: server request ID
`ct`: connection time in milliseconds
`st`: sending time in milliseconds
`rt`: receiving time in milliseconds
`bs`: bytes sent
`br`: bytes received
`m`: HTTP method (GET, PUT etc)
`u`: Encoded HTTP URL

Note that these performance numbers are also sent back to the ADLS Gen 2 API endpoints
in the `x-ms-abfs-client-latency` HTTP headers in subsequent requests. Azure uses these
settings to track their end-to-end latency.

### <a name="drivermetricoptions"></a> Driver Metric Options

Config `fs.azure.metric.format` provides an option to select the format of IDs included in the `header` for metrics.
This config accepts a String value corresponding to the following enum options.
`INTERNAL_METRIC_FORMAT` : backoff + footer metrics
`INTERNAL_BACKOFF_METRIC_FORMAT` : backoff metrics
`INTERNAL_FOOTER_METRIC_FORMAT` : footer metrics
`EMPTY` : default

`fs.azure.metric.account.name`: This configuration parameter is used to specify the name of the account which will be
used to push the metrics to the backend. We can configure a separate account to push metrics to the store or use the
same for as the existing account on which other requests are made.

```xml

<property>
    <name>fs.azure.metric.account.name</name>
    <value>METRICACCOUNTNAME.dfs.core.windows.net</value>
</property>
```

`fs.azure.metric.account.key`: This is the access key for the storage account used for pushing metrics to the store.

```xml

<property>
    <name>fs.azure.metric.account.key</name>
    <value>ACCOUNTKEY</value>
</property>
```

`fs.azure.metric.uri`: This configuration provides the uri in the format of 'https://`<accountname>`
.dfs.core.windows.net/`<containername>`'. This should be a part of the config in order to prevent extra calls to create
the filesystem. We use an existing filsystem to push the metrics.

```xml

<property>
    <name>fs.azure.metric.uri</name>
    <value>https://METRICACCOUNTNAME.dfs.core.windows.net/CONTAINERNAME</value>
</property>
```

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

### `java.io.IOException: The ownership on the staging directory /tmp/hadoop-yarn/staging/user1/.staging is not as expected. It is owned by <principal_id>. The directory must be owned by the submitter user1 or user1`

When using [Azure Managed Identities](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview), the files/directories in ADLS Gen2 by default will be owned by the service principal object id i.e. principal ID & submitting jobs as the local OS user 'user1' results in the above exception.

The fix is to mimic the ownership to the local OS user, by adding the below properties to`core-site.xml`.

```xml
<property>
  <name>fs.azure.identity.transformer.service.principal.id</name>
  <value>service principal object id</value>
  <description>
  An Azure Active Directory object ID (oid) used as the replacement for names contained
  in the list specified by “fs.azure.identity.transformer.service.principal.substitution.list”.
  Notice that instead of setting oid, you can also set $superuser here.
  </description>
</property>
<property>
  <name>fs.azure.identity.transformer.service.principal.substitution.list</name>
  <value>user1</value>
  <description>
  A comma separated list of names to be replaced with the service principal ID specified by
  “fs.azure.identity.transformer.service.principal.id”.  This substitution occurs
  when setOwner, setAcl, modifyAclEntries, or removeAclEntries are invoked with identities
  contained in the substitution list. Notice that when in non-secure cluster, asterisk symbol *
  can be used to match all user/group.
  </description>
</property>
```

Once the above properties are configured, `hdfs dfs -ls abfs://container1@abfswales1.dfs.core.windows.net/` shows the ADLS Gen2 files/directories are now owned by 'user1'.

## <a name="KnownIssues"></a> Known Issues

Following failures are known and expected to fail as of now.
1. AzureBlobFileSystem.setXAttr() and AzureBlobFileSystem.getXAttr() will fail when attempted on root ("/") path with `Operation failed: "The request URI is invalid.", HTTP 400 Bad Request`

## <a name="testing"></a> Testing ABFS

See the relevant section in [Testing Azure](testing_azure.html).
