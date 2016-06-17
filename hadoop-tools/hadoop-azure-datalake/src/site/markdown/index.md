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

* [Introduction](#Introduction)
* [Features](#Features)
* [Limitations](#Limitations)
* [Usage](#Usage)
    * [Concepts](#Concepts)
        * [Webhdfs Compliance](#Webhdfs_Specification_Compliance)
        * [OAuth2 Support](#OAuth2_Support)
        * [Read Ahead Buffer Management](Read_Ahead_Buffer_Management)
    * [Configuring Credentials & FileSystem](#Configuring_Credentials)
        * [Using Refresh Token](#Refresh_Token)
        * [Using Client Keys](#Client_Credential_Token)
    * [Enabling ADL Filesystem](#Enabling_ADL)
    * [Accessing adl URLs](#Accessing_adl_URLs)
* [Testing the hadoop-azure Module](#Testing_the_hadoop-azure_Module)

## <a name="Introduction" />Introduction

The hadoop-azure-datalake module provides support for integration with
[Azure Data Lake Store]( https://azure.microsoft.com/en-in/documentation/services/data-lake-store/).
The jar file is named azure-datalake-store.jar.

## <a name="Features" />Features

* Read and write data stored in an Azure Data Lake Storage account.
* Partial support for [Webhdfs Specification 2.7.0](https://hadoop.apache.org/docs/r2.7.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)
* Reference file system paths using URLs using the `adl` scheme for Secure Webhdfs i.e. SSL
  encrypted access.
* Can act as a source of data in a MapReduce job, or a sink.
* Tested on both Linux and Windows.
* Tested for scale.

## <a name="Limitations" />Limitations
Partial or no support for the following operations in [Webhdfs Specification 2.7.0](https://hadoop.apache.org/docs/r2.7.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html):

* Operation on Symbolic Link
* Proxy Users
* File Truncate
* File Checksum
* File replication factor
* Home Directory Partial supported based on OAuth2 token information and not the active user on Hadoop cluster.
* Extended Attributes(XAttrs) Operations
* Snapshot Operations
* Delegation Token Operations
* User and group information returned as ListStatus and GetFileStatus is in form of GUID associated in Azure Active Directory.

## <a name="Usage" />Usage

### <a name="Concepts" />Concepts
Azure Data Lake Storage access path syntax is

    adl://<Account Name>.azuredatalakestore.net/

Get started with azure data lake account with [https://azure.microsoft.com/en-in/documentation/articles/data-lake-store-get-started-portal/](https://azure.microsoft.com/en-in/documentation/articles/data-lake-store-get-started-portal/)

#### <a name="Webhdfs_Specification_Compliance" />Webhdfs Compliance
Azure Data Lake Storage exposes a public REST endpoint as per [Webhdfs Specification 2.7.0](https://hadoop.apache.org/docs/r2.7.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) to access storage file system.

Syntax to access Azure data lake storage account over [Webhdfs Specification 2.7.0](https://hadoop.apache.org/docs/r2.7.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) is

    https://<Account Name>.azuredatalakestore.net/webhdfs/v1/<File System Path>?<Query paramaters>


#### <a name="#OAuth2_Support" />OAuth2 Support
Usage of Azure Data Lake Storage requires OAuth2 bearer token to be present as part of the HTTPS header as per OAuth2 specification. Valid OAuth2 bearer token should be obtained from Azure Active Directory for valid users who have  access to Azure Data Lake Storage Account.

Azure Active Directory (Azure AD) is Microsoft’s multi-tenant cloud based directory and identity management service. See [https://azure.microsoft.com/en-in/documentation/articles/active-directory-whatis/](https://azure.microsoft.com/en-in/documentation/articles/active-directory-whatis/)

Following sections describes on OAuth2 configuration in core-site.xml.

#### <a name="#Read_Ahead_Buffer_Management" />Read Ahead Buffer Management
Azure Data Lake Storage offers high throughput. To maximize throughput, applications can  use this feature to buffer data concurrently, in memory during read operation. This data is cached in memory per process per stream.


To Enable/Disable read ahead feature.

    <property>
        <name>adl.feature.override.readahead</name>
        <value>true</value>
        <description>
            Enables read aheads in the ADL client, the feature is used to improve read throughput.
            This works in conjunction with the value set in adl.feature.override.readahead.max.buffersize.
            When set to false the read ahead feature is turned off.
            Default : True if not configured.
        </description>
    </property>

To configure read ahead buffer size.

    <property>
        <name>adl.feature.override.readahead.max.buffersize</name>
        <value>8388608</value>
        <description>
            Define maximum buffer size to cache read ahead data, this is allocated per process to
            cache read ahead data. Applicable only when adl.feature.override.readahead is set to true.
            Default : 8388608 Byte i.e. 8MB if not configured.
        </description>
    </property>

To configure number of concurrent connection to Azure Data Lake Storage Account.

    <property>
        <name>adl.feature.override.readahead.max.concurrent.connection</name>
        <value>2</value>
        <description>
            Define maximum concurrent connection can be established to
            read ahead. If the data size is<4MB then only 1 read n/w connection
            is set. If the data size is >4MB but<8MB then 2 read n/w
            connection
            is set. Data >8MB then value set under the property would
             take
            effect. Applicable only when adl.feature.override.readahead is set
            to true and buffer size is >8MB.
            It is recommended to reset this property if the adl.feature.override.readahead.max.buffersize
            is < 8MB to gain performance. Application has to consider
             throttling
            limit for the account as well before configuring large buffer size.
        </description>
    </property>

## <a name="Configuring_Credentials" />Configuring Credentials & FileSystem
Credentials can be configured using either a refresh token (associated with a user) or a client credential (analogous to a service principal).

### <a name="Refresh_Token" />Using Refresh Token

Update core-site.xml for OAuth2 configuration

         <property>
            <name>dfs.webhdfs.oauth2.refresh.token.expires.ms.since.epoch</name>
            <value>0</value>
         </property>

         <property>
            <name>dfs.webhdfs.oauth2.credential</name>
            <value>bearer.and.refresh.token</value>
         </property>

        <property>
            <name>dfs.webhdfs.oauth2.access.token</name>
            <value>NOT_SET</value>
        </property>

        <property>
            <name>dfs.webhdfs.oauth2.refresh.url</name>
            <value>https://login.windows.net/common/oauth2/token/</value>
        </property>

        <property>
            <name>dfs.webhdfs.oauth2.access.token.provider</name>
            <value>org.apache.hadoop.fs.adl.oauth2.CachedRefreshTokenBasedAccessTokenProvider</value>
        </property>

Application require to set Client id and OAuth2 refresh token from Azure Active Directory associated with client id. See [https://github.com/AzureAD/azure-activedirectory-library-for-java](https://github.com/AzureAD/azure-activedirectory-library-for-java).

**Do not share client id and refresh token, it must be kept secret.**

        <property>
            <name>dfs.webhdfs.oauth2.client.id</name>
            <value></value>
        </property>

        <property>
            <name>dfs.webhdfs.oauth2.refresh.token</name>
            <value></value>
        </property>


### <a name="Client_Credential_Token" />Using Client Keys

#### Generating the Service Principal
1.  Go to the portal (https://portal.azure.com)
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

#### Adding the service principal to your ADL Account
1.  Go to the portal again, and open your ADL account
2.  Select Users under Settings
3.  Add your user name you created in Step 6 above (note that it does not show up in the list, but will be found if you searched for the name)
4.  Add "Owner" role

#### Configure core-site.xml
Add the following properties to your core-site.xml

    <property>
      <name>dfs.webhdfs.oauth2.access.token.provider</name>
      <value>org.apache.hadoop.hdfs.web.oauth2.AzureADClientCredentialBasedAccesTokenProvider</value>
    </property>

    <property>
      <name>dfs.webhdfs.oauth2.refresh.url</name>
      <value>TOKEN ENDPOINT FROM STEP 7 ABOVE</value>
    </property>

    <property>
      <name>dfs.webhdfs.oauth2.client.id</name>
      <value>CLIENT ID FROM STEP 7 ABOVE</value>
    </property>

    <property>
      <name>dfs.webhdfs.oauth2.credential</name>
      <value>PASSWORD FROM STEP 7 ABOVE</value>
    </property>

    <property>
      <name>fs.adls.oauth2.resource</name>
      <value>https://management.core.windows.net/</value>
    </property>

    <property>
      <name>fs.defaultFS</name>
      <value>YOUR ADL STORE URL (e.g., https://example.azuredatalakestore.net) </value>
    </property>


## <a name="Enabling_ADL" />Enabling ADL Filesystem

For ADL FileSystem to take effect. Update core-site.xml with

        <property>
            <name>fs.adl.impl</name>
            <value>org.apache.hadoop.fs.adl.AdlFileSystem</value>
        </property>

        <property>
            <name>fs.AbstractFileSystem.adl.impl</name>
            <value>org.apache.hadoop.fs.adl.Adl</value>
        </property>


### <a name="Accessing_adl_URLs" />Accessing adl URLs

After credentials are configured in core-site.xml, any Hadoop component may
reference files in that Azure Data Lake Storage account by using URLs of the following
format:

    adl://<Account Name>.azuredatalakestore.net/<path>

The schemes `adl` identify a URL on a file system backed by Azure
Data Lake Storage.  `adl` utilizes encrypted HTTPS access for all interaction with
the Azure Data Lake Storage API.

For example, the following
[FileSystem Shell](../hadoop-project-dist/hadoop-common/FileSystemShell.html)
commands demonstrate access to a storage account named `youraccount`.

    > hadoop fs -mkdir adl://yourcontainer.azuredatalakestore.net/testDir

    > hadoop fs -put testFile adl://yourcontainer.azuredatalakestore.net/testDir/testFile

    > hadoop fs -cat adl://yourcontainer.azuredatalakestore.net/testDir/testFile
    test file content
## <a name="Testing_the_hadoop-azure_Module" />Testing the azure-datalake-store Module
The hadoop-azure module includes a full suite of unit tests. Most of the tests will run without additional configuration by running mvn test. This includes tests against mocked storage, which is an in-memory emulation of Azure Data Lake Storage.

A selection of tests can run against the Azure Data Lake Storage. To run tests against Adl storage. Please configure contract-test-options.xml with Adl account information mentioned in the above sections. Also turn on contract test execution flag to trigger tests against Azure Data Lake Storage.

    <property>
      <name>dfs.adl.test.contract.enable</name>
      <value>true</value>
    </property>
