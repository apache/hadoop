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

# Hadoop Azure Support: WASB Driver

## Introduction
WASB Driver is a legacy Hadoop File System driver that was developed to support
[FNS(FlatNameSpace) Azure Storage accounts](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction)
that do not honor File-Folder syntax.
HDFS Folder operations hence are mimicked at client side by WASB driver and
certain folder operations like Rename and Delete can lead to a lot of IOPs with
client-side enumeration and orchestration of rename/delete operation blob by blob.
It was not ideal for other APIs too as initial checks for path is a file or folder
needs to be done over multiple metadata calls. These led to a degraded performance.

To provide better service to Analytics users, Microsoft released [ADLS Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
which are HNS (Hierarchical Namespace) enabled, i.e. File-Folder aware storage accounts.
ABFS driver was designed to overcome the inherent deficiencies of WASB and users
were informed to migrate to ABFS driver.

### Challenges and limitations of WASB Driver
Users of the legacy WASB driver face a number of challenges and limitations:
1. They cannot leverage the optimizations and benefits of the latest ABFS driver.
2. They need to deal with the compatibility issues should the files and folders were
modified with the legacy WASB driver and the ABFS driver concurrently in a phased
transition situation.
3. There are differences for supported features for FNS and HNS over ABFS Driver.
4. In certain cases, they must perform a significant amount of re-work on their
workloads to migrate to the ABFS driver, which is available only on HNS enabled
accounts in a fully tested and supported scenario.

## Deprecation plans for WASB Driver
We are introducing a new feature that will enable the ABFS driver to support
FNS accounts (over BlobEndpoint that WASB Driver uses) using the ABFS scheme.
This feature will enable us to use the ABFS driver to interact with data stored in GPv2
(General Purpose v2) storage accounts.

With this feature, the users who still use the legacy WASB driver will be able
to migrate to the ABFS driver without much re-work on their workloads. They will
however need to change the URIs from the WASB scheme to the ABFS scheme.

Once ABFS driver has built FNS support capability to migrate WASB users, WASB
driver will be marked for removal in next major release. This will remove any ambiguity
for new users onboards as there will be only one Microsoft driver for Azure Storage
and migrating users will get SLA bound support for driver and service,
which was not guaranteed over WASB.

We anticipate that this feature will serve as a stepping stone for users to
move to HNS enabled accounts with the ABFS driver, which is our recommended stack
for big data analytics on ADLS Gen2.

### Impact for existing ABFS users using ADLS Gen2 (HNS enabled account)
This feature does not impact the existing users who are using ADLS Gen2 Accounts
(HNS enabled account) with ABFS driver.

They do not need to make any changes to their workloads or configurations. They
will still enjoy the benefits of HNS, such as atomic operations, fine-grained
access control, scalability, and performance.

### Official recommendation
Microsoft continues to recommend all Big Data and Analytics users to use
Azure Data Lake Gen2 (ADLS Gen2) using the ABFS driver and will continue to optimize
this scenario in the future, we believe that this new option will help all those
users to transition to a supported scenario immediately, while they plan to
ultimately move to ADLS Gen2 (HNS enabled account).

### New Authentication Options for a migrating user
Below auth types that WASB provides will continue to work on the new FNS over
ABFS Driver over configuration that accepts these SAS types (similar to WASB):
1. SharedKey
2. Account SAS
3. Service/Container SAS

Below authentication types that were not supported by WASB driver but supported by
ABFS driver will continue to be available for new FNS over ABFS Driver
1. OAuth 2.0 Client Credentials
2. OAuth 2.0: Refresh Token
3. Azure Managed Identity
4. Custom OAuth 2.0 Token Provider

Refer to [ABFS Authentication](abfs.html/authentication) for more details.

### ABFS Features Not Available for migrating Users
Certain features of ABFS Driver will be available only to users using HNS accounts with ABFS driver.
1. ABFS Driver's SAS Token Provider plugin for UserDelegation SAS and Fixed SAS.
2. Client Provided Encryption Key (CPK) support for Data ingress and egress.
