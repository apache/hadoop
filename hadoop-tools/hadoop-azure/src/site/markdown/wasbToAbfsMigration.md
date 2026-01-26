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

# WASB to ABFS Driver Configuration Conversion Script

To support customer onboard for migration from WASB to ABFS driver, we've
introduced a script to help with the configuration changes required
for the same.

## Introduction

ABFS driver has now built support for
FNS accounts (over BlobEndpoint that WASB Driver uses) using the ABFS scheme.
Refer to: [ABFS Driver for Namespace Disabled Accounts](./fns_blob.html) for more details.

The legacy WASB driver has been **deprecated** and is no longer recommended for
use. Refer to: [WASB Deprecation](./wasb.html) for more details.
It's highly recommended for current WASB Driver users to migrate to ABFS driver,
the only Microsoft driver for Azure Storage.

Microsoft recommends all Big Data and Analytics users to use
Azure Data Lake Gen2 (ADLS Gen2) using the ABFS driver. It is thus preferred to
upgrade to HNS-enabled accounts and use
ABFS driver with DFS endpoint. Alternatively, if there are dependencies on
features that are incompatible with HNS, ABFS driver can be used with Blob
endpoint over non-HNS accounts.

## Script Overview

This script converts the provided WASB configuration file to be compatible with
the Azure Blob File System (ABFS) driver. It performs the following tasks:

1. Prompts the user to select the type of namespace (HNS or Non-HNS) for the
   storage account.
2. Renames specific configurations.
3. Removes unsupported and obsolete configurations.
4. Ensures correct endpoint are used.

## Usage

The user needs to provide the path to the WASB XML configuration file while
running the script.
The script will then prompt the user to select the type of namespace (HNS or
Non-HNS) for the storage account.
This is important as it determines how the script will process the
configurations. Ensuring the correct option is selected is crucial for a
successful migration.

If the user is unsure, please contact our team at **askabfs@microsoft.com** or
follow the instructions below to check from Azure Portal:

* Go to the Azure Portal and navigate to your storage account.
* In the left-hand menu, select 'Overview' section and look for 'Properties'.
* Under 'Blob service', check if 'Hierarchical namespace' is enabled or
  disabled.

After the script has completed, it will output the modified configurations to a
new file named `abfs-converted-config.xml` in the same directory.
It would have the necessary configuration changes made to make it compatible
with the ABFS driver.

Example to run the script:

```shell
./configsupport.sh <path-to-xml-file>
```

For any queries or support, kindly reach out to us at 'askabfs@microsoft.com'