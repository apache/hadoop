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

# Azure Blob Storage REST API (Blob Endpoint)

## Introduction
The REST API for Blob Storage defines HTTP operations against the storage account, containers(filesystems), and blobs.(files)
The API includes the operations listed in the following table.

| Operation                                             | Resource Type | Description                                                                                 |
|-------------------------------------------------------|---------------|---------------------------------------------------------------------------------------------|
| [Create Container](#create-container)                 | Filesystem    | Creates a new azure storage container to be used as an hadoop filesystem.                   |
| [Delete Container](#delete-container)                 | Filesystem    | Deletes the specified container acting as hadoop filesystem.                                |
| [Set Container Metadata](#set-container-metadata)     | Filesystem    | Sets the metadata of the specified container acting as hadoop filesystem.                   |
| [Get Container Properties](#get-container-properties) | Filesystem    | Gets the metadata of the specified container acting as hadoop filesystem.                   |
| [List Blobs](#list-blobs)                             | Filesystem    | Lists the paths under the specified directory inside container acting as hadoop filesystem. |
| [Put Blob](#put-blob)                                 | Path          | Creates a new path or updates an existing path under the specified filesystem (container).  |
| [Lease Blob](#lease-blob)                             | Path          | Establishes and manages a lease on the specified path.                                      |
| [Put Block](#put-block)                               | Path          | Appends Data to an already created blob at specified path.                                  |
| [Put Block List](#put-block-list)                     | Path          | Flushes The Appended Data to the blob at specified path.                                    |
| [Set Blob Metadata](#set-blob-metadata)               | Path          | Sets the user-defined attributes of the blob at specified path.                             |
| [Get Blob Properties](#get-blob-properties)           | Path          | Gets the user-defined attributes of the blob at specified path.                             |
| [Get Blob](#get-blob)                                 | Path          | Reads data from the blob at specified path.                                                 |
| [Delete Blob](#delete-blob)                           | Path          | Deletes the blob at specified path.                                                         |
| [Get Block List](#get-block-list)                     | Path          | Retrieves the list of blocks that have been uploaded as part of a block blob.               |
| [Copy Blob](#copy-blob)                               | Path          | Copies a blob to a destination within the storage account.                                  |

## Create Container
The Create Container operation creates a new container under the specified account. If the container with the same name
already exists, the operation fails.
Rest API Documentation: [Create Container](https://docs.microsoft.com/en-us/rest/api/storageservices/create-container)

## Delete Container
The Delete Container operation marks the specified container for deletion. The container and any blobs contained within it.
Rest API Documentation: [Delete Container](https://docs.microsoft.com/en-us/rest/api/storageservices/delete-container)

## Set Container Metadata
The Set Container Metadata operation sets user-defined metadata for the specified container as one or more name-value pairs.
Rest API Documentation: [Set Container Metadata](https://docs.microsoft.com/en-us/rest/api/storageservices/set-container-metadata)

## Get Container Properties
The Get Container Properties operation returns all user-defined metadata and system properties for the specified container. The returned data doesn't include the container's list of blobs.
Rest API Documentation: [Get Container Properties](https://docs.microsoft.com/en-us/rest/api/storageservices/get-container-properties)

## List Blobs
The List Blobs operation returns a list of the blobs under the specified container.
Rest API Documentation: [List Blobs](https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs)

## Put Blob
The Put Blob operation creates a new block blob, or updates the content of an existing block blob.
The Put Blob operation will overwrite all contents of an existing blob with the same name.
When you update an existing block blob, you overwrite any existing metadata on the blob.
The content of the existing blob is overwritten with the content of the new blob.
Partial updates are not supported with Put Blob
Rest API Documentation: [Put Blob](https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob)

## Lease Blob
The Lease Blob operation creates and manages a lock on a blob for write and delete operations. The lock duration can be 15 to 60 seconds, or can be infinite.
Rest API Documentation: [Lease Blob](https://docs.microsoft.com/en-us/rest/api/storageservices/lease-blob)

## Put Block
The Put Block operation creates a new block to be committed as part of a blob.
Rest API Documentation: [Put Block](https://docs.microsoft.com/en-us/rest/api/storageservices/put-block)

## Put Block List
The Put Block List operation writes a blob by specifying the list of block IDs that make up the blob. To be written as part of a blob, a block must have been successfully written to the server in an earlier Put Block operation. You can call Put Block List to update a blob by uploading only those blocks that have changed and then committing the new and existing blocks together.
Rest API Documentation: [Put Block List](https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list)

## Set Blob Metadata
The Set Blob Metadata operation sets user-defined metadata for the specified blob as one or more name-value pairs.
Rest API Documentation: [Set Blob Metadata](https://docs.microsoft.com/en-us/rest/api/storageservices/set-blob-metadata)

## Get Blob Properties
The Get Blob Properties operation returns all user-defined metadata, standard HTTP properties, and system properties for the blob.
Rest API Documentation: [Get Blob Properties](https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties)

## Get Blob
The Get Blob operation reads or downloads a blob from the system, including its metadata and properties.
Rest API Documentation: [Get Blob](https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob)

## Delete Blob
The Delete Blob operation marks the specified blob for deletion. The blob is later deleted during garbage collection.
Rest API Documentation: [Delete Blob](https://docs.microsoft.com/en-us/rest/api/storageservices/delete-blob)

## Get Block List
The Get Block List operation retrieves the list of blocks that have been uploaded as part of a block blob.
Rest API Documentation: [Get Block List](https://docs.microsoft.com/en-us/rest/api/storageservices/get-block-list)

## Copy Blob
The Copy Blob operation copies a blob to a destination within the storage account.
Rest API Documentation: [Copy Blob](https://docs.microsoft.com/en-us/rest/api/storageservices/copy-blob)