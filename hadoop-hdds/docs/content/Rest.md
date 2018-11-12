---
title: REST API
menu:
   main:
      parent: Client
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

The Ozone REST API's allows user to access ozone via  REST protocol.

## Authentication and Authorization

For time being, The default authentication mode of REST API is insecure access
mode, which is *Simple* mode. Under this mode, ozone server trusts the user
name specified by client and it does not perform any authentication.

User name can be specified in HTTP header by

* `x-ozone-user: {USER_NAME}`

for example if add following header *x-ozone-user: bilbo* in the HTTP request,
then operation will be executed as *bilbo* user.
In *Simple* mode, there is no real authorization either. Client can be
authorized to obtain administrator privilege by using HTTP header

* `Authorization: {AUTH_METHOD} {SIGNATURE}`

for example set following header *Authorization: OZONE root* in the HTTP request,
then ozone will authorize the client with administrator privilege.

## Common REST Headers

The following HTTP headers must be set for each REST call.

| Property | Description |
|:---- |:----
| Authorization | The authorization field determines which authentication method is used by ozone. Currently only *simple* mode is supported, the corresponding value is *OZONE*. Optionally an user name can be set as *OZONE {USER_NAME}* to authorize as a particular user. |
| Date | Standard HTTP header that represents dates. The format is - day of the week, month, day, year and time (military time format) in GMT. Any other time zone will be rejected by ozone server. Eg. *Date : Mon, Apr 4, 2016 06:22:00 GMT*. This field is required. |
| x-ozone-version | A required HTTP header to indicate which version of API this call will be communicating to. E.g *x-ozone-version: v1*. Currently ozone only publishes v1 version API. |

## Common Reply Headers

The common reply headers are part of all Ozone server replies.

| Property | Description |
|:---- |:----
| Date | This is the HTTP date header and it is set to server’s local time expressed in GMT. |
| x-ozone-request-id | This is a UUID string that represents an unique request ID. This ID is used to track the request through the ozone system and is useful for debugging purposes. |
| x-ozone-server-name | Fully qualified domain name of the sever which handled the request. |

## Volume APIs

### Create a Volume

This API allows admins to create a new storage volume.

Schema:

- `POST /{volume}?quota=<VOLUME_QUOTA>`

Query Parameter:

| Query Parameter | Value | Description |
|:---- |:---- |:----
| quota | long<BYTES \| MB \| GB \| TB> | Optional. Quota size in BYTEs, MBs, GBs or TBs |

Sample HTTP POST request:

    curl -i -X POST -H "x-ozone-user: bilbo" -H "x-ozone-version: v1" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "Authorization:OZONE root" "http://localhost:9880/volume-to-create"

this request creates a volume as user *bilbo*, the authorization field is set to *OZONE root* because this call requires administration privilege. The client receives a response with zero content length.

    HTTP/1.1 201 Created
    x-ozone-server-name: localhost
    x-ozone-request-id: 2173deb5-bbb7-4f0a-8236-f354784e3bae
    Date: Tue, 27 Jun 2017 07:42:04 GMT
    Content-Type: application/octet-stream
    Content-Length: 0
    Connection: keep-alive

### Update Volume

This API allows administrators to update volume info such as ownership and quota. This API requires administration privilege.

Schema:

- `PUT /{volume}?quota=<VOLUME_QUOTA>`

Query Parameter:

| Query Parameter | Value | Description |
|:---- |:---- |:----
| quota | long<BYTES \| MB \| GB \| TB>  \| remove | Optional. Quota size in BYTEs, MBs, GBs or TBs. Or use string value *remove* to remove an existing quota for a volume. |

Sample HTTP PUT request:

    curl -X PUT -H "Authorization:OZONE root" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "x-ozone-version: v1" -H "x-ozone-user: john"  http://localhost:9880/volume-to-update

this request modifies the owner of */volume-to-update* to *john*.

### Delete Volume

This API allows user to delete a volume owned by themselves if the volume is not empty. Administrators can delete volumes owned by any user.

Schema:

- `DELETE /{volume}`

Sample HTTP DELETE request:

    curl -i -X DELETE -H "Authorization:OZONE root" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "x-ozone-version: v1" -H "x-ozone-user: bilbo"  http://localhost:9880/volume-to-delete

this request deletes an empty volume */volume-to-delete*. The client receives a zero length content.

    HTTP/1.1 200 OK
    x-ozone-server-name: localhost
    x-ozone-request-id: 6af14c64-e3a9-40fe-9634-df60b7cbbc6a
    Date: Tue, 27 Jun 2017 08:49:52 GMT
    Content-Type: application/octet-stream
    Content-Length: 0
    Connection: keep-alive

### Info Volume

This API allows user to read the info of a volume owned by themselves. Administrators can read volume info owned by any user.

Schema:

- `GET /{volume}?info=volume`

Query Parameter:

| Query Parameter | Value | Description |
|:---- |:---- |:----
| info | "volume" | Required and enforced with this value. |

Sample HTTP GET request:

    curl -i -H "x-ozone-user: bilbo" -H "x-ozone-version: v1" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "Authorization:OZONE" "http://localhost:9880/volume-of-bilbo?info=volume"

this request gets the info of volume */volume-of-bilbo*, the client receives a response with a JSON object of volume info.

    HTTP/1.1 200 OK
    x-ozone-server-name: localhost
    x-ozone-request-id: a2224806-beaf-42dd-a68e-533cd7508f74
    Date: Tue, 27 Jun 2017 07:55:35 GMT
    Content-Type: application/octet-stream
    Content-Length: 171
    Connection: keep-alive

    {
      "owner" : { "name" : "bilbo" },
      "quota" : { "unit" : "TB", "size" : 1048576 },
      "volumeName" : "volume-of-bilbo",
      "createdOn" : "Tue, 27 Jun 2017 07:42:04 GMT",
      "createdBy" : "root"
    }

### List Volumes

This API allows user to list all volumes owned by themselves. Administrators can list all volumes owned by any user.

Schema:

- `GET /?prefix=<PREFIX>&max-keys=<MAX_RESULT_SIZE>&prev-key=<PREVIOUS_VOLUME_KEY>`

Query Parameter:

| Query Parameter | Value | Description |
|:---- |:---- |:----
| prefix | string | Optional. Only volumes with this prefix are included in the result. |
| max-keys | int | Optional. Maximum number of volumes included in the result. Default is 1024 if not specified. |
| prev-key | string | Optional. Volume name from where listing should start, this key is excluded in the result. It must be a valid volume name. |
| root-scan | bool | Optional. List all volumes in the cluster if this is set to true. Default false. |

Sample HTTP GET request:

    curl -i -H "x-ozone-user: bilbo" -H "x-ozone-version: v1" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "Authorization:OZONE" "http://localhost:9880/?max-keys=100&prefix=Jan"

this request gets all volumes owned by *bilbo* and each volume's name contains prefix *Jan*, the result at most contains *100* entries. The client receives a list of SON objects, each of them describes the info of a volume.

    HTTP/1.1 200 OK
    x-ozone-server-name: localhost
    x-ozone-request-id: 7fa0dce1-a8bd-4387-bc3c-1dac4b710bb1
    Date: Tue, 27 Jun 2017 08:07:04 GMT
    Content-Type: application/octet-stream
    Content-Length: 602
    Connection: keep-alive

    {
      "volumes" : [
        {
          "owner" : { "name" : "bilbo"},
          "quota" : { "unit" : "TB", "size" : 2 },
          "volumeName" : "Jan-vol1",
          "createdOn" : "Tue, 27 Jun 2017 07:42:04 GMT",
          "createdBy" : root
      },
      ...
      ]
    }

## Bucket APIs

### Create Bucket

This API allows an user to create a bucket in a volume.

Schema:

- `POST /{volume}/{bucket}`

Additional HTTP Headers:

| HTTP Header | Value | Description |
|:---- |:---- |:----
| x-ozone-acl | ozone ACLs | Optional. Ozone acls. |
| x-ozone-storage-class | <DEFAULT \| ARCHIVE \| DISK \| RAM_DISK \| SSD > | Optional. Storage type for a volume. |
| x-ozone-bucket-versioning | enabled/disabled | Optional. Do enable bucket versioning or not. |

Sample HTTP POST request:

    curl -i -X POST -H "x-ozone-user: bilbo" -H "x-ozone-version: v1" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "Authorization:OZONE" http://localhost:9880/volume-of-bilbo/bucket-0

this request creates a bucket *bucket-0* under volume *volume-of-bilbo*.

    HTTP/1.1 201 Created
    x-ozone-server-name: localhost
    x-ozone-request-id: 49acfeec-4c85-470a-872b-2eaebd8d751e
    Date: Tue, 27 Jun 2017 08:55:25 GMT
    Content-Type: application/octet-stream
    Content-Length: 0
    Connection: keep-alive

### Update Bucket

Updates bucket meta-data, like ACLs.

Schema:

- `PUT /{volume}/{bucket}`

Additional HTTP Headers:

| HTTP Header | Value | Description |
|:---- |:---- |:----
| x-ozone-acl | ozone ACLs | Optional. Ozone acls. |
| x-ozone-bucket-versioning | enabled/disabled | Optional. Do enable bucket versioning or not. |

Sample HTTP PUT request:

    curl -i -X PUT -H "x-ozone-user: bilbo" -H "x-ozone-version: v1" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "Authorization:OZONE" -H "x-ozone-acl: ADD user:peregrin:rw" http://localhost:9880/volume-of-bilbo/bucket-to-update

this request adds an ACL policy specified by HTTP header *x-ozone-acl* to bucket */volume-of-bilbo/bucket-to-update*, the ACL field *ADD user:peregrin:rw* gives add additional read/write permission to user *peregrin* to this bucket.

    HTTP/1.1 200 OK
    x-ozone-server-name: localhost
    x-ozone-request-id: b061a295-5faf-4b98-94b9-8b3e87c8eb5e
    Date: Tue, 27 Jun 2017 09:02:37 GMT
    Content-Type: application/octet-stream
    Content-Length: 0
    Connection: keep-alive

### Delete Bucket

Deletes a bucket if it is empty. An user can only delete bucket owned by themselves, and administrators can delete buckets owned by any user, as long as it is empty.

Schema:

- `DELETE /{volume}/{bucket}`

Sample HTTP DELETE request:

    curl -i -X DELETE -H "Authorization:OZONE root" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "x-ozone-version: v1" -H "x-ozone-user:bilbo" "http://localhost:9880/volume-of-bilbo/bucket-0"

this request deletes bucket */volume-of-bilbo/bucket-0*. The client receives a zero length content response.

    HTTP/1.1 200 OK
    x-ozone-server-name: localhost
    x-ozone-request-id: f57acd7a-2116-4c2f-aa2f-5a483db81c9c
    Date: Tue, 27 Jun 2017 09:16:52 GMT
    Content-Type: application/octet-stream
    Content-Length: 0
    Connection: keep-alive


### Info Bucket

This API returns information about a given bucket.

Schema:

- `GET /{volume}/{bucket}?info=bucket`

Query Parameters:

| Query Parameter | Value | Description |
|:---- |:---- |:----
| info | "bucket" | Required and enforced with this value. |

Sample HTTP GET request:

    curl -i -H "x-ozone-user: bilbo" -H "x-ozone-version: v1" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "Authorization:OZONE" "http://localhost:9880/volume-of-bilbo/bucket-0?info=bucket"

this request gets the info of bucket */volume-of-bilbo/bucket-0*. The client receives a response of JSON object contains bucket info.

    HTTP/1.1 200 OK
    x-ozone-server-name: localhost
    x-ozone-request-id: f125485b-8cae-4c7f-a2d6-5b1fefd6f193
    Date: Tue, 27 Jun 2017 09:08:31 GMT
    Content-Type: application/json
    Content-Length: 138
    Connection: keep-alive

    {
      "volumeName" : "volume-of-bilbo",
      "bucketName" : "bucket-0",
      "createdOn" : "Tue, 27 Jun 2017 08:55:25 GMT",
      "acls" : [ ],
      "versioning" : "DISABLED",
      "storageType" : "DISK"
    }

### List Buckets

List buckets in a given volume.

Schema:

- `GET /{volume}?prefix=<PREFIX>&max-keys=<MAX_RESULT_SIZE>&prev-key=<PREVIOUS_BUCKET_KEY>`

Query Parameters:

| Query Parameter | Value | Description |
|:---- |:---- |:----
| prefix | string | Optional. Only buckets with this prefix are included in the result. |
| max-keys | int | Optional. Maximum number of buckets included in the result. Default is 1024 if not specified. |
| prev-key | string | Optional. Bucket name from where listing should start, this key is excluded in the result. It must be a valid bucket name. |

Sample HTTP GET request:

    curl -i -H "x-ozone-user: bilbo" -H "x-ozone-version: v1" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "Authorization:OZONE" "http://localhost:9880/volume-of-bilbo?max-keys=10"

this request lists all the buckets under volume *volume-of-bilbo*, and the result at most contains 10 entries. The client receives response of a array of JSON objects, each of them represents for a bucket info.

    HTTP/1.1 200 OK
    x-ozone-server-name: localhost
    x-ozone-request-id: e048c3d5-169c-470f-9903-632d9f9e32d5
    Date: Tue, 27 Jun 2017 09:12:18 GMT
    Content-Type: application/octet-stream
    Content-Length: 207
    Connection: keep-alive

    {
      "buckets" : [ {
        "volumeName" : "volume-of-bilbo",
        "bucketName" : "bucket-0",
        "createdOn" : "Tue, 27 Jun 2017 08:55:25 GMT",
        "acls" : [ ],
        "versioning" : null,
        "storageType" : "DISK",
        "bytesUsed" : 0,
        "keyCount" : 0
        },
        ...
      ]
    }

## Key APIs

### Put Key

This API allows user to create or overwrite keys inside of a bucket.

Schema:

- `PUT /{volume}/{bucket}/{key}`

Additional HTTP headers:

| HTTP Header | Value | Description |
|:---- |:---- |:----
| Content-MD5 | MD5 digest | Standard HTTP header, file hash. |

Sample PUT HTTP request:

    curl -X PUT -T /path/to/localfile -H "Authorization:OZONE" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "x-ozone-version: v1" -H "x-ozone-user:bilbo" "http://localhost:9880/volume-of-bilbo/bucket-0/file-0"

this request uploads a local file */path/to/localfile* specified by option *-T* to ozone as user *bilbo*, mapped to ozone key */volume-of-bilbo/bucket-0/file-0*. The client receives a zero length content response.

### Get Key

This API allows user to get or download a key from an ozone bucket.

Schema:

- `GET /{volume}/{bucket}/{key}`

Sample HTTP GET request:

    curl -i -H "x-ozone-user: bilbo" -H "x-ozone-version: v1" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "Authorization:OZONE" "http://localhost:9880/volume-of-bilbo/bucket-0/file-0"

this request reads the content of key */volume-of-bilbo/bucket-0/file-0*. If the content of the file is plain text, it can be directly dumped onto stdout.

    HTTP/1.1 200 OK
    Content-Type: application/octet-stream
    x-ozone-server-name: localhost
    x-ozone-request-id: 1bcd7de7-d8e3-46bb-afee-bdc933d383b8
    Date: Tue, 27 Jun 2017 09:35:29 GMT
    Content-Length: 6
    Connection: keep-alive

    Hello Ozone!

if the file is not plain text, specify *-O* option in curl command and the file *file-0* will be downloaded into current working directory, file name will be same as the key. A sample request like following:

    curl -O -i -H "x-ozone-user: bilbo" -H "x-ozone-version: v1" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "Authorization:OZONE" "http://localhost:9880/volume-of-bilbo/bucket-0/file-1"

response looks like following:

    % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
    100 6148k  100 6148k    0     0  24.0M      0 --:--:-- --:--:-- --:--:-- 24.1M

### Delete Key

This API allows user to delete a key from a bucket.

Schema:

- `DELETE /{volume}/{bucket}/{key}`

Sample HTTP DELETE request:

    curl -i -X DELETE -H "Authorization:OZONE root" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "x-ozone-version: v1" -H "x-ozone-user:bilbo" "http://localhost:9880/volume-of-bilbo/bucket-0/file-0"

this request deletes key */volume-of-bilbo/bucket-0/file-0*. The client receives a zero length content result:

    HTTP/1.1 200 OK
    x-ozone-server-name: localhost
    x-ozone-request-id: f8c4a373-dd5f-4e3a-b6c4-ddf7e191fe91
    Date: Tue, 27 Jun 2017 14:19:48 GMT
    Content-Type: application/octet-stream
    Content-Length: 0
    Connection: keep-alive

### Info Key

This API returns information about a given key.

Schema:

- `GET /{volume}/{bucket}/{key}?info=key`

Query Parameter:

| Query Parameter | Value | Description |
|:---- |:---- |:----
| info | String, "key" | Required and enforced with this value. |

Sample HTTP DELETE request:

    curl -i -H "x-ozone-user: bilbo" -H "x-ozone-version: v1" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "Authorization:OZONE" "http://localhost:9880/volume-of-bilbo/buket-0/file-0?info=key"

this request returns information of the key */volume-of-bilbo/bucket-0/file-0*. The client receives a JSON object listed attributes of the key.

    HTTP/1.1 200 OK
    x-ozone-server-name: localhost
    x-ozone-request-id: c674343c-a0f2-49e4-bbd6-daa73e7dc131
    Date: Mon, 03 Jul 2017 14:28:45 GMT
    Content-Type: application/octet-stream
    Content-Length: 73
    Connection: keep-alive

    {
      "version" : 0,
      "md5hash" : null,
      "createdOn" : "Mon, 26 Jun 2017 04:23:30 GMT",
      "modifiedOn" : "Mon, 26 Jun 2017 04:23:30 GMT",
      "size" : 0,
      "keyName" : "file-0"
    }

### List Keys

This API allows user to list keys in a bucket.

Schema:

- `GET /{volume}/{bucket}?prefix=<PREFIX>&max-keys=<MAX_RESULT_SIZE>&prev-key=<PREVIOUS_KEY>`

Query Parameters:

| Query Parameter | Value | Description |
|:---- |:---- |:----
| prefix | string | Optional. Only keys with this prefix are included in the result. |
| max-keys | int | Optional. Maximum number of keys included in the result. Default is 1024 if not specified. |
| prev-key | string | Optional. Key name from where listing should start, this key is excluded in the result. It must be a valid key name. |

Sample HTTP GET request:

    curl -i -H "x-ozone-user: bilbo" -H "x-ozone-version: v1" -H "Date: Mon, 26 Jun 2017 04:23:30 GMT" -H "Authorization:OZONE" "http:/localhost:9880/volume-of-bilbo/bucket-0/?max-keys=100&prefix=file"

this request list keys under bucket */volume-of-bilbo/bucket-0*, the listing result is filtered by prefix *file*. The client receives an array of JSON objects, each of them represents the info of a matched key.

    HTTP/1.1 200 OK
    x-ozone-server-name: localhost
    x-ozone-request-id: 7f9fc970-9904-4c56-b671-83a086c6f555
    Date: Tue, 27 Jun 2017 09:48:59 GMT
    Content-Type: application/json
    Content-Length: 209
    Connection: keep-alive

    {
      "name" : null,
      "prefix" : file,
      "maxKeys" : 0,
      "truncated" : false,
      "keyList" : [ {
          "version" : 0,
          "md5hash" : null,
          "createdOn" : "Mon, 26 Jun 2017 04:23:30 GMT",
          "modifiedOn" : "Mon, 26 Jun 2017 04:23:30 GMT",
          "size" : 0,
          "keyName" : "file-0"
          },
          ...
       ]
    }
