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

WebHDFS REST API
================

* [WebHDFS REST API](#WebHDFS_REST_API)
    * [Document Conventions](#Document_Conventions)
    * [Introduction](#Introduction)
        * [Operations](#Operations)
        * [FileSystem URIs vs HTTP URLs](#FileSystem_URIs_vs_HTTP_URLs)
        * [HDFS Configuration Options](#HDFS_Configuration_Options)
    * [Authentication](#Authentication)
    * [Proxy Users](#Proxy_Users)
    * [File and Directory Operations](#File_and_Directory_Operations)
        * [Create and Write to a File](#Create_and_Write_to_a_File)
        * [Append to a File](#Append_to_a_File)
        * [Concat File(s)](#Concat_Files)
        * [Open and Read a File](#Open_and_Read_a_File)
        * [Make a Directory](#Make_a_Directory)
        * [Create a Symbolic Link](#Create_a_Symbolic_Link)
        * [Rename a File/Directory](#Rename_a_FileDirectory)
        * [Delete a File/Directory](#Delete_a_FileDirectory)
        * [Truncate a File](#Truncate_a_File)
        * [Status of a File/Directory](#Status_of_a_FileDirectory)
        * [List a Directory](#List_a_Directory)
    * [Other File System Operations](#Other_File_System_Operations)
        * [Get Content Summary of a Directory](#Get_Content_Summary_of_a_Directory)
        * [Get File Checksum](#Get_File_Checksum)
        * [Get Home Directory](#Get_Home_Directory)
        * [Set Permission](#Set_Permission)
        * [Set Owner](#Set_Owner)
        * [Set Replication Factor](#Set_Replication_Factor)
        * [Set Access or Modification Time](#Set_Access_or_Modification_Time)
        * [Modify ACL Entries](#Modify_ACL_Entries)
        * [Remove ACL Entries](#Remove_ACL_Entries)
        * [Remove Default ACL](#Remove_Default_ACL)
        * [Remove ACL](#Remove_ACL)
        * [Set ACL](#Set_ACL)
        * [Get ACL Status](#Get_ACL_Status)
        * [Check access](#Check_access)
    * [Extended Attributes(XAttrs) Operations](#Extended_AttributesXAttrs_Operations)
        * [Set XAttr](#Set_XAttr)
        * [Remove XAttr](#Remove_XAttr)
        * [Get an XAttr](#Get_an_XAttr)
        * [Get multiple XAttrs](#Get_multiple_XAttrs)
        * [Get all XAttrs](#Get_all_XAttrs)
        * [List all XAttrs](#List_all_XAttrs)
    * [Snapshot Operations](#Snapshot_Operations)
        * [Create Snapshot](#Create_Snapshot)
        * [Delete Snapshot](#Delete_Snapshot)
        * [Rename Snapshot](#Rename_Snapshot)
    * [Delegation Token Operations](#Delegation_Token_Operations)
        * [Get Delegation Token](#Get_Delegation_Token)
        * [Get Delegation Tokens](#Get_Delegation_Tokens)
        * [Renew Delegation Token](#Renew_Delegation_Token)
        * [Cancel Delegation Token](#Cancel_Delegation_Token)
    * [Error Responses](#Error_Responses)
        * [HTTP Response Codes](#HTTP_Response_Codes)
            * [Illegal Argument Exception](#Illegal_Argument_Exception)
            * [Security Exception](#Security_Exception)
            * [Access Control Exception](#Access_Control_Exception)
            * [File Not Found Exception](#File_Not_Found_Exception)
    * [JSON Schemas](#JSON_Schemas)
        * [ACL Status JSON Schema](#ACL_Status_JSON_Schema)
        * [XAttrs JSON Schema](#XAttrs_JSON_Schema)
        * [XAttrNames JSON Schema](#XAttrNames_JSON_Schema)
        * [Boolean JSON Schema](#Boolean_JSON_Schema)
        * [ContentSummary JSON Schema](#ContentSummary_JSON_Schema)
        * [FileChecksum JSON Schema](#FileChecksum_JSON_Schema)
        * [FileStatus JSON Schema](#FileStatus_JSON_Schema)
            * [FileStatus Properties](#FileStatus_Properties)
        * [FileStatuses JSON Schema](#FileStatuses_JSON_Schema)
        * [Long JSON Schema](#Long_JSON_Schema)
        * [Path JSON Schema](#Path_JSON_Schema)
        * [RemoteException JSON Schema](#RemoteException_JSON_Schema)
        * [Token JSON Schema](#Token_JSON_Schema)
            * [Token Properties](#Token_Properties)
        * [Tokens JSON Schema](#Tokens_JSON_Schema)
    * [HTTP Query Parameter Dictionary](#HTTP_Query_Parameter_Dictionary)
        * [ACL Spec](#ACL_Spec)
        * [XAttr Name](#XAttr_Name)
        * [XAttr Value](#XAttr_Value)
        * [XAttr set flag](#XAttr_set_flag)
        * [XAttr value encoding](#XAttr_value_encoding)
        * [Access Time](#Access_Time)
        * [Block Size](#Block_Size)
        * [Buffer Size](#Buffer_Size)
        * [Create Parent](#Create_Parent)
        * [Delegation](#Delegation)
        * [Destination](#Destination)
        * [Do As](#Do_As)
        * [Fs Action](#Fs_Action)
        * [Group](#Group)
        * [Length](#Length)
        * [Modification Time](#Modification_Time)
        * [Offset](#Offset)
        * [Old Snapshot Name](#Old_Snapshot_Name)
        * [Op](#Op)
        * [Overwrite](#Overwrite)
        * [Owner](#Owner)
        * [Permission](#Permission)
        * [Recursive](#Recursive)
        * [Renewer](#Renewer)
        * [Replication](#Replication)
        * [Snapshot Name](#Snapshot_Name)
        * [Sources](#Sources)
        * [Token](#Token)
        * [Token Kind](#Token_Kind)
        * [Token Service](#Token_Service)
        * [Username](#Username)

Document Conventions
--------------------

| `Monospaced` | Used for commands, HTTP request and responses and code blocks. |
|:---- |:---- |
| `<Monospaced>` | User entered values. |
| `[Monospaced]` | Optional values. When the value is not specified, the default value is used. |
| *Italics* | Important phrases and words. |

Introduction
------------

The HTTP REST API supports the complete [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html)/[FileContext](../../api/org/apache/hadoop/fs/FileContext.html) interface for HDFS. The operations and the corresponding FileSystem/FileContext methods are shown in the next section. The Section [HTTP Query Parameter Dictionary](#HTTP_Query_Parameter_Dictionary) specifies the parameter details such as the defaults and the valid values.

### Operations

*   HTTP GET
    * [`OPEN`](#Open_and_Read_a_File) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).open)
    * [`GETFILESTATUS`](#Status_of_a_FileDirectory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getFileStatus)
    * [`LISTSTATUS`](#List_a_Directory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).listStatus)
    * [`GETCONTENTSUMMARY`](#Get_Content_Summary_of_a_Directory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getContentSummary)
    * [`GETFILECHECKSUM`](#Get_File_Checksum) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getFileChecksum)
    * [`GETHOMEDIRECTORY`](#Get_Home_Directory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getHomeDirectory)
    * [`GETDELEGATIONTOKEN`](#Get_Delegation_Token) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getDelegationToken)
    * [`GETDELEGATIONTOKENS`](#Get_Delegation_Tokens) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getDelegationTokens)
    * [`GETXATTRS`](#Get_an_XAttr) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getXAttr)
    * [`GETXATTRS`](#Get_multiple_XAttrs) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getXAttrs)
    * [`GETXATTRS`](#Get_all_XAttrs) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getXAttrs)
    * [`LISTXATTRS`](#List_all_XAttrs) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).listXAttrs)
    * [`CHECKACCESS`](#Check_access) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).access)
*   HTTP PUT
    * [`CREATE`](#Create_and_Write_to_a_File) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).create)
    * [`MKDIRS`](#Make_a_Directory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).mkdirs)
    * [`CREATESYMLINK`](#Create_a_Symbolic_Link) (see [FileContext](../../api/org/apache/hadoop/fs/FileContext.html).createSymlink)
    * [`RENAME`](#Rename_a_FileDirectory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).rename)
    * [`SETREPLICATION`](#Set_Replication_Factor) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setReplication)
    * [`SETOWNER`](#Set_Owner) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setOwner)
    * [`SETPERMISSION`](#Set_Permission) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setPermission)
    * [`SETTIMES`](#Set_Access_or_Modification_Time) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setTimes)
    * [`RENEWDELEGATIONTOKEN`](#Renew_Delegation_Token) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).renewDelegationToken)
    * [`CANCELDELEGATIONTOKEN`](#Cancel_Delegation_Token) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).cancelDelegationToken)
    * [`CREATESNAPSHOT`](#Create_Snapshot) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).createSnapshot)
    * [`RENAMESNAPSHOT`](#Rename_Snapshot) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).renameSnapshot)
    * [`SETXATTR`](#Set_XAttr) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setXAttr)
    * [`REMOVEXATTR`](#Remove_XAttr) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).removeXAttr)
*   HTTP POST
    * [`APPEND`](#Append_to_a_File) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).append)
    * [`CONCAT`](#Concat_Files) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).concat)
    * [`TRUNCATE`](#Truncate_a_File) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).concat)
*   HTTP DELETE
    * [`DELETE`](#Delete_a_FileDirectory) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).delete)
    * [`DELETESNAPSHOT`](#Delete_Snapshot) (see [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).deleteSnapshot)

### FileSystem URIs vs HTTP URLs

The FileSystem scheme of WebHDFS is "`webhdfs://`". A WebHDFS FileSystem URI has the following format.

      webhdfs://<HOST>:<HTTP_PORT>/<PATH>

The above WebHDFS URI corresponds to the below HDFS URI.

      hdfs://<HOST>:<RPC_PORT>/<PATH>

In the REST API, the prefix "`/webhdfs/v1`" is inserted in the path and a query is appended at the end. Therefore, the corresponding HTTP URL has the following format.

      http://<HOST>:<HTTP_PORT>/webhdfs/v1/<PATH>?op=...

### HDFS Configuration Options

Below are the HDFS configuration options for WebHDFS.

| Property Name | Description |
|:---- |:---- |
| `dfs.webhdfs.enabled ` | Enable/disable WebHDFS in Namenodes and Datanodes |
| `dfs.web.authentication.kerberos.principal` | The HTTP Kerberos principal used by Hadoop-Auth in the HTTP endpoint. The HTTP Kerberos principal MUST start with 'HTTP/' per Kerberos HTTP SPNEGO specification. A value of "\*" will use all HTTP principals found in the keytab. |
| `dfs.web.authentication.kerberos.keytab ` | The Kerberos keytab file with the credentials for the HTTP Kerberos principal used by Hadoop-Auth in the HTTP endpoint. |

Authentication
--------------

When security is *off*, the authenticated user is the username specified in the `user.name` query parameter. If the `user.name` parameter is not set, the server may either set the authenticated user to a default web user, if there is any, or return an error response.

When security is *on*, authentication is performed by either Hadoop delegation token or Kerberos SPNEGO. If a token is set in the `delegation` query parameter, the authenticated user is the user encoded in the token. If the `delegation` parameter is not set, the user is authenticated by Kerberos SPNEGO.

Below are examples using the `curl` command tool.

1.  Authentication when security is off:

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?[user.name=<USER>&]op=..."

2.  Authentication using Kerberos SPNEGO when security is on:

        curl -i --negotiate -u : "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=..."

3.  Authentication using Hadoop delegation token when security is on:

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?delegation=<TOKEN>&op=..."

See also: [Authentication for Hadoop HTTP web-consoles](../hadoop-common/HttpAuthentication.html)

Proxy Users
-----------

When the proxy user feature is enabled, a proxy user *P* may submit a request on behalf of another user *U*. The username of *U* must be specified in the `doas` query parameter unless a delegation token is presented in authentication. In such case, the information of both users *P* and *U* must be encoded in the delegation token.

1.  A proxy request when security is off:

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?[user.name=<USER>&]doas=<USER>&op=..."

2.  A proxy request using Kerberos SPNEGO when security is on:

        curl -i --negotiate -u : "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?doas=<USER>&op=..."

3.  A proxy request using Hadoop delegation token when security is on:

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?delegation=<TOKEN>&op=..."

File and Directory Operations
-----------------------------

### Create and Write to a File

* Step 1: Submit a HTTP PUT request without automatically following redirects and without sending the file data.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
                            [&overwrite=<true |false>][&blocksize=<LONG>][&replication=<SHORT>]
                            [&permission=<OCTAL>][&buffersize=<INT>]"

    The request is redirected to a datanode where the file data is to be written:

        HTTP/1.1 307 TEMPORARY_REDIRECT
        Location: http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=CREATE...
        Content-Length: 0

* Step 2: Submit another HTTP PUT request using the URL in the `Location` header with the file data to be written.

        curl -i -X PUT -T <LOCAL_FILE> "http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=CREATE..."

    The client receives a `201 Created` response with zero content length and the WebHDFS URI of the file in the `Location` header:

        HTTP/1.1 201 Created
        Location: webhdfs://<HOST>:<PORT>/<PATH>
        Content-Length: 0

**Note** that the reason of having two-step create/append is for preventing clients to send out data before the redirect. This issue is addressed by the "`Expect: 100-continue`" header in HTTP/1.1; see [RFC 2616, Section 8.2.3](http://www.w3.org/Protocols/rfc2616/rfc2616-sec8.html#sec8.2.3). Unfortunately, there are software library bugs (e.g. Jetty 6 HTTP server and Java 6 HTTP client), which do not correctly implement "`Expect: 100-continue`". The two-step create/append is a temporary workaround for the software library bugs.

See also: [`overwrite`](#Overwrite), [`blocksize`](#Block_Size), [`replication`](#Replication), [`permission`](#Permission), [`buffersize`](#Buffer_Size), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).create

### Append to a File

* Step 1: Submit a HTTP POST request without automatically following redirects and without sending the file data.

        curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"

    The request is redirected to a datanode where the file data is to be appended:

        HTTP/1.1 307 TEMPORARY_REDIRECT
        Location: http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=APPEND...
        Content-Length: 0

* Step 2: Submit another HTTP POST request using the URL in the `Location` header with the file data to be appended.

        curl -i -X POST -T <LOCAL_FILE> "http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=APPEND..."

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See the note in the previous section for the description of why this operation requires two steps.

See also: [`buffersize`](#Buffer_Size), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).append

### Concat File(s)

* Submit a HTTP POST request.

        curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CONCAT&sources=<PATHS>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [`sources`](#Sources), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).concat

### Open and Read a File

* Submit a HTTP GET request with automatically following redirects.

        curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
                            [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"

    The request is redirected to a datanode where the file data can be read:

        HTTP/1.1 307 TEMPORARY_REDIRECT
        Location: http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=OPEN...
        Content-Length: 0

    The client follows the redirect to the datanode and receives the file data:

        HTTP/1.1 200 OK
        Content-Type: application/octet-stream
        Content-Length: 22

        Hello, webhdfs user!

See also: [`offset`](#Offset), [`length`](#Length), [`buffersize`](#Buffer_Size), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).open

### Make a Directory

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MKDIRS[&permission=<OCTAL>]"

    The client receives a response with a [`boolean` JSON object](#Boolean_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"boolean": true}

See also: [`permission`](#Permission), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).mkdirs

### Create a Symbolic Link

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATESYMLINK
                                      &destination=<PATH>[&createParent=<true |false>]"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [`destination`](#Destination), [`createParent`](#Create_Parent), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).createSymlink

### Rename a File/Directory

* Submit a HTTP PUT request.

        curl -i -X PUT "<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>"

    The client receives a response with a [`boolean` JSON object](#Boolean_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"boolean": true}

See also: [`destination`](#Destination), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).rename

### Delete a File/Directory

* Submit a HTTP DELETE request.

        curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
                                      [&recursive=<true |false>]"

    The client receives a response with a [`boolean` JSON object](#Boolean_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"boolean": true}

See also: [`recursive`](#Recursive), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).delete

### Truncate a File

* Submit a HTTP POST request.

        curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=TRUNCATE&newlength=<LONG>"

    The client receives a response with a [`boolean` JSON object](#Boolean_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked
        
        {"boolean": true}

See also: [`newlength`](#New_Length), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).truncate

### Status of a File/Directory

* Submit a HTTP GET request.

        curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS"

    The client receives a response with a [`FileStatus` JSON object](#FileStatus_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
          "FileStatus":
          {
            "accessTime"      : 0,
            "blockSize"       : 0,
            "group"           : "supergroup",
            "length"          : 0,             //in bytes, zero for directories
            "modificationTime": 1320173277227,
            "owner"           : "webuser",
            "pathSuffix"      : "",
            "permission"      : "777",
            "replication"     : 0,
            "type"            : "DIRECTORY"    //enum {FILE, DIRECTORY, SYMLINK}
          }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getFileStatus

### List a Directory

* Submit a HTTP GET request.

        curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"

    The client receives a response with a [`FileStatuses` JSON object](#FileStatuses_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Content-Length: 427

        {
          "FileStatuses":
          {
            "FileStatus":
            [
              {
                "accessTime"      : 1320171722771,
                "blockSize"       : 33554432,
                "group"           : "supergroup",
                "length"          : 24930,
                "modificationTime": 1320171722771,
                "owner"           : "webuser",
                "pathSuffix"      : "a.patch",
                "permission"      : "644",
                "replication"     : 1,
                "type"            : "FILE"
              },
              {
                "accessTime"      : 0,
                "blockSize"       : 0,
                "group"           : "supergroup",
                "length"          : 0,
                "modificationTime": 1320895981256,
                "owner"           : "szetszwo",
                "pathSuffix"      : "bar",
                "permission"      : "711",
                "replication"     : 0,
                "type"            : "DIRECTORY"
              },
              ...
            ]
          }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).listStatus

Other File System Operations
----------------------------

### Get Content Summary of a Directory

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETCONTENTSUMMARY"

    The client receives a response with a [`ContentSummary` JSON object](#ContentSummary_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
          "ContentSummary":
          {
            "directoryCount": 2,
            "fileCount"     : 1,
            "length"        : 24930,
            "quota"         : -1,
            "spaceConsumed" : 24930,
            "spaceQuota"    : -1
          }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getContentSummary

### Get File Checksum

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM"

    The request is redirected to a datanode:

        HTTP/1.1 307 TEMPORARY_REDIRECT
        Location: http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM...
        Content-Length: 0

    The client follows the redirect to the datanode and receives a [`FileChecksum` JSON object](#FileChecksum_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
          "FileChecksum":
          {
            "algorithm": "MD5-of-1MD5-of-512CRC32",
            "bytes"    : "eadb10de24aa315748930df6e185c0d ...",
            "length"   : 28
          }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getFileChecksum

### Get Home Directory

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/?op=GETHOMEDIRECTORY"

    The client receives a response with a [`Path` JSON object](#Path_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"Path": "/user/szetszwo"}

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getHomeDirectory

### Set Permission

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETPERMISSION
                                      [&permission=<OCTAL>]"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [`permission`](#Permission), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setPermission

### Set Owner

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETOWNER
                                      [&owner=<USER>][&group=<GROUP>]"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [`owner`](#Owner), [`group`](#Group), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setOwner

### Set Replication Factor

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETREPLICATION
                                      [&replication=<SHORT>]"

    The client receives a response with a [`boolean` JSON object](#Boolean_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"boolean": true}

See also: [`replication`](#Replication), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setReplication

### Set Access or Modification Time

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETTIMES
                                      [&modificationtime=<TIME>][&accesstime=<TIME>]"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [`modificationtime`](#Modification_Time), [`accesstime`](#Access_Time), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setTimes

### Modify ACL Entries

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MODIFYACLENTRIES
                                      &aclspec=<ACLSPEC>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).modifyAclEntries

### Remove ACL Entries

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=REMOVEACLENTRIES
                                      &aclspec=<ACLSPEC>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).removeAclEntries

### Remove Default ACL

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=REMOVEDEFAULTACL"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).removeDefaultAcl

### Remove ACL

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=REMOVEACL"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).removeAcl

### Set ACL

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETACL
                                      &aclspec=<ACLSPEC>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setAcl

### Get ACL Status

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETACLSTATUS"

    The client receives a response with a [`AclStatus` JSON object](#ACL_Status_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
            "AclStatus": {
                "entries": [
                    "user:carla:rw-", 
                    "group::r-x"
                ], 
                "group": "supergroup", 
                "owner": "hadoop", 
                "permission":"775",
                "stickyBit": false
            }
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getAclStatus

### Check access

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CHECKACCESS
                                      &fsaction=<FSACTION>

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).access

Extended Attributes(XAttrs) Operations
--------------------------------------

### Set XAttr

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETXATTR
                                      &xattr.name=<XATTRNAME>&xattr.value=<XATTRVALUE>
                                      &flag=<FLAG>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).setXAttr

### Remove XAttr

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=REMOVEXATTR
                                      &xattr.name=<XATTRNAME>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).removeXAttr

### Get an XAttr

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETXATTRS
                                      &xattr.name=<XATTRNAME>&encoding=<ENCODING>"

    The client receives a response with a [`XAttrs` JSON object](#XAttrs_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
            "XAttrs": [
                {
                    "name":"XATTRNAME",
                    "value":"XATTRVALUE"
                }
            ]
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getXAttr

### Get multiple XAttrs

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETXATTRS
                                      &xattr.name=<XATTRNAME1>&xattr.name=<XATTRNAME2>
                                      &encoding=<ENCODING>"

    The client receives a response with a [`XAttrs` JSON object](#XAttrs_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
            "XAttrs": [
                {
                    "name":"XATTRNAME1",
                    "value":"XATTRVALUE1"
                },
                {
                    "name":"XATTRNAME2",
                    "value":"XATTRVALUE2"
                }
            ]
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getXAttrs

### Get all XAttrs

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETXATTRS
                                      &encoding=<ENCODING>"

    The client receives a response with a [`XAttrs` JSON object](#XAttrs_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
            "XAttrs": [
                {
                    "name":"XATTRNAME1",
                    "value":"XATTRVALUE1"
                },
                {
                    "name":"XATTRNAME2",
                    "value":"XATTRVALUE2"
                },
                {
                    "name":"XATTRNAME3",
                    "value":"XATTRVALUE3"
                }
            ]
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getXAttrs

### List all XAttrs

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTXATTRS"

    The client receives a response with a [`XAttrNames` JSON object](#XAttrNames_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
            "XAttrNames":"[\"XATTRNAME1\",\"XATTRNAME2\",\"XATTRNAME3\"]"
        }

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).listXAttrs

Snapshot Operations
-------------------

### Create Snapshot

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATESNAPSHOT[&snapshotname=<SNAPSHOTNAME>]"

    The client receives a response with a [`Path` JSON object](#Path_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"Path": "/user/szetszwo/.snapshot/s1"}

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).createSnapshot

### Delete Snapshot

* Submit a HTTP DELETE request.

        curl -i -X DELETE "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=DELETESNAPSHOT&snapshotname=<SNAPSHOTNAME>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).deleteSnapshot

### Rename Snapshot

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAMESNAPSHOT
                           &oldsnapshotname=<SNAPSHOTNAME>&snapshotname=<SNAPSHOTNAME>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).renameSnapshot

Delegation Token Operations
---------------------------

### Get Delegation Token

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/?op=GETDELEGATIONTOKEN&renewer=<USER>&service=<SERVICE>&kind=<KIND>"

    The client receives a response with a [`Token` JSON object](#Token_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
          "Token":
          {
            "urlString": "JQAIaG9y..."
          }
        }

See also: [`renewer`](#Renewer), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getDelegationToken, [`kind`](#Token_Kind), [`service`](#Token_Service)

### Get Delegation Tokens

* Submit a HTTP GET request.

        curl -i "http://<HOST>:<PORT>/webhdfs/v1/?op=GETDELEGATIONTOKENS&renewer=<USER>"

    The client receives a response with a [`Tokens` JSON object](#Tokens_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {
          "Tokens":
          {
            "Token":
            [
              {
                "urlString":"KAAKSm9i ..."
              }
            ]
          }
        }

See also: [`renewer`](#Renewer), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).getDelegationTokens

### Renew Delegation Token

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/?op=RENEWDELEGATIONTOKEN&token=<TOKEN>"

    The client receives a response with a [`long` JSON object](#Long_JSON_Schema):

        HTTP/1.1 200 OK
        Content-Type: application/json
        Transfer-Encoding: chunked

        {"long": 1320962673997}           //the new expiration time

See also: [`token`](#Token), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).renewDelegationToken

### Cancel Delegation Token

* Submit a HTTP PUT request.

        curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/?op=CANCELDELEGATIONTOKEN&token=<TOKEN>"

    The client receives a response with zero content length:

        HTTP/1.1 200 OK
        Content-Length: 0

See also: [`token`](#Token), [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).cancelDelegationToken

Error Responses
---------------

When an operation fails, the server may throw an exception. The JSON schema of error responses is defined in [RemoteException JSON Schema](#RemoteException_JSON_Schema). The table below shows the mapping from exceptions to HTTP response codes.

### HTTP Response Codes

| Exceptions | HTTP Response Codes |
|:---- |:---- |
| `IllegalArgumentException ` | `400 Bad Request ` |
| `UnsupportedOperationException` | `400 Bad Request ` |
| `SecurityException ` | `401 Unauthorized ` |
| `IOException ` | `403 Forbidden ` |
| `FileNotFoundException ` | `404 Not Found ` |
| `RumtimeException ` | `500 Internal Server Error` |

Below are examples of exception responses.

#### Illegal Argument Exception

    HTTP/1.1 400 Bad Request
    Content-Type: application/json
    Transfer-Encoding: chunked

    {
      "RemoteException":
      {
        "exception"    : "IllegalArgumentException",
        "javaClassName": "java.lang.IllegalArgumentException",
        "message"      : "Invalid value for webhdfs parameter \"permission\": ..."
      }
    }

#### Security Exception

    HTTP/1.1 401 Unauthorized
    Content-Type: application/json
    Transfer-Encoding: chunked

    {
      "RemoteException":
      {
        "exception"    : "SecurityException",
        "javaClassName": "java.lang.SecurityException",
        "message"      : "Failed to obtain user group information: ..."
      }
    }

#### Access Control Exception

    HTTP/1.1 403 Forbidden
    Content-Type: application/json
    Transfer-Encoding: chunked

    {
      "RemoteException":
      {
        "exception"    : "AccessControlException",
        "javaClassName": "org.apache.hadoop.security.AccessControlException",
        "message"      : "Permission denied: ..."
      }
    }

#### File Not Found Exception

    HTTP/1.1 404 Not Found
    Content-Type: application/json
    Transfer-Encoding: chunked

    {
      "RemoteException":
      {
        "exception"    : "FileNotFoundException",
        "javaClassName": "java.io.FileNotFoundException",
        "message"      : "File does not exist: /foo/a.patch"
      }
    }

JSON Schemas
------------

All operations, except for [`OPEN`](#Open_and_Read_a_File), either return a zero-length response or a JSON response. For [`OPEN`](#Open_and_Read_a_File), the response is an octet-stream. The JSON schemas are shown below. See [draft-zyp-json-schema-03](http://tools.ietf.org/id/draft-zyp-json-schema-03.html) for the syntax definitions of the JSON schemas.

**Note** that the default value of [`additionalProperties`](http://tools.ietf.org/id/draft-zyp-json-schema-03.html#additionalProperties) is an empty schema which allows any value for additional properties. Therefore, all WebHDFS JSON responses allow any additional property. However, if additional properties are included in the responses, they are considered as optional properties in order to maintain compatibility.

### ACL Status JSON Schema

```json
{
  "name"      : "AclStatus",
  "properties":
  {
    "AclStatus":
    {
      "type"      : "object",
      "properties":
      {
        "entries":
        {
          "type": "array"
          "items":
          {
            "description": "ACL entry.",
            "type": "string"
          }
        },
        "group":
        {
          "description": "The group owner.",
          "type"       : "string",
          "required"   : true
        },
        "owner":
        {
          "description": "The user who is the owner.",
          "type"       : "string",
          "required"   : true
        },
        "stickyBit":
        {
          "description": "True if the sticky bit is on.",
          "type"       : "boolean",
          "required"   : true
        },
      }
    }
  }
}
```

### XAttrs JSON Schema

```json
{
  "name"      : "XAttrs",
  "properties":
  {
    "XAttrs":
    {
      "type"      : "array",
      "items":
      {
        "type"    " "object",
        "properties":
        {
          "name":
          {
            "description": "XAttr name.",
            "type"       : "string",
            "required"   : true
          },
          "value":
          {
            "description": "XAttr value.",
            "type"       : "string"
          }
        }
      }
    }
  }
}
```

### XAttrNames JSON Schema

```json
{
  "name"      : "XAttrNames",
  "properties":
  {
    "XAttrNames":
    {
      "description": "XAttr names.",
      "type"       : "string"
      "required"   : true
    }
  }
}
```

### Boolean JSON Schema

```json
{
  "name"      : "boolean",
  "properties":
  {
    "boolean":
    {
      "description": "A boolean value",
      "type"       : "boolean",
      "required"   : true
    }
  }
}
```

See also: [`MKDIRS`](#Make_a_Directory), [`RENAME`](#Rename_a_FileDirectory), [`DELETE`](#Delete_a_FileDirectory), [`SETREPLICATION`](#Set_Replication_Factor)

### ContentSummary JSON Schema

```json
{
  "name"      : "ContentSummary",
  "properties":
  {
    "ContentSummary":
    {
      "type"      : "object",
      "properties":
      {
        "directoryCount":
        {
          "description": "The number of directories.",
          "type"       : "integer",
          "required"   : true
        },
        "fileCount":
        {
          "description": "The number of files.",
          "type"       : "integer",
          "required"   : true
        },
        "length":
        {
          "description": "The number of bytes used by the content.",
          "type"       : "integer",
          "required"   : true
        },
        "quota":
        {
          "description": "The namespace quota of this directory.",
          "type"       : "integer",
          "required"   : true
        },
        "spaceConsumed":
        {
          "description": "The disk space consumed by the content.",
          "type"       : "integer",
          "required"   : true
        },
        "spaceQuota":
        {
          "description": "The disk space quota.",
          "type"       : "integer",
          "required"   : true
        }
      }
    }
  }
}
```

See also: [`GETCONTENTSUMMARY`](#Get_Content_Summary_of_a_Directory)

### FileChecksum JSON Schema


```json
{
  "name"      : "FileChecksum",
  "properties":
  {
    "FileChecksum":
    {
      "type"      : "object",
      "properties":
      {
        "algorithm":
        {
          "description": "The name of the checksum algorithm.",
          "type"       : "string",
          "required"   : true
        },
        "bytes":
        {
          "description": "The byte sequence of the checksum in hexadecimal.",
          "type"       : "string",
          "required"   : true
        },
        "length":
        {
          "description": "The length of the bytes (not the length of the string).",
          "type"       : "integer",
          "required"   : true
        }
      }
    }
  }
}
```

### FileStatus JSON Schema

```json
{
  "name"      : "FileStatus",
  "properties":
  {
    "FileStatus": fileStatusProperties      //See FileStatus Properties
  }
}
```

See also: [`FileStatus` Properties](#FileStatus_Properties), [`GETFILESTATUS`](#Status_of_a_FileDirectory), [FileStatus](../../api/org/apache/hadoop/fs/FileStatus.html)

#### FileStatus Properties

JavaScript syntax is used to define `fileStatusProperties` so that it can be referred in both `FileStatus` and `FileStatuses` JSON schemas.

```javascript
var fileStatusProperties =
{
  "type"      : "object",
  "properties":
  {
    "accessTime":
    {
      "description": "The access time.",
      "type"       : "integer",
      "required"   : true
    },
    "blockSize":
    {
      "description": "The block size of a file.",
      "type"       : "integer",
      "required"   : true
    },
    "group":
    {
      "description": "The group owner.",
      "type"       : "string",
      "required"   : true
    },
    "length":
    {
      "description": "The number of bytes in a file.",
      "type"       : "integer",
      "required"   : true
    },
    "modificationTime":
    {
      "description": "The modification time.",
      "type"       : "integer",
      "required"   : true
    },
    "owner":
    {
      "description": "The user who is the owner.",
      "type"       : "string",
      "required"   : true
    },
    "pathSuffix":
    {
      "description": "The path suffix.",
      "type"       : "string",
      "required"   : true
    },
    "permission":
    {
      "description": "The permission represented as a octal string.",
      "type"       : "string",
      "required"   : true
    },
    "replication":
    {
      "description": "The number of replication of a file.",
      "type"       : "integer",
      "required"   : true
    },
   "symlink":                                         //an optional property
    {
      "description": "The link target of a symlink.",
      "type"       : "string"
    },
   "type":
    {
      "description": "The type of the path object.",
      "enum"       : ["FILE", "DIRECTORY", "SYMLINK"],
      "required"   : true
    }
  }
};
```

### FileStatuses JSON Schema

A `FileStatuses` JSON object represents an array of `FileStatus` JSON objects.

```json
{
  "name"      : "FileStatuses",
  "properties":
  {
    "FileStatuses":
    {
      "type"      : "object",
      "properties":
      {
        "FileStatus":
        {
          "description": "An array of FileStatus",
          "type"       : "array",
          "items"      : fileStatusProperties      //See FileStatus Properties
        }
      }
    }
  }
}
```

See also: [`FileStatus` Properties](#FileStatus_Properties), [`LISTSTATUS`](#List_a_Directory), [FileStatus](../../api/org/apache/hadoop/fs/FileStatus.html)

### Long JSON Schema

```json
{
  "name"      : "long",
  "properties":
  {
    "long":
    {
      "description": "A long integer value",
      "type"       : "integer",
      "required"   : true
    }
  }
}
```

See also: [`RENEWDELEGATIONTOKEN`](#Renew_Delegation_Token),

### Path JSON Schema

```json
{
  "name"      : "Path",
  "properties":
  {
    "Path":
    {
      "description": "The string representation a Path.",
      "type"       : "string",
      "required"   : true
    }
  }
}
```

See also: [`GETHOMEDIRECTORY`](#Get_Home_Directory), [Path](../../api/org/apache/hadoop/fs/Path.html)

### RemoteException JSON Schema

```json
{
  "name"      : "RemoteException",
  "properties":
  {
    "RemoteException":
    {
      "type"      : "object",
      "properties":
      {
        "exception":
        {
          "description": "Name of the exception",
          "type"       : "string",
          "required"   : true
        },
        "message":
        {
          "description": "Exception message",
          "type"       : "string",
          "required"   : true
        },
        "javaClassName":                                     //an optional property
        {
          "description": "Java class name of the exception",
          "type"       : "string",
        }
      }
    }
  }
}
```
See also: [Error Responses](#Error_Responses)

### Token JSON Schema

```json
{
  "name"      : "Token",
  "properties":
  {
    "Token": tokenProperties      //See Token Properties
  }
}
```

See also: [`Token` Properties](#Token_Properties), [`GETDELEGATIONTOKEN`](#Get_Delegation_Token), the note in [Delegation](#Delegation).

#### Token Properties

JavaScript syntax is used to define `tokenProperties` so that it can be referred in both `Token` and `Tokens` JSON schemas.

```json
var tokenProperties =
{
  "type"      : "object",
  "properties":
  {
    "urlString":
    {
      "description": "A delegation token encoded as a URL safe string.",
      "type"       : "string",
      "required"   : true
    }
  }
}
```

### Tokens JSON Schema

A `Tokens` JSON object represents an array of `Token` JSON objects.

```json
{
  "name"      : "Tokens",
  "properties":
  {
    "Tokens":
    {
      "type"      : "object",
      "properties":
      {
        "Token":
        {
          "description": "An array of Token",
          "type"       : "array",
          "items"      : "Token": tokenProperties      //See Token Properties
        }
      }
    }
  }
}
```

See also: [`Token` Properties](#Token_Properties), [`GETDELEGATIONTOKENS`](#Get_Delegation_Tokens), the note in [Delegation](#Delegation).

HTTP Query Parameter Dictionary
-------------------------------

### ACL Spec

| Name | `aclspec` |
|:---- |:---- |
| Description | The ACL spec included in ACL modification operations. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | See [Permissions and HDFS](./HdfsPermissionsGuide.html). |
| Syntax | See [Permissions and HDFS](./HdfsPermissionsGuide.html). |

### XAttr Name

| Name | `xattr.name` |
|:---- |:---- |
| Description | The XAttr name of a file/directory. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | Any string prefixed with user./trusted./system./security.. |
| Syntax | Any string prefixed with user./trusted./system./security.. |

### XAttr Value

| Name | `xattr.value` |
|:---- |:---- |
| Description | The XAttr value of a file/directory. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | An encoded value. |
| Syntax | Enclosed in double quotes or prefixed with 0x or 0s. |

See also: [Extended Attributes](./ExtendedAttributes.html)

### XAttr set flag

| Name | `flag` |
|:---- |:---- |
| Description | The XAttr set flag. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | CREATE,REPLACE. |
| Syntax | CREATE,REPLACE. |

See also: [Extended Attributes](./ExtendedAttributes.html)

### XAttr value encoding

| Name | `encoding` |
|:---- |:---- |
| Description | The XAttr value encoding. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | text | hex | base64 |
| Syntax | text | hex | base64 |

See also: [Extended Attributes](./ExtendedAttributes.html)

### Access Time

| Name | `accesstime` |
|:---- |:---- |
| Description | The access time of a file/directory. |
| Type | long |
| Default Value | -1 (means keeping it unchanged) |
| Valid Values | -1 or a timestamp |
| Syntax | Any integer. |

See also: [`SETTIMES`](#Set_Access_or_Modification_Time)

### Block Size

| Name | `blocksize` |
|:---- |:---- |
| Description | The block size of a file. |
| Type | long |
| Default Value | Specified in the configuration. |
| Valid Values | \> 0 |
| Syntax | Any integer. |

See also: [`CREATE`](#Create_and_Write_to_a_File)

### Buffer Size

| Name | `buffersize` |
|:---- |:---- |
| Description | The size of the buffer used in transferring data. |
| Type | int |
| Default Value | Specified in the configuration. |
| Valid Values | \> 0 |
| Syntax | Any integer. |

See also: [`CREATE`](#Create_and_Write_to_a_File), [`APPEND`](#Append_to_a_File), [`OPEN`](#Open_and_Read_a_File)

### Create Parent

| Name | `createparent` |
|:---- |:---- |
| Description | If the parent directories do not exist, should they be created? |
| Type | boolean |
| Default Value | false |
| Valid Values | true |
| Syntax | true |

See also: [`CREATESYMLINK`](#Create_a_Symbolic_Link)

### Delegation

| Name | `delegation` |
|:---- |:---- |
| Description | The delegation token used for authentication. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | An encoded token. |
| Syntax | See the note below. |

**Note** that delegation tokens are encoded as a URL safe string; see `encodeToUrlString()` and `decodeFromUrlString(String)` in `org.apache.hadoop.security.token.Token` for the details of the encoding.

See also: [Authentication](#Authentication)

### Destination

| Name | `destination` |
|:---- |:---- |
| Description | The destination path. |
| Type | Path |
| Default Value | \<empty\> (an invalid path) |
| Valid Values | An absolute FileSystem path without scheme and authority. |
| Syntax | Any path. |

See also: [`CREATESYMLINK`](#Create_a_Symbolic_Link), [`RENAME`](#Rename_a_FileDirectory)

### Do As

| Name | `doas` |
|:---- |:---- |
| Description | Allowing a proxy user to do as another user. |
| Type | String |
| Default Value | null |
| Valid Values | Any valid username. |
| Syntax | Any string. |

See also: [Proxy Users](#Proxy_Users)

### Fs Action

| Name | `fsaction` |
|:---- |:---- |
| Description | File system operation read/write/execute |
| Type | String |
| Default Value | null (an invalid value) |
| Valid Values | Strings matching regex pattern  "[rwx-]{3} " |
| Syntax |  "[rwx-]{3} " |

See also: [`CHECKACCESS`](#Check_access),

### Group

| Name | `group` |
|:---- |:---- |
| Description | The name of a group. |
| Type | String |
| Default Value | \<empty\> (means keeping it unchanged) |
| Valid Values | Any valid group name. |
| Syntax | Any string. |

See also: [`SETOWNER`](#Set_Owner)

### Length

| Name | `length` |
|:---- |:---- |
| Description | The number of bytes to be processed. |
| Type | long |
| Default Value | null (means the entire file) |
| Valid Values | \>= 0 or null |
| Syntax | Any integer. |

See also: [`OPEN`](#Open_and_Read_a_File)

### Modification Time

| Name | `modificationtime` |
|:---- |:---- |
| Description | The modification time of a file/directory. |
| Type | long |
| Default Value | -1 (means keeping it unchanged) |
| Valid Values | -1 or a timestamp |
| Syntax | Any integer. |

See also: [`SETTIMES`](#Set_Access_or_Modification_Time)

### New Length

| Name | `newlength` |
|:---- |:---- |
| Description | The size the file is to be truncated to. |
| Type | long |
| Valid Values | \>= 0 |
| Syntax | Any long. |

### Offset

| Name | `offset` |
|:---- |:---- |
| Description | The starting byte position. |
| Type | long |
| Default Value | 0 |
| Valid Values | \>= 0 |
| Syntax | Any integer. |

See also: [`OPEN`](#Open_and_Read_a_File)

### Old Snapshot Name

| Name | `oldsnapshotname` |
|:---- |:---- |
| Description | The old name of the snapshot to be renamed. |
| Type | String |
| Default Value | null |
| Valid Values | An existing snapshot name. |
| Syntax | Any string. |

See also: [`RENAMESNAPSHOT`](#Rename_Snapshot)

### Op

| Name | `op` |
|:---- |:---- |
| Description | The name of the operation to be executed. |
| Type | enum |
| Default Value | null (an invalid value) |
| Valid Values | Any valid operation name. |
| Syntax | Any string. |

See also: [Operations](#Operations)

### Overwrite

| Name | `overwrite` |
|:---- |:---- |
| Description | If a file already exists, should it be overwritten? |
| Type | boolean |
| Default Value | false |
| Valid Values | true |
| Syntax | true |

See also: [`CREATE`](#Create_and_Write_to_a_File)

### Owner

| Name | `owner` |
|:---- |:---- |
| Description | The username who is the owner of a file/directory. |
| Type | String |
| Default Value | \<empty\> (means keeping it unchanged) |
| Valid Values | Any valid username. |
| Syntax | Any string. |

See also: [`SETOWNER`](#Set_Owner)

### Permission

| Name | `permission` |
|:---- |:---- |
| Description | The permission of a file/directory. |
| Type | Octal |
| Default Value | 755 |
| Valid Values | 0 - 1777 |
| Syntax | Any radix-8 integer (leading zeros may be omitted.) |

See also: [`CREATE`](#Create_and_Write_to_a_File), [`MKDIRS`](#Make_a_Directory), [`SETPERMISSION`](#Set_Permission)

### Recursive

| Name | `recursive` |
|:---- |:---- |
| Description | Should the operation act on the content in the subdirectories? |
| Type | boolean |
| Default Value | false |
| Valid Values | true |
| Syntax | true |

See also: [`RENAME`](#Rename_a_FileDirectory)

### Renewer

| Name | `renewer` |
|:---- |:---- |
| Description | The username of the renewer of a delegation token. |
| Type | String |
| Default Value | \<empty\> (means the current user) |
| Valid Values | Any valid username. |
| Syntax | Any string. |

See also: [`GETDELEGATIONTOKEN`](#Get_Delegation_Token), [`GETDELEGATIONTOKENS`](#Get_Delegation_Tokens)

### Replication

| Name | `replication` |
|:---- |:---- |
| Description | The number of replications of a file. |
| Type | short |
| Default Value | Specified in the configuration. |
| Valid Values | \> 0 |
| Syntax | Any integer. |

See also: [`CREATE`](#Create_and_Write_to_a_File), [`SETREPLICATION`](#Set_Replication_Factor)

### Snapshot Name

| Name | `snapshotname` |
|:---- |:---- |
| Description | The name of the snapshot to be created/deleted. Or the new name for snapshot rename. |
| Type | String |
| Default Value | null |
| Valid Values | Any valid snapshot name. |
| Syntax | Any string. |

See also: [`CREATESNAPSHOT`](#Create_Snapshot), [`DELETESNAPSHOT`](#Delete_Snapshot), [`RENAMESNAPSHOT`](#Rename_Snapshot)

### Sources

| Name | `sources` |
|:---- |:---- |
| Description | A list of source paths. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | A list of comma seperated absolute FileSystem paths without scheme and authority. |
| Syntax | Any string. |

See also: [`CONCAT`](#Concat_Files)

### Token

| Name | `token` |
|:---- |:---- |
| Description | The delegation token used for the operation. |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | An encoded token. |
| Syntax | See the note in [Delegation](#Delegation). |

See also: [`RENEWDELEGATIONTOKEN`](#Renew_Delegation_Token), [`CANCELDELEGATIONTOKEN`](#Cancel_Delegation_Token)

### Token Kind

| Name | `kind` |
|:---- |:---- |
| Description | The kind of the delegation token requested |
| Type | String |
| Default Value | \<empty\> (Server sets the default kind for the service) |
| Valid Values | A string that represents token kind e.g "HDFS\_DELEGATION\_TOKEN" or "WEBHDFS delegation" |
| Syntax | Any string. |

See also: [`GETDELEGATIONTOKEN`](#Get_Delegation_Token)

### Token Service

| Name | `service` |
|:---- |:---- |
| Description | The name of the service where the token is supposed to be used, e.g. ip:port of the namenode |
| Type | String |
| Default Value | \<empty\> |
| Valid Values | ip:port in string format or logical name of the service |
| Syntax | Any string. |

See also: [`GETDELEGATIONTOKEN`](#Get_Delegation_Token)

### Username

| Name | `user.name` |
|:---- |:---- |
| Description | The authenticated user; see [Authentication](#Authentication). |
| Type | String |
| Default Value | null |
| Valid Values | Any valid username. |
| Syntax | Any string. |

See also: [Authentication](#Authentication)
