---
title: "Ozone Security Overview"
date: "2019-April-03"
menu:
   main:
       parent: Architecture
weight: 11
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

# Security in Ozone #
Starting with badlands release (ozone-0.4.0-alpha) ozone cluster can be secured against external threats. Specifically it can be configured for following security features:

1. Authentication
2. Authorization
3. Audit
4. Transparent Data Encryption (TDE)

## Authentication ##
### Kerberos ###
Similar to hadoop, Ozone allows kerberos-based authentication. So one way to setup identities for all the daemons and clients is to create kerberos keytabs and configure it like any other service in hadoop.

### Tokens ###
Tokens are widely used in Hadoop to achieve lightweight authentication without compromising on security. Main motivation for using tokens inside Ozone is to prevent the unauthorized access while keeping the protocol lightweight and without sharing secret over the wire. Ozone utilizes three types of token:

#### Delegation token ####
Once client establishes their identity via kerberos they can request a delegation token from OzoneManager. This token can be used by a client to prove its identity until the token expires. Like Hadoop delegation tokens, an Ozone delegation token has 3 important fields:

Renewer:    User responsible for renewing the token.
Issue date:  Time at which token was issued.
Max date:    Time after which token can’t be renewed.

Token operations like get, renew and cancel can only be performed over an Kerberos authenticated connection. Clients can use delegation token to establish connection with OzoneManager and perform any file system/object store related operations like, listing the objects in a bucket or creating a volume etc.

#### Block Tokens ####
Block tokens are similar to delegation tokens in sense that they are signed by OzoneManager. Block tokens are created by OM (OzoneManager) when a client request involves interaction with DataNodes such as read/write Ozone keys. Unlike delegation tokens there is no client API to request block tokens. Instead, they are handed transparently to client along with key/block locations. Block tokens are validated by Datanodes when receiving read/write requests from clients. Block token can't be renewed explicitly by client. Client with expired block token will need to refetch the key/block locations to get new block tokens.
#### S3Token ####
Like block tokens S3Tokens are handled transparently for clients. It is signed by S3secret created by client. S3Gateway creates this token for every s3 client request. To create an S3Token user must have a S3 secret.

### Certificates ###
Apart from kerberos and tokens Ozone utilizes certificate based authentication for Ozone service components. To enable this, SCM (StorageContainerManager) bootstraps itself as an Certificate Authority when security is enabled. This allows all daemons inside Ozone to have an SCM signed certificate. Below is brief descriptions of steps involved:
Datanodes and OzoneManagers submits a CSR (certificate signing request) to SCM.
SCM verifies identity of DN (Datanode) or OM via Kerberos and generates a certificate.
This certificate is used by OM and DN to prove their identities.
Datanodes use OzoneManager certificate to validate block tokens. This is possible because both of them trust SCM signed certificates. (i.e OzoneManager and Datanodes)

## Authorization ##
Ozone provides a pluggable API to control authorization of all client related operations. Default implementation allows every request. Clearly it is not meant for production environments. To configure a more fine grained policy one may configure Ranger plugin for Ozone. Since it is a pluggable module clients can also implement their own custom authorization policy and configure it using [ozone.acl.authorizer.class].

## Audit ##
Ozone provides ability to audit all read & write operations to OM, SCM and Datanodes. Ozone audit leverages the Marker feature which enables user to selectively audit only READ or WRITE operations by a simple config change without restarting the service(s).
To enable/disable audit of READ operations, set filter.read.onMatch to NEUTRAL or DENY respectively. Similarly, the audit of WRITE operations can be controlled using filter.write.onMatch.

Generating audit logs is only half the job, so Ozone also provides AuditParser - a sqllite based command line utility to parse/query audit logs with predefined templates(ex. Top 5 commands) and options for custom query. Once the log file has been loaded to AuditParser, one can simply run a template as shown below:
ozone auditparser <path to db file> template top5cmds

Similarly, users can also execute custom query using:
ozone auditparser <path to db file> query "select * from audit where level=='FATAL'"

## Transparent Data Encryption ##
Ozone TDE setup process and usage are very similar to HDFS TDE. The major difference is that Ozone TDE is enabled at Ozone bucket level when a bucket is created.

To create an encrypted bucket, client need to

* Create a bucket encryption key with hadoop key CLI (same as you do for HDFS encryption zone key)
```
hadoop key create key1
```
* Create an encrypted bucket with -k option
```
ozone sh bucket create -k key1 /vol1/ez1
```
After that the usage will be transparent to the client and end users, i.e., all data written to encrypted bucket are encrypted at datanodes.

To know more about how to setup a secure Ozone cluster refer to [How to setup secure Ozone cluster]("SetupSecureOzone.md")
Ozone [security architecture document](https://issues.apache.org/jira/secure/attachment/12911638/HadoopStorageLayerSecurity.pdf) can be referred for a deeper dive into Ozone Security architecture.