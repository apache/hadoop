---
title: "Setup secure ozone cluster"
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
# Setup secure ozone cluster #
To enable security in ozone cluster **ozone.security.enabled** should be set to true.

ozone.security.enabled| true
----------------------|------
## Kerberos ##
Configuration for service daemons:

Property|Description
--------|------------------------------------------------------------
hdds.scm.kerberos.principal     | The SCM service principal. Ex scm/_HOST@REALM.COM_
hdds.scm.kerberos.keytab.file   |The keytab file used by SCM daemon to login as its service principal.
ozone.om.kerberos.principal     |The OzoneManager service principal. Ex om/_HOST@REALM.COM
ozone.om.kerberos.keytab.file   |The keytab file used by SCM daemon to login as its service principal.
hdds.scm.http.kerberos.principal|SCM http server service principal.
hdds.scm.http.kerberos.keytab   |The keytab file used by SCM http server to login as its service principal.
ozone.om.http.kerberos.principal|OzoneManager http server principal.
ozone.om.http.kerberos.keytab   |The keytab file used by OM http server to login as its service principal.
ozone.s3g.keytab.file           |The keytab file used by S3 gateway. Ex /etc/security/keytabs/HTTP.keytab
ozone.s3g.authentication.kerberos.principal|S3 Gateway principal. Ex HTTP/_HOST@EXAMPLE.COM
## Tokens ##

## Delegation token ##
Delegation tokens are enabled by default when security is enabled.

## Block Tokens ##
hdds.block.token.enabled     | true
-----------------------------|------

## S3Token ##
S3 token are enabled by default when security is enabled.
To use S3 tokens users need to perform following steps:
* S3 clients should get the secret access id and user secret from OzoneManager.
```
ozone s3 getsecret
```
* Setup secret in aws configs:
```
aws configure set default.s3.signature_version s3v4
aws configure set aws_access_key_id ${accessId}
aws configure set aws_secret_access_key ${secret}
aws configure set region us-west-1
```

## Certificates ##
Certificates are used internally inside Ozone. Its enabled be default when security is enabled.

## Authorization ##
Default access authorizer for Ozone approves every request. It is not suitable for production environments. It is recommended that clients use ranger plugin for Ozone to manage authorizations.

Property|Description
--------|------------------------------------------------------------
ozone.acl.enabled         | true
ozone.acl.authorizer.class| org.apache.ranger.authorization.ozone.authorizer.RangerOzoneAuthorizer

## TDE ##
To use TDE clients must set KMS URI.

hadoop.security.key.provider.path  | KMS uri. Ex kms://http@kms-host:9600/kms
-----------------------------------|-----------------------------------------