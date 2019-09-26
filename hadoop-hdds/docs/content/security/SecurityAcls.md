---
title: "Ozone ACLs"
date: "2019-April-03"
weight: 6
summary: Native Ozone Authorizer provides Access Control List (ACL) support for Ozone without Ranger integration.
icon: transfer
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

Ozone supports a set of native ACLs. These ACLs can be used independently or
along with Ranger. If Apache Ranger is enabled, then ACL will be checked
first with Ranger and then Ozone's internal ACLs will be evaluated.

Ozone ACLs are a super set of Posix and S3 ACLs.

The general format of an ACL is _object_:_who_:_rights_.

Where an _object_ can be:

1. **Volume** - An Ozone volume.  e.g. _/volume_
2. **Bucket** - An Ozone bucket. e.g. _/volume/bucket_
3. **Key** - An object key or an object. e.g. _/volume/bucket/key_
4. **Prefix** - A path prefix for a specific key. e.g. _/volume/bucket/prefix1/prefix2_

Where a _who_ can be:

1. **User** - A user in the Kerberos domain. User like in Posix world can be
named or unnamed.
2. **Group** - A group in the Kerberos domain. Group also like in Posix world
can
be named or unnamed.
3. **World** - All authenticated users in the Kerberos domain. This maps to
others in the Posix domain.
4. **Anonymous** - Ignore the user field completely. This is an extension to
the Posix semantics, This is needed for S3 protocol, where we express that
we have no way of knowing who the user is or we don't care.


<div class="alert alert-success" role="alert">
  A S3 user accesing Ozone via AWS v4 signature protocol will be translated
  to the appropriate Kerberos user by Ozone Manager.
</div>

Where a _right_ can be:

1. **Create** – This ACL provides a user the ability to create buckets in a
volume and keys in a bucket. Please note: Under Ozone, Only admins can create volumes.
2. **List** – This ACL allows listing of buckets and keys. This ACL is attached
 to the volume and buckets which allow listing of the child objects. Please note: The user and admins can list the volumes owned by the user.
3. **Delete** – Allows the user to delete a volume, bucket or key.
4. **Read** – Allows the user to read the metadata of a Volume and Bucket and
data stream and metadata of a key.
5. **Write** - Allows the user to write the metadata of a Volume and Bucket and
allows the user to overwrite an existing ozone key.
6. **Read_ACL** – Allows a user to read the ACL on a specific object.
7. **Write_ACL** – Allows a user to write the ACL on a specific object.

<h3>Ozone Native ACL APIs</h3>

The ACLs can be manipulated by a set of APIs supported by Ozone. The APIs
supported are:

1. **SetAcl** – This API will take user principal, the name, type
of the ozone object and a list of ACLs.
2. **GetAcl** – This API will take the name and type of the ozone object
and will return a list of ACLs.
3. **AddAcl** - This API will take the name, type of the ozone object, the
ACL, and add it to existing ACL entries of the ozone object.
4. **RemoveAcl** - This API will take the name, type of the
ozone object and the ACL that has to be removed.
