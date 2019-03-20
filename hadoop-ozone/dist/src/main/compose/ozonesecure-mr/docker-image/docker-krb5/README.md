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

# Experimental UNSECURE krb5 Kerberos container.

Only for development. Not for production.

The docker image contains a rest service which provides keystore and keytab files without any authentication!

Master password: Welcome1

Principal: admin/admin@EXAMPLE.COM Password: Welcome1

Test:

```
docker run --net=host krb5

docker run --net=host -it --entrypoint=bash krb5
kinit admin/admin
#pwd: Welcome1
klist
```
