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

Hadoop HDFS over HTTP - Using HTTP Tools
========================================

Security
--------

Out of the box HttpFS supports both pseudo authentication and Kerberos HTTP SPNEGO authentication.

### Pseudo Authentication

With pseudo authentication the user name must be specified in the `user.name=<USERNAME>` query string parameter of a HttpFS URL. For example:

    $ curl "http://<HTTFS_HOST>:14000/webhdfs/v1?op=homedir&user.name=babu"

### Kerberos HTTP SPNEGO Authentication

Kerberos HTTP SPNEGO authentication requires a tool or library supporting Kerberos HTTP SPNEGO protocol.

IMPORTANT: If using `curl`, the `curl` version being used must support GSS (`curl -V` prints out 'GSS' if it supports it).

For example:

    $ kinit
    Please enter the password for user@LOCALHOST:
    $ curl --negotiate -u foo "http://<HTTPFS_HOST>:14000/webhdfs/v1?op=homedir"
    Enter host password for user 'foo':

NOTE: the `-u USER` option is required by the `--negotiate` but it is not used. Use any value as `USER` and when asked for the password press [ENTER] as the password value is ignored.

### Remembering Who I Am (Establishing an Authenticated Session)

As most authentication mechanisms, Hadoop HTTP authentication authenticates users once and issues a short-lived authentication token to be presented in subsequent requests. This authentication token is a signed HTTP Cookie.

When using tools like `curl`, the authentication token must be stored on the first request doing authentication, and submitted in subsequent requests. To do this with curl the `-b` and `-c` options to save and send HTTP Cookies must be used.

For example, the first request doing authentication should save the received HTTP Cookies.

Using Pseudo Authentication:

    $ curl -c ~/.httpfsauth "http://<HTTPFS_HOST>:14000/webhdfs/v1?op=homedir&user.name=foo"

Using Kerberos HTTP SPNEGO authentication:

    $ curl --negotiate -u foo -c ~/.httpfsauth "http://<HTTPFS_HOST>:14000/webhdfs/v1?op=homedir"

Then, subsequent requests forward the previously received HTTP Cookie:

    $ curl -b ~/.httpfsauth "http://<HTTPFS_HOST>:14000/webhdfs/v1?op=liststatus"
