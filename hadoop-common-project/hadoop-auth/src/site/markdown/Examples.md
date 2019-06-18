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

Hadoop Auth, Java HTTP SPNEGO - Examples
========================================

Accessing a Hadoop Auth protected URL Using a browser
-----------------------------------------------------

**IMPORTANT:** The browser must support HTTP Kerberos SPNEGO. For example, Firefox or Internet Explorer.

For Firefox access the low level configuration page by loading the `about:config` page. Then go to the `network.negotiate-auth.trusted-uris` preference and add the hostname or the domain of the web server that is HTTP Kerberos SPNEGO protected (if using multiple domains and hostname use comma to separate them).

Accessing a Hadoop Auth protected URL Using `curl`
--------------------------------------------------

**IMPORTANT:** The `curl` version must support GSS, run `curl -V`.

    $ curl -V
    curl 7.19.7 (universal-apple-darwin10.0) libcurl/7.19.7 OpenSSL/0.9.8l zlib/1.2.3
    Protocols: tftp ftp telnet dict ldap http file https ftps
    Features: GSS-Negotiate IPv6 Largefile NTLM SSL libz

Login to the KDC using **kinit** and then use `curl` to fetch protected URL:

    $ kinit
    Please enter the password for tucu@LOCALHOST:
    $ curl --negotiate -u : -b ~/cookiejar.txt -c ~/cookiejar.txt http://$(hostname -f):8080/hadoop-auth-examples/kerberos/who
    Enter host password for user 'tucu':

    Hello Hadoop Auth Examples!

*   The `--negotiate` option enables SPNEGO in `curl`.

*   The `-u :` option is required but the user ignored (the principal
    that has been kinit-ed is used).

*   The `-b` and `-c` are use to store and send HTTP Cookies.

Using the Java Client
---------------------

Use the `AuthenticatedURL` class to obtain an authenticated HTTP connection:

    ...
    URL url = new URL("http://localhost:8080/hadoop-auth/kerberos/who");
    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    ...
    HttpURLConnection conn = new AuthenticatedURL().openConnection(url, token);
    ...
    conn = new AuthenticatedURL().openConnection(url, token);
    ...

Building and Running the Examples
---------------------------------

Download Hadoop-Auth's source code, the examples are in the `src/main/examples` directory.

### Server Example:

Edit the `hadoop-auth-examples/src/main/webapp/WEB-INF/web.xml` and set the right configuration init parameters for the `AuthenticationFilter` definition configured for Kerberos (the right Kerberos principal and keytab file must be specified). Refer to the [Configuration document](./Configuration.html) for details.

Create the web application WAR file by running the `mvn package` command.

Deploy the WAR file in a servlet container. For example, if using Tomcat, copy the WAR file to Tomcat's `webapps/` directory.

Start the servlet container.

### Accessing the server using `curl`

Try accessing protected resources using `curl`. The protected resources are:

    $ kinit
    Please enter the password for tucu@LOCALHOST:

    $ curl http://localhost:8080/hadoop-auth-examples/anonymous/who

    $ curl http://localhost:8080/hadoop-auth-examples/simple/who?user.name=foo

    $ curl --negotiate -u : -b ~/cookiejar.txt -c ~/cookiejar.txt http://$(hostname -f):8080/hadoop-auth-examples/kerberos/who

### Accessing the server using the Java client example

    $ kinit
    Please enter the password for tucu@LOCALHOST:

    $ cd examples

    $ mvn exec:java -Durl=http://localhost:8080/hadoop-auth-examples/kerberos/who

    ....

    Token value: "u=tucu,p=tucu@LOCALHOST,t=kerberos,e=1295305313146,s=sVZ1mpSnC5TKhZQE3QLN5p2DWBo="
    Status code: 200 OK

    You are: user[tucu] principal[tucu@LOCALHOST]

    ....
