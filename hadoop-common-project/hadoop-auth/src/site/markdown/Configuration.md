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

Hadoop Auth, Java HTTP SPNEGO - Server Side Configuration
=========================================================

Server Side Configuration Setup
-------------------------------

The AuthenticationFilter filter is Hadoop Auth's server side component.

This filter must be configured in front of all the web application resources that required authenticated requests. For example:

The Hadoop Auth and dependent JAR files must be in the web application classpath (commonly the `WEB-INF/lib` directory).

Hadoop Auth uses SLF4J-API for logging. Auth Maven POM dependencies define the SLF4J API dependency but it does not define the dependency on a concrete logging implementation, this must be addded explicitly to the web application. For example, if the web applicationan uses Log4j, the SLF4J-LOG4J12 and LOG4J jar files must be part of the web application classpath as well as the Log4j configuration file.

### Common Configuration parameters

*   `config.prefix`: If specified, all other configuration parameter names
    must start with the prefix. The default value is no prefix.

*   `[PREFIX.]type`: the authentication type keyword (`simple` or \
    `kerberos`) or a Authentication handler implementation.

*   `[PREFIX.]signature.secret.file`: When `signer.secret.provider` is set to
    `file`, this is the location of file including the secret used to sign the HTTP cookie.

*   `[PREFIX.]token.validity`: The validity -in seconds- of the generated
    authentication token. The default value is `36000` seconds. This is also
    used for the rollover interval when `signer.secret.provider` is set to
    `random` or `zookeeper`.

*   `[PREFIX.]cookie.domain`: domain to use for the HTTP cookie that stores
    the authentication token.

*   `[PREFIX.]cookie.path`: path to use for the HTTP cookie that stores the
    authentication token.

*   `signer.secret.provider`: indicates the name of the SignerSecretProvider
    class to use. Possible values are: `file`, `random`,
    `zookeeper`, or a classname. If not specified, the `file`
    implementation will be used; and failing that, the `random`
    implementation will be used. If "file" is to be used, one need to specify
    `signature.secret.file` and point to the secret file.

### Kerberos Configuration

**IMPORTANT**: A KDC must be configured and running.

To use Kerberos SPNEGO as the authentication mechanism, the authentication filter must be configured with the following init parameters:

*   `[PREFIX.]type`: the keyword `kerberos`.

*   `[PREFIX.]kerberos.principal`: The web-application Kerberos principal
    name. The Kerberos principal name must start with `HTTP/...`. For
    example: `HTTP/localhost@LOCALHOST`. There is no default value.

*   `[PREFIX.]kerberos.keytab`: The path to the keytab file containing
    the credentials for the kerberos principal. For example:
    `/Users/tucu/tucu.keytab`. There is no default value.

**Example**:

```xml
    <web-app version="2.5" xmlns="http://java.sun.com/xml/ns/javaee">
        ...

        <filter>
            <filter-name>kerberosFilter</filter-name>
            <filter-class>org.apache.hadoop.security.authentication.server.AuthenticationFilter</filter-class>
            <init-param>
                <param-name>type</param-name>
                <param-value>kerberos</param-value>
            </init-param>
            <init-param>
                <param-name>token.validity</param-name>
                <param-value>30</param-value>
            </init-param>
            <init-param>
                <param-name>cookie.domain</param-name>
                <param-value>.foo.com</param-value>
            </init-param>
            <init-param>
                <param-name>cookie.path</param-name>
                <param-value>/</param-value>
            </init-param>
            <init-param>
                <param-name>kerberos.principal</param-name>
                <param-value>HTTP/localhost@LOCALHOST</param-value>
            </init-param>
            <init-param>
                <param-name>kerberos.keytab</param-name>
                <param-value>/tmp/auth.keytab</param-value>
            </init-param>
        </filter>

        <filter-mapping>
            <filter-name>kerberosFilter</filter-name>
            <url-pattern>/kerberos/*</url-pattern>
        </filter-mapping>

        ...
    </web-app>
```

### Pseudo/Simple Configuration

To use Pseudo/Simple as the authentication mechanism (trusting the value of the query string parameter 'user.name'), the authentication filter must be configured with the following init parameters:

*   `[PREFIX.]type`: the keyword `simple`.

*   `[PREFIX.]simple.anonymous.allowed`: is a boolean parameter that
    indicates if anonymous requests are allowed or not. The default value is
    `false`.

**Example**:

```xml
    <web-app version="2.5" xmlns="http://java.sun.com/xml/ns/javaee">
        ...

        <filter>
            <filter-name>simpleFilter</filter-name>
            <filter-class>org.apache.hadoop.security.authentication.server.AuthenticationFilter</filter-class>
            <init-param>
                <param-name>type</param-name>
                <param-value>simple</param-value>
            </init-param>
            <init-param>
                <param-name>token.validity</param-name>
                <param-value>30</param-value>
            </init-param>
            <init-param>
                <param-name>cookie.domain</param-name>
                <param-value>.foo.com</param-value>
            </init-param>
            <init-param>
                <param-name>cookie.path</param-name>
                <param-value>/</param-value>
            </init-param>
            <init-param>
                <param-name>simple.anonymous.allowed</param-name>
                <param-value>false</param-value>
            </init-param>
        </filter>

        <filter-mapping>
            <filter-name>simpleFilter</filter-name>
            <url-pattern>/simple/*</url-pattern>
        </filter-mapping>

        ...
    </web-app>
```

### AltKerberos Configuration

**IMPORTANT**: A KDC must be configured and running.

The AltKerberos authentication mechanism is a partially implemented derivative of the Kerberos SPNEGO authentication mechanism which allows a "mixed" form of authentication where Kerberos SPNEGO is used by non-browsers while an alternate form of authentication (to be implemented by the user) is used for browsers. To use AltKerberos as the authentication mechanism (besides providing an implementation), the authentication filter must be configured with the following init parameters, in addition to the previously mentioned Kerberos SPNEGO ones:

*   `[PREFIX.]type`: the full class name of the implementation of
    AltKerberosAuthenticationHandler to use.

*   `[PREFIX.]alt-kerberos.non-browser.user-agents`: a comma-separated
    list of which user-agents should be considered non-browsers.

**Example**:

```xml
    <web-app version="2.5" xmlns="http://java.sun.com/xml/ns/javaee">
        ...

        <filter>
            <filter-name>kerberosFilter</filter-name>
            <filter-class>org.apache.hadoop.security.authentication.server.AuthenticationFilter</filter-class>
            <init-param>
                <param-name>type</param-name>
                <param-value>org.my.subclass.of.AltKerberosAuthenticationHandler</param-value>
            </init-param>
            <init-param>
                <param-name>alt-kerberos.non-browser.user-agents</param-name>
                <param-value>java,curl,wget,perl</param-value>
            </init-param>
            <init-param>
                <param-name>token.validity</param-name>
                <param-value>30</param-value>
            </init-param>
            <init-param>
                <param-name>cookie.domain</param-name>
                <param-value>.foo.com</param-value>
            </init-param>
            <init-param>
                <param-name>cookie.path</param-name>
                <param-value>/</param-value>
            </init-param>
            <init-param>
                <param-name>kerberos.principal</param-name>
                <param-value>HTTP/localhost@LOCALHOST</param-value>
            </init-param>
            <init-param>
                <param-name>kerberos.keytab</param-name>
                <param-value>/tmp/auth.keytab</param-value>
            </init-param>
        </filter>

        <filter-mapping>
            <filter-name>kerberosFilter</filter-name>
            <url-pattern>/kerberos/*</url-pattern>
        </filter-mapping>

        ...
    </web-app>
```

### LDAP Configuration

**IMPORTANT**: A LDAP server must be configured and running. When TLS is enabled for communication with LDAP server (either via ldaps scheme or 'start TLS' extension), configure the public certificate of the LDAP server in the local truststore.

The LDAP authentication mechanism uses HTTP Basic authentication scheme to verify user specified credentials against a configured LDAP (or Active
Directory) server. The authentication filter must be configured with the following init parameters:

*   `[PREFIX.]type`: The keyword `ldap`.

*   `[PREFIX.]ldap.providerurl`: The url of the LDAP server.

*   `[PREFIX.]ldap.basedn`: The base distinguished name (DN) to be used with the LDAP server. This value is appended to the provided user id for authentication purpose. This property is not useful in case of Active Directory server.

*   `[PREFIX.]ldap.binddomain`: The LDAP bind domain value to be used with the LDAP server. This property is optional and useful only in case of Active Directory server (e.g. example.com).

*   `[PREFIX.]ldap.enablestarttls`: A boolean value used to define if the LDAP server supports 'StartTLS' extension.

**Example**:

```xml
    <web-app version="2.5" xmlns="http://java.sun.com/xml/ns/javaee">
        ...

        <filter>
            <filter-name>authFilter</filter-name>
            <filter-class>org.apache.hadoop.security.authentication.server.AuthenticationFilter</filter-class>
            <init-param>
                <param-name>type</param-name>
                <param-value>ldap</param-value>
            </init-param>
            <init-param>
                <param-name>ldap.providerurl</param-name>
                <param-value>ldap://ldap-server-host:8920</param-value>
            </init-param>
            <init-param>
                <param-name>ldap.basedn</param-name>
                <param-value>ou=users,dc=example,dc=com</param-value>
            </init-param>
            <init-param>
                <param-name>ldap.enablestarttls</param-name>
                <param-value>true</param-value>
            </init-param>
         </filter>

        <filter-mapping>
            <filter-name>authFilter</filter-name>
            <url-pattern>/ldap/*</url-pattern>
        </filter-mapping>

        ...
    </web-app>
```

### Multi-scheme Configuration

**IMPORTANT**: This configuration supports multiple authentication mechanisms (e.g. kerberos, ldap etc.) together. Please refer to the documentation for each individual scheme for configuration related details.

The multi-scheme authentication mechanism supports multiple authentication mechanisms (e.g. kerberos, ldap etc.) by implementing a HTTP auth negotiation mechanism (Please refer to RFC-2616). For enabling each type of authentication mechanism (e.g. ldap) a corresponding authentication handler must be configured. Please refer to following configuration parameters:

*   `[PREFIX.]type`: The keyword `multi-scheme`.

*   `[PREFIX.]multi-scheme-auth-handler.schemes`: A comma separated list of HTTP authentication mechanisms supported by this handler. It is a required parameter and it does not have a default value (e.g. multi-scheme-auth-handler.schemes=basic,negotiate).

*   `[PREFIX.]multi-scheme-auth-handler.schemes.<scheme-name>.handler`: The authentication handler implementation to be used for the specified authentication scheme. It does not have a default value (e.g. multi-scheme-auth-handler.schemes.negotiate.handler=kerberos). Add this handler configuration for each of the scheme configured.

In addition to these parameters, please specify the init parameters for each handler configured as well.


**Example**:

```xml
    <web-app version="2.5" xmlns="http://java.sun.com/xml/ns/javaee">
        ...

        <filter>
            <filter-name>authFilter</filter-name>
            <filter-class>org.apache.hadoop.security.authentication.server.AuthenticationFilter</filter-class>
            <init-param>
                <param-name>type</param-name>
                <param-value>multi-scheme</param-value>
            </init-param>
            <init-param>
                <param-name>multi-scheme-auth-handler.schemes</param-name>
                <param-value>basic,negotiate</param-value>
            </init-param>
            <init-param>
                <param-name>multi-scheme-auth-handler.basic.handler</param-name>
                <param-value>ldap</param-value>
            </init-param>
            <init-param>
                <param-name>multi-scheme-auth-handler.negotiate.handler</param-name>
                <param-value>kerberos</param-value>
            </init-param>
            <init-param>
                <param-name>ldap.providerurl</param-name>
                <param-value>ldap://ldap-server-host:8920</param-value>
            </init-param>
            <init-param>
                <param-name>ldap.basedn</param-name>
                <param-value>ou=users,dc=example,dc=com</param-value>
            </init-param>
            <init-param>
                <param-name>ldap.enablestarttls</param-name>
                <param-value>true</param-value>
            </init-param>
            <init-param>
                <param-name>token.validity</param-name>
                <param-value>30</param-value>
            </init-param>
            <init-param>
                <param-name>cookie.domain</param-name>
                <param-value>.foo.com</param-value>
            </init-param>
            <init-param>
                <param-name>cookie.path</param-name>
                <param-value>/</param-value>
            </init-param>
            <init-param>
                <param-name>kerberos.principal</param-name>
                <param-value>HTTP/localhost@LOCALHOST</param-value>
            </init-param>
            <init-param>
                <param-name>kerberos.keytab</param-name>
                <param-value>/tmp/auth.keytab</param-value>
            </init-param>
         </filter>

        <filter-mapping>
            <filter-name>authFilter</filter-name>
            <url-pattern>/multi-scheme/*</url-pattern>
        </filter-mapping>

        ...
    </web-app>
```


### SignerSecretProvider Configuration

The SignerSecretProvider is used to provide more advanced behaviors for the secret used for signing the HTTP Cookies.

These are the relevant configuration properties:

*   `signer.secret.provider`: indicates the name of the
    SignerSecretProvider class to use. Possible values are: "file",
    "random", "zookeeper", or a classname. If not specified, the "file"
    implementation will be used; and failing that, the "random" implementation
    will be used. If "file" is to be used, one need to specify `signature.secret.file`
    and point to the secret file.

*   `[PREFIX.]signature.secret.file`: When `signer.secret.provider` is set
    to `file` or not specified, this is the value for the secret used to
    sign the HTTP cookie.

*   `[PREFIX.]token.validity`: The validity -in seconds- of the generated
    authentication token. The default value is `36000` seconds. This is
    also used for the rollover interval when `signer.secret.provider` is
    set to `random` or `zookeeper`.

The following configuration properties are specific to the `zookeeper` implementation:

*   `signer.secret.provider.zookeeper.connection.string`: Indicates the
    ZooKeeper connection string to connect with. The default value is `localhost:2181`

*   `signer.secret.provider.zookeeper.path`: Indicates the ZooKeeper path
    to use for storing and retrieving the secrets. All servers
    that need to coordinate their secret should point to the same path

*   `signer.secret.provider.zookeeper.auth.type`: Indicates the auth type
    to use. Supported values are `none` and `sasl`. The default
    value is `none`.

*   `signer.secret.provider.zookeeper.kerberos.keytab`: Set this to the
    path with the Kerberos keytab file. This is only required if using
    Kerberos.

*   `signer.secret.provider.zookeeper.kerberos.principal`: Set this to the
    Kerberos principal to use. This only required if using Kerberos.

*   `signer.secret.provider.zookeeper.disconnect.on.shutdown`: Whether to close the
    ZooKeeper connection when the provider is shutdown. The default value is `true`.
    Only set this to `false` if a custom Curator client is being provided and
    the disconnection is being handled elsewhere.

The following attribute in the ServletContext can also be set if desired:
*   `signer.secret.provider.zookeeper.curator.client`: A CuratorFramework client
    object can be passed here. If given, the "zookeeper" implementation will use
    this Curator client instead of creating its own, which is useful if you already
    have a Curator client or want more control over its configuration.

**Example**:

```xml
    <web-app version="2.5" xmlns="http://java.sun.com/xml/ns/javaee">
        ...

        <filter>
            <!-- AuthenticationHandler configs not shown -->
            <init-param>
                <param-name>signer.secret.provider</param-name>
                <param-value>file</param-value>
            </init-param>
            <init-param>
                <param-name>signature.secret.file</param-name>
                <param-value>/myapp/secret_file</param-value>
            </init-param>
        </filter>

        ...
    </web-app>
```

**Example**:

```xml
    <web-app version="2.5" xmlns="http://java.sun.com/xml/ns/javaee">
        ...

        <filter>
            <!-- AuthenticationHandler configs not shown -->
            <init-param>
                <param-name>signer.secret.provider</param-name>
                <param-value>random</param-value>
            </init-param>
            <init-param>
                <param-name>token.validity</param-name>
                <param-value>30</param-value>
            </init-param>
        </filter>

        ...
    </web-app>
```

**Example**:

```xml
    <web-app version="2.5" xmlns="http://java.sun.com/xml/ns/javaee">
        ...

        <filter>
            <!-- AuthenticationHandler configs not shown -->
            <init-param>
                <param-name>signer.secret.provider</param-name>
                <param-value>zookeeper</param-value>
            </init-param>
            <init-param>
                <param-name>token.validity</param-name>
                <param-value>30</param-value>
            </init-param>
            <init-param>
                <param-name>signer.secret.provider.zookeeper.connection.string</param-name>
                <param-value>zoo1:2181,zoo2:2181,zoo3:2181</param-value>
            </init-param>
            <init-param>
                <param-name>signer.secret.provider.zookeeper.path</param-name>
                <param-value>/myapp/secrets</param-value>
            </init-param>
            <init-param>
                <param-name>signer.secret.provider.zookeeper.kerberos.keytab</param-name>
                <param-value>/tmp/auth.keytab</param-value>
            </init-param>
            <init-param>
                <param-name>signer.secret.provider.zookeeper.kerberos.principal</param-name>
                <param-value>HTTP/localhost@LOCALHOST</param-value>
            </init-param>
        </filter>

        ...
    </web-app>
```
