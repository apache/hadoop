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

# YARN Application Security

Anyone writing a YARN application needs to understand the process, in order
to write short-lived applications or long-lived services. They also need to
start testing on secure clusters during early development stages, in order
to write code that actually works.


## How YARN Security works

YARN Resource Managers (RMs) and Node Managers (NMs) co-operate to execute
the user's application with the identity and hence access rights of that user.

The (active) Resource Manager:

1. Finds space in a cluster to deploy the core of the application,
the Application Master (AM).

1. Requests that the NM on that node allocate a container and start the AM in it.

1. Communicates with the AM, so that the AM can request new containers and
manipulate/release current ones, and to provide notifications about allocated
and running containers.

The Node Managers:

1. *Localize* resources: Download from HDFS or other filesystem into a local directory. This
is done using the delegation tokens attached to the container launch context. (For non-HDFS
resources, using other credentials such as object store login details in cluster configuration
files)

1. Start the application as the user.

1. Monitor the application and report failure to the RM.

To execute code in the cluster, a YARN application must:

1. Have a client-side application which sets up the `ApplicationSubmissionContext`
detailing what is to be launched. This includes:

    * A list of files in HDFS to be localized
    * The environment variables to set in the container.
    * The commands to execute in the container to start the application.
    * The security credentials needed to launch the application

1. Have an Application Master which is actually launched, registers with
the YARN RM and listens for events. Any AM which wishes to execute work in
other containers must request them off the RM, and, when allocated, create
a `ContainerLaunchContext` containing the command to execute, the
environment to execute the command, binaries to localize and all relevant
security credentials.

1. Even with the NM handling the localization process, the AM must still
be able to retrieve the security credentials supplied at launch time so
that it itself may work with HDFS and any other services, and to pass some or
all of these credentials down to the launched containers. 

### Acquiring and Adding tokens to a YARN Application

The delegation tokens which a YARN application needs must be acquired
from a program executing as an authenticated user. For a YARN application,
this means the user launching the application. It is the client-side part
of the YARN application which must do this

1. Log in via `UserGroupInformation`.
1. Identify all tokens which must be acquired.
1. Request the specific tokens desired.
1. Marshall all tokens into a byte buffer.
1. Add them to the `ContainerLaunchContext` within the `ApplicationSubmissionContext`.

Which tokens are required? By default: HDFS.

Applications talking to other services, such as Apache HBase and Apache Hive,
should request tokens from these services, using the libraries of these
services to acquire delegation tokens. All tokens can be added to the same
set of credentials, then saved to a byte buffer for submission.

### Extracting tokens within the AM

## Other Aspects of YARN Security

### AM/RM Token Refresh

The AM/RM token is renewed automatically; the AM pushes out a new token
to the AM within an `allocate` message. Consult the `AMRMClientImpl` class
to see the process.

### Token Renewal on AM Restart

Even if an application is renewing tokens regularly, if an AM fails and is
restarted, it gets restarted from that original
`ApplicationSubmissionContext`. The tokens there may have expired, so localization
may fail, even before the issue of credentials to talk to other services.

How is this problem addressed? The YARN Resource Manager handles it, by
renewing the tokens on behalf of the user.

### Timeline Server integration

The [Application Timeline Server](TimelineServer.html) can be deployed as a secure service
—in which case the application will need the relevant token to authenticate with
it. This process is handled automatically in `YarnClientImpl` if ATS is
enabled in a secure cluster. The AM-side `TimelineClient` client manages
token renewal automatically.

### Unmanaged Application Masters

Unmanaged application masters are not launched in a container set up by
the RM and NM, so cannot automatically pick up an AM/RM token at launch time.
The `YarnClient.getAMRMToken()` API permits an Unmanaged AM to request an AM/RM
token. Consult `UnmanagedAMLauncher` for the specifics.

### Identity on an insecure cluster: `HADOOP_USER_NAME`

In an insecure cluster, the application will run as the identity of 
the account of the node manager, typically something such as `yarn`
or `mapred`. By default, HDFS will therefore access the (insecure)
filesystem as that user, with a different home directory, and with
a different user identified in audit logs and on file system owner attributes.
 
This can be avoided by having the client identify the identify of the
HDFS/Hadoop user under which the application is expected to run. *This
does not affect the OS-level user or the applications access rights
to the local machine*.

When Kerberos is disabled, the identity of a user is picked up
by Hadoop first from the environment variable `HADOOP_USER_NAME`, 
then from the OS-level username (e.g. the system property `user.name`).

YARN applications should propagate the user name of the user launching
an applicatin by setting this environment variable.

```java
Map<String, String> env = new HashMap<>();
String userName = UserGroupInformation.getCurrentUser().getUserName();
env.put(UserGroupInformation.HADOOP_USER_NAME, userName);
containerLaunchContext.setEnvironment(env);
```

Note that this environment variable is picked up in all applications
which talk to HDFS via the hadoop libraries. That is, if set, it
is the identity picked up by HBase and other applications executed
within the environment of a YARN container within which this environment
variable is set.

### Oozie integration and `HADOOP_TOKEN_FILE_LOCATION`

Apache Oozie can launch an application in a secure cluster either by acquiring
all relevant credentials, saving them to a file in the local filesystem,
then setting the path to this file in the environment variable
`HADOOP_TOKEN_FILE_LOCATION`. This is of course the same variable name as that
passed down -by YARN in launched containers, as is similar content: a byte
array with credentials. 

Here, however, the environment variable is set in the environment
executing the YARN client. This client must use the token information saved
in the named file *instead of acquiring any tokens of its own*. 


## Securing Long-lived YARN Services

There is a time limit on all token renewals, after which tokens won't renew,
and the application will stop working. This is somewhere between 72h and 7 days.

Any YARN service intended to run for an extended period of time *must* have
a strategy for renewing credentials.

Here are the strategies:

### Pre-installed Keytabs for AM and containers

A keytab is provided for the application's use on every node.

This is done by:

1. Installing it in every cluster node's local filesystem
1. Providing the path to this in a configuration option.
1. The application loading the credentials via `UserGroupInformation.loginUserFromKeytab()`

The keytab must be in a secure directory path, where
only the service (and other trusted accounts) can read it. Distribution
becomes a responsibility of the cluster operations team.
 
This is effectively how all static Hadoop applications get their security credentials.

### Keytabs for AM and containers distributed via YARN


1. A keytab is uploaded to HDFS.
1. When launching the AM, the keytab is listed as a resource to localize to
the AM's container.
1. The Application Master is configured with the relative path to the keytab,
and logs in with `UserGroupInformation.loginUserFromKeytab()`.
1. When the AM launches the container, it list the HDFS path to the keytab
as a resource to localize.
1. It adds the HDFS delegation token to the container launch context, so
that the keytab and other application files can be localized.
1. Launched containers must themselves log in via `UserGroupInformation.loginUserFromKeytab()`.


This avoids the administration task of installing keytabs for specific services
across the entire cluster.

It does require the client to have access to the keytab
and, as it is uploaded to the distributed filesystem, must be secured through
the appropriate path permissions/ACLs.

As all containers have access to the keytab, all code executing in the containers
has to be trusted. Malicious code (or code escaping some form of sandbox)
could read the keytab, and hence have access to the cluster until the keys
expire or are revoked.

This is the strategy implemented by Apache Slider (incubating).

### AM keytab distributed via YARN, AM generated and renewed delegation
tokens for containers.

1. A keytab is uploaded to HDFS.
1. When launching the AM, the keytab is listed as a resource to localize to
the AM's container.
1. The Application Master is configured with the relative path to the keytab,
and logs in with `UserGroupInformation.loginUserFromKeytab()`.
1. When the AM launches a container, it acquires all the delegation tokens
needed by that container, and adds them to the container container launch context.
1. Launched containers must load the delegation tokens from `$HADOOP_TOKEN_FILE_LOCATION`,
and use them (including renewals) until they can no longer be renewed.
1. The AM must implement an IPC interface which permits containers to request
a new set of delegation tokens; this interface must itself use authentication
and ideally wire encryption.
1. Before a delegation token is due to expire, the processes running in the containers
must request new tokens from the Application Master, over the IPC channel.

This is the strategy used by Apache Spark 1.5+.

Because only the AM has direct access to the keytab, it is less exposed.
Code running in the containers only has access to the delegation tokens.

However, those containers will have access to HDFS from the tokens
passed in at container launch, so will have access to the copy of the keytab
used for launching the AM. While the AM could delete that keytab on launch,
doing so would stop YARN being able to successfully relaunch the AM after any
failure.

### Client-side push of renewed Delegation Tokens

This strategy may be the sole one acceptable to a strict operations team: a client process
running on an account holding a kerberos TGT negotiates with all needed cluster services
for delegation tokens, tokens which are then pushed out to the Application Master via
some RPC interface.

This does require the client process to be re-executed on a regular basis; a cron or Oozie job
can do this. The AM will need to implement an IPC API over which renewed
tokens can be provided.

### AM-initiated reboot

Because the RM refreshes tokens on AM restart, a YARN service configured
to preserve containers over AM restarts could just consider restarting the AM
every 24-48 hours, pick up the refreshed tokens and then (somehow) propagate
them to the containers.

There is no documented application which does this; we are aware that it is
possible.

## Securing YARN Application Web UIs and REST APIs

YARN provides a straightforward way of giving every YARN application SPNEGO authenticated
web pages: it implements SPNEGO authentication in the Resource Manager Proxy. 
YARN web UI are expected to load the AM proxy filter when setting up its web UI; this filter
will redirect all HTTP Requests coming from any host other than the RM Proxy hosts to an
RM proxy, to which the client app/browser must re-issue the request. The client will authenticate
against the principal of the RM Proxy (usually `yarn`), and, once authenticated, have its
request forwared.

As a result, all client interactions are SPNEGO-authenticated, without the YARN application
itself needing any kerberos principal for the clients to authenticate against.

Known weaknesses in this approach are

1. As calls coming from the proxy hosts are not redirected, any application running
on those hosts has unrestricted access to the YARN applications. This is why in a secure cluster
the proxy hosts must run on cluster nodes which do not run end user code (i.e. not run YARN
NodeManagers and hence schedule YARN containers, nor support logins by end users).
1. The HTTP requests between proxy and YARN RM Server are not encrypted.

## Securing YARN Application REST APIs

YARN REST APIs running on the same port as the registered web UI of a YARN application are
automatically authenticated via SPNEGO authentication in the RM proxy.
 
Any REST endpoint (and equally, any web UI) brought up on a different port does not
support SPNEGO authentication unless implemented in the YARN application itself.


## Checklist for YARN Applications

Here is the checklist of core actions which a YARN application must do
to successfully launch in a YARN cluster.

### Client

`[ ]` Client checks for security being enabled via `UserGroupInformation.isSecurityEnabled()`

In a secure cluster

`[ ]` Client checks for environment variable `HADOOP_TOKEN_FILE_LOCATION`. 
If set, loads the Hadoop credentials from it.

`[ ]` Client acquires NN delegation tokens for the local HDFS filesystems, as
needed for localization on AM launch.

`[ ]` Client acquires delegation tokens for all services to
be used in YARN application.

`[ ]` Client sets tokens on AM `ContainerLaunchContext.setTokens()`.


In an insecure cluster

`[ ]` Propagate local username to YARN AM, hence HDFS identity via the
`HADOOP_USER_NAME` environment variable.

### App Master

`[ ]` In a secure cluster, AM retrieves security tokens from `HADOOP_TOKEN_FILE_LOCATION`
environment variable,

`[ ]` AM/RM token is extracted to build a credential set without this token -for
container launch.

### Container Launch by AM

`[ ]` Tokens to be passed to containers are passed set via 
`ContainerLaunchContext.setTokens()`.


`[ ]` In an insecure cluster, propagate `HADOOP_USER_NAME` environment variable.

### Launched Containers

`[ ]` Call `UserGroupInformation.isSecurityEnabled()` to trigger security setup.

### YARN service

`[ ]` Application has a token renewal strategy: shared keytab, AM keytab or
client-side token refresh.

`[ ]` In a secure cluster, keytab is either already in HDFS (and checked for),
or it is in the local FS of the client, in which case it must be uploaded and added to
the list of resources to localize.

`[ ]` Client acquires HDFS delegation token for Container Launch Context.

`[ ]` AM logs in as principal in keytab via `loginUserFromKeytab()`.

`[ ]` AM extracts AM/RM token from `HADOOP_TOKEN_FILE_LOCATION` env var.

`[ ]` For launched containers, either Keytab is propagated, or AM acquires/attaches
all required delegation tokens to the Container Launch context alongside the
HDFS delegation token.

## Testing YARN applications in a secure cluster.

It is straightforward to be confident that a YARN application works in secure
cluster. The process to do so is: test on a secure cluster.

Even a single VM-cluster can be set up with security enabled. If doing so,
we recommend turning security up to its strictest, with SPNEGO-authenticated
Web UIs (and hence RM Proxy), as well as IPC wire encryption. Setting the
kerberos token expiry to under an hour will find kerberos expiry problems
early —so is also recommended.

`[ ]` Application launched in secure cluster.

`[ ]` Launched application runs as user submitting job (tip: log `user.name`
system property in AM).

`[ ]` Web browser interaction verified in secure cluster.

`[ ]` REST client interation (GET operations) tested.

`[ ]` Application continues to run after Kerberos Token expiry.

`[ ]` Application does not launch if user lacks Kerberos credentials.

`[ ]` If the application supports the timeline server, verify that it publishes
events in a secure cluster.

`[ ]` If the application integrates with other applications, such as HBase or Hive,
verify that the interaction works in a secure cluster.

`[ ]` If the application communicates with remote HDFS clusters, verify
that it can do so in a secure cluster (i.e. that the client extracted any
delegation tokens for this at launch time)

**Remember**: If you don't test your YARN application in a secure cluster,
it won't work.

