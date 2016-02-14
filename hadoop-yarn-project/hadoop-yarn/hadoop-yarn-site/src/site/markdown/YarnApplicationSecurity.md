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

    * A list of files in the cluster's filesystem to be "localized".
    * The environment variables to set in the container.
    * The commands to execute in the container to start the application.
    * Any security credentials needed by YARN to launch the application.
    * Any security credentials needed by the application to interact
    with any Hadoop cluster services and applications.

1. Have an Application Master which, when launched, registers with
the YARN RM and listens for events. Any AM which wishes to execute work in
other containers must request them off the RM, and, when allocated, create
a `ContainerLaunchContext` containing the command to execute, the
environment to execute the command, binaries to localize and all relevant
security credentials.

1. Even with the NM handling the localization process, the AM must itself
be able to retrieve the security credentials supplied at launch time so
that it itself may work with HDFS and any other services, and to pass some or
all of these credentials down to the launched containers.

### Acquiring and Adding tokens to a YARN Application

The delegation tokens which a YARN application needs must be acquired
from a program executing as an authenticated user. For a YARN application,
this means the user launching the application. It is the client-side part
of the YARN application which must do this:

1. Log in via `UserGroupInformation`.
1. Identify all tokens which must be acquired.
1. Request these tokens from the specific Hadoop services.
1. Marshall all tokens into a byte buffer.
1. Add them to the `ContainerLaunchContext` within the `ApplicationSubmissionContext`.

Which tokens are required? Normally, at least a token to access HDFS.

An application must request a delegation token from every filesystem with
which it intends to interact —including the cluster's main FS.
`FileSystem.addDelegationTokens(renewer, credentials)` can be used to collect these;
it is a no-op on those filesystems which do not issue tokens (including
non-kerberized HDFS clusters).

Applications talking to other services, such as Apache HBase and Apache Hive,
must request tokens from these services, using the libraries of these
services to acquire delegation tokens. All tokens can be added to the same
set of credentials, then saved to a byte buffer for submission.

The Application Timeline Server also needs a delegation token. This is handled
automatically on AM launch.

### Extracting tokens within the AM

When the Application Master is launched and any of the UGI/Hadoop operations
which trigger a user login invoked, the UGI class will automatically load in all tokens
saved in the file named by the environment variable `HADOOP_TOKEN_FILE_LOCATION`.

This happens on an insecure cluster along with a secure one, and on a secure
cluster even if a keytab is used by the application. Why? Because the
AM/RM token needed to authenticate the application with the YARN RM is always
supplied this way.

This means you have a relative similar workflow across secure and insecure clusters.

1. Suring AM startup, log in to Kerberos.
A call to `UserGroupInformation.isSecurityEnabled()` will trigger this operation.

1. Enumerate the current user's credentials, through a call of
`UserGroupInformation.getCurrentUser().getCredentials()`.

1. Filter out the AMRM token, resulting in a new set of credentials. In an
insecure cluster, the list of credentials will now be empty; in a secure cluster
they will contain

1. Set the credentials of all containers to be launched to this (possibly empty)
list of credentials.

1. If the filtered list of tokens to renew, is non-empty start up a thread
to renew them.

### Token Renewal

Tokens *expire*: they have a limited lifespan. An application wishing to
use a token past this expiry date must *renew* the token before the token
expires.

Hadoop automatically sets up a delegation token renewal thread when needed,
the `DelegationTokenRenewer`.

It is the responsibility of the application to renew all tokens other
than the AMRM and timeline tokens.

Here are the different strategies

1. Don't. Rely on the lifespan of the application being so short that token
renewal is not needed. For applications whose life can always be measured
in minutes or tens of minutes, this is a viable strategy.

1. Start a background thread/Executor to renew the tokens at a regular interval.
This what most YARN applications do.

## Other Aspects of YARN Security


### AM/RM Token Refresh

The AM/RM token is renewed automatically; the AM pushes out a new token
to the AM within an `allocate` message. Consult the `AMRMClientImpl` class
to see the process. *Your AM code does not need to worry about this process*

### Token Renewal on AM Restart

Even if an application is renewing tokens regularly, if an AM fails and is
restarted, it gets restarted from that original
`ApplicationSubmissionContext`. The tokens there may have expired, so localization
may fail, even before the issue of credentials to talk to other services.

How is this problem addressed? The YARN Resource Manager gets a new token
for the node managers, if needed.

More precisely

1. The token passed by the RM to the NM for localization is refreshed/updated as needed.
1. Tokens in the app launch context for use by the application are *not* refreshed.
That is, if it has an out of date HDFS token —that token is not renewed. This
also holds for tokens for for Hive, HBase, etc.
1. Therefore, to survive AM restart after token expiry, your AM has to get the
NMs to localize the keytab or make no HDFS accesses until (somehow) a new token has been passed to them from a client.

This is primarily an issue for long-lived services (see below).

### Unmanaged Application Masters

Unmanaged application masters are not launched in a container set up by
the RM and NM, so cannot automatically pick up an AM/RM token at launch time.
The `YarnClient.getAMRMToken()` API permits an Unmanaged AM to request an AM/RM
token. Consult `UnmanagedAMLauncher` for the specifics.

### Identity on an insecure cluster: `HADOOP_USER_NAME`

In an insecure cluster, the application will run as the identity of
the account of the node manager, typically something such as `yarn`
or `mapred`. By default, the application will access HDFS
as that user, with a different home directory, and with
a different user identified in audit logs and on file system owner attributes.

This can be avoided by having the client identify the identify of the
HDFS/Hadoop user under which the application is expected to run. *This
does not affect the OS-level user or the application's access rights
to the local machine*.

When Kerberos is disabled, the identity of a user is picked up
by Hadoop first from the environment variable `HADOOP_USER_NAME`,
then from the OS-level username (e.g. the system property `user.name`).

YARN applications should propagate the user name of the user launching
an application by setting this environment variable.

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
`HADOOP_TOKEN_FILE_LOCATION`. This is of course the same environment variable
passed down by YARN in launched containers, as is similar content: a byte
array with credentials.

Here, however, the environment variable is set in the environment
executing the YARN client. This client must use the token information saved
in the named file *instead of acquiring any tokens of its own*.

Loading in the token file is automatic: UGI does it during user login.

The client is then responsible for passing the same credentials into the
AM launch context. This can be done simply by passing down the current
credentials.

```java
credentials = new Credentials(
    UserGroupInformation.getCurrentUser().getCredentials());
```

### Timeline Server integration

The [Application Timeline Server](TimelineServer.html) can be deployed as a secure service
—in which case the application will need the relevant token to authenticate with
it. This process is handled automatically in `YarnClientImpl` if ATS is
enabled in a secure cluster. Similarly, the AM-side `TimelineClient` YARN service
class manages token renewal automatically via the ATS's SPNEGO-authenticated REST API.

If you need to prepare a set of delegation tokens for a YARN application launch
via Oozie, this can be done via the timeline client API.

```java
try(TimelineClient timelineClient = TimelineClient.createTimelineClient()) {
  timelineClient.init(conf);
  timelineClient.start();
  Token<TimelineDelegationTokenIdentifier> token =
      timelineClient.getDelegationToken(rmprincipal));
  credentials.addToken(token.getService(), token);
}
```

### Cancelling Tokens

Applications *may* wish to cancel tokens they hold when terminating their AM.
This ensures that the tokens are no-longer valid.

This is not mandatory, and as a clean shutdown of a YARN application cannot
be guaranteed, it is not possible to guarantee that the tokens will always
be during application termination. However, it does reduce the window of
vulnerability to stolen tokens.

## Securing Long-lived YARN Services

There is a time limit on all token renewals, after which tokens won't renew,
causing the application to stop working. This is somewhere between seventy-two
hours and seven days.

Any YARN service intended to run for an extended period of time *must* have
a strategy for renewing credentials.

Here are the strategies:

### Pre-installed Keytabs for AM and containers

A keytab is provided for the application's use on every node.

This is done by:

1. Installing it in every cluster node's local filesystem.
1. Providing the path to this in a configuration option.
1. The application loading the credentials via
  `UserGroupInformation.loginUserFromKeytab()`.

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

1. When the AM launches the container, it lists the HDFS path to the keytab
as a resource to localize.

1. It adds the HDFS delegation token to the container launch context, so
that the keytab and other application files can be localized.

1. Launched containers must themselves log in via
  `UserGroupInformation.loginUserFromKeytab()`. UGI handles the login, and
  schedules a background thread to relogin the user periodically.

1. Token creation is handled automatically in the Hadoop IPC and REST APIs,
the containers stay logged in via kerberos for their entire duration.

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

### AM keytab distributed via YARN; AM regenerates delegation tokens for containers.

1. A keytab is uploaded to HDFS by the client.

1. When launching the AM, the keytab is listed as a resource to localize to
the AM's container.

1. The Application Master is configured with the relative path to the keytab,
and logs in with `UserGroupInformation.loginUserFromKeytab()`. The UGI
codepath will still automatically load the file references by
`$HADOOP_TOKEN_FILE_LOCATION`, which is how the AMRM token is picked up.

1. When the AM launches a container, it acquires all the delegation tokens
needed by that container, and adds them to the container's container launch context.

1. Launched containers must load the delegation tokens from `$HADOOP_TOKEN_FILE_LOCATION`,
and use them (including renewals) until they can no longer be renewed.

1. The AM must implement an IPC interface which permits containers to request
a new set of delegation tokens; this interface must itself use authentication
and ideally wire encryption.

1. Before a delegation token is due to expire, the processes running in the containers
must request new tokens from the Application Master over the IPC channel.

1. When the containers need the new tokens, the AM, logged in with a keytab,
 asks the various cluster services for new tokens.

(Note there is an alternative direction for refresh operations: from AM
 to the containers, again over whatever IPC channel is implemented between
 AM and containers). The rest of the algorithm: AM regenerated tokens passed
 to containers over IPC.

This is the strategy used by Apache Spark 1.5+, with a netty-based protocol
between containers and the AM for token updates.

Because only the AM has direct access to the keytab, it is less exposed.
Code running in the containers only has access to the delegation tokens.

However, those containers will have access to HDFS from the tokens
passed in at container launch, so will have access to the copy of the keytab
used for launching the AM. While the AM could delete that keytab on launch,
doing so would stop YARN being able to successfully relaunch the AM after any
failure.

### Client-side Token Push

This strategy may be the sole one acceptable to a strict operations team: a client process
running on an account holding a Kerberos TGT negotiates with all needed cluster services
for new delegation tokens, tokens which are then pushed out to the Application Master via
some RPC interface.

This does require the client process to be re-executed on a regular basis; a cron or Oozie job
can do this. The AM will need to implement an IPC API over which renewed
tokens can be provided. (Note that as Oozie can collect the tokens itself,
all the updater application needs to do whenever executed is set up an IPC
connection with the AM and pass up the current user's credentials).

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

Known weaknesses in this approach are:

1. As calls coming from the proxy hosts are not redirected, any application running
on those hosts has unrestricted access to the YARN applications. This is why in a secure cluster
the proxy hosts *must* run on cluster nodes which do not run end user code (i.e. not run YARN
NodeManagers and hence schedule YARN containers, nor support logins by end users).

1. The HTTP requests between proxy and YARN RM Server are not currently encrypted.
That is: HTTPS is not supported.

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

In a secure cluster:

`[ ]` If `HADOOP_TOKEN_FILE_LOCATION` is unset, client acquires delegation tokens
 for the local filesystems, with the RM principal set as the renewer.

`[ ]` If `HADOOP_TOKEN_FILE_LOCATION` is unset, client acquires delegation tokens
for all other services to be used in the YARN application.

`[ ]` If `HADOOP_TOKEN_FILE_LOCATION` is set, client uses the current user's credentials
as the source of all tokens to be added to the container launch context.

`[ ]` Client sets all tokens on AM `ContainerLaunchContext.setTokens()`.

`[ ]` Recommended: if it is set in the client's environment,
client sets the environment variable `HADOOP_JAAS_DEBUG=true`
in the Container launch context of the AM.

In an insecure cluster:

`[ ]` Propagate local username to YARN AM, hence HDFS identity via the
`HADOOP_USER_NAME` environment variable.

### App Master

`[ ]` In a secure cluster, AM retrieves security tokens from `HADOOP_TOKEN_FILE_LOCATION`
environment variable (automatically done by UGI).

`[ ]` A copy the token set is filtered to remove the AM/RM token and any timeline
token.

`[ ]` A thread or executor is started to renew threads on a regular basis.

`[ ]` Recommended: AM cancels tokens when application completes.

### Container Launch by AM

`[ ]` Tokens to be passed to containers are passed via
`ContainerLaunchContext.setTokens()`.

`[ ]` In an insecure cluster, propagate the `HADOOP_USER_NAME` environment variable.

`[ ]` Recommended: AM sets the environment variable `HADOOP_JAAS_DEBUG=true`
in the Container launch context if it is set in the AM's environment.

### Launched Containers

`[ ]` Call `UserGroupInformation.isSecurityEnabled()` to trigger security setup.

`[ ]` A thread or executor is started to renew threads on a regular basis.

### YARN service

`[ ]` Application developers have chosen and implemented a token renewal strategy:
shared keytab, AM keytab or client-side token refresh.

`[ ]` In a secure cluster, the keytab is either already in HDFS (and checked for),
or it is in the local FS of the client, in which case it must be uploaded and added to
the list of resources to localize.

`[ ]` If stored in HDFS, keytab permissions should be checked. If the keytab
is readable by principals other than the current user, warn,
and consider actually failing the launch (similar to the normal `ssh` application.)

`[ ]` Client acquires HDFS delegation token and and attaches to the AM Container
Launch Context,

`[ ]` AM logs in as principal in keytab via `loginUserFromKeytab()`.

`[ ]` (AM extracts AM/RM token from the `HADOOP_TOKEN_FILE_LOCATION` environment
variable).

`[ ]` For launched containers, either the keytab is propagated, or
the AM acquires/attaches all required delegation tokens to the Container Launch
context alongside the HDFS delegation token needed by the NMs.

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

## Important

*If you don't test your YARN application in a secure Hadoop cluster,
it won't work.*

And without those tests: *your users will be the ones to find out
that your application doesn't work in a secure cluster.*

Bear that in mind when considering how much development effort to put into
Kerberos support.
