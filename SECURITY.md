<!-- SPDX-License-Identifier: Apache-2.0 -->

# Apache Hadoop Security Model

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL
NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and
"OPTIONAL" in this document are to be interpreted as described in
RFC 2119.

This document defines the security model of Apache Hadoop: the deployments it is
designed to protect, the boundaries it defends, and — equally importantly — the
things which are *not* vulnerabilities. It exists for human reporters and for
anyone using automated or AI-assisted tooling to look for security issues.

**TL;DR: Hadoop's security model defends a Kerberos-secured cluster running on a
trusted operating system, behind a network perimeter, with a valid site
configuration. Findings which only apply outside that model are bugs, not
vulnerabilities.**

## Before Filing a Report (Including AI-Assisted Reports)

The deployment Hadoop's security model defends is a **Kerberos-secured cluster**.
Many findings that look like vulnerabilities in other contexts are not
vulnerabilities here, because the surrounding deployment is trusted by design.

You *MUST NOT* file a security report for:

- Issues that require the operator to edit their own Hadoop site configuration,
  place malicious/vulnerabile libraries on their own classpath,
  or pass malicious arguments to their own command invocation.
- **Job submission running user-supplied code.** Submitting work to YARN or
  MapReduce executes the submitter's code as the submitter's identity. That is
  the product, not a vulnerability. See the threat model below.
- **Denial of service at scale.** A large Hadoop cluster exists to execute jobs
  at scale; such a cluster can itself be used to mount distributed attacks, and
  authenticated users can exhaust resources. Resource exhaustion and performance
  degradation from legitimate authenticated use are out of scope.
- Issues that require the attacker to already hold cluster or remote-store
  credentials, a valid Kerberos principal, or local disk access.
- Anything against the **default insecure (non-Kerberos) mode** — it is insecure
  by design (see the deployment model below).
- **Transitive CVEs** in dependencies Hadoop builds or ships against. See
  [Third Party Modules](#third-party-modules).
- Raw **scanner output** (Snyk, Dependabot, Trivy, Zizmor, etc.) without a
  reproducer against the current `trunk` branch.
- Theoretical findings ("an attacker who could X might then Y") without a
  reproduction.

A valid report includes:

- The Hadoop version, and ideally the git SHA it was reproduced against.
- The OS and version, Java version, and other environment information considered relevant.
- The exact steps, configuration, and commands used to reproduce it.
- The observed in-scope failure, and what was expected instead.
- Where a CVE/CVSS score is claimed, the reasoning behind that score.

### For Partly/Fully AI-Generated Reports

AI-assisted reports are accepted **only** if the submitter has verified the
finding by hand against current source and includes a runnable reproducer.

In addition, the submitter of an AI-generated report is

1. REQUIRED to understand what Hadoop is, to understand the claimed vulnerability,
and to be able to explain it in their own words — including justifying any claimed CVE or CVSS
scores. If the submitter is unable to do this, then any credit for a resulting
CVE will be assigned to the AI tool alone, and not to the submitter.

2. MUST declare the AI tool used, and be willing to provide the log.
   The log is a key part of AI tool reports, and we need to be able to track/replicate these.

*Unverified LLM-generated reports waste maintainer time and will be closed
without further response.* Repeat offenders may be banned for an extended period. 

## Reporting a Vulnerability

Report security vulnerabilities in Apache Hadoop privately to
**security@hadoop.apache.org**.

* Do not cc: any public mailing list.
* Do **not** open a public JIRA issue, GitHub
issue, or pull request for an unfixed vulnerability.

For vulnerabilities in CI pipelines, see
[Reporting Vulnerabilities in CI Pipelines](#reporting-vulnerabilities-in-ci-pipelines).

See the Apache Software Foundation's
[guidelines for reporting security issues](https://www.apache.org/security/) for
the responsible-disclosure process that applies to all ASF projects.

## Third Party Modules

### Reporting a Known CVE in a Hadoop Dependency

Do not report the existence of a published CVE in a Hadoop dependency
to the security list. These are published and do not need to be treated as
confidential.

These are considered improvements in the project, and are managed in
the project's [issue tracker](https://issues.apache.org/jira/issues/).
1. Search for any existing issue covering the dependency upgrade.
2. If it exists, read it, its discussion, the PRs etc, and see what versions
   it has been merged to.
3. If it hasn't been merged, look at why and get involved: major work is likely to be
   needed.
4. If there isn't an issue, create one and start work on the PR!

Tip: an easy way to check for the version of a library to ship in the trunk
release of hadoop is the [LICENSE-binary](./LICENSE-binary) file.

Please do not send an email listing the CVEs an automated scan
tool reported and requesting updates, timelines etc.
Open source development is a community process, and addressing this is done
in the [developer mailing lists](https://hadoop.apache.org/mailing_lists.html).
Join the community to help get your needs addressed.

If you cannot find existing information on whether the project is affected by the issue in the advisory,
it may be up to you, as a part of the project community, to participate in its handling.
Ensure you provide detailed information when starting a discussion - review how the project uses the dependency and have your opinion on the priority to upgrade,
or even remove, the dependency.
Contributions upgrading the dependency to a version that is not affected by the problem are generally welcomed, though will not typically expedite the release schedule.

Actively participating in the release process, especially qualifying pre-release artifacts in your
own deployments, is the most effective way of accelerating the release timetable.

### Providing Advance Warning of a Critical CVE in a Hadoop Dependency

If a team providing a library which Hadoop bundles has a critical CVE which
a forthcoming fix will correct, they are encouraged to notify the hadoop security
list so we can identify whether the project is exposed, help review and validate fixes and
co-ordinate releases.

We SHALL treat all such reports as confidential.

### Reporting a Newly-Discovered Vulnerability in a Third-Party Module

Security bugs in third-party modules (the JVM, the Kerberos infrastructure, cloud
SDKs, connectors, or any other dependency) should be reported to their respective
maintainers, through their own security-reporting mechanisms — after verifying
the issue is in scope of *their* threat model and reproduces against *their*
current release.

## Supported Versions

Security fixes are made only to the most recent Apache Hadoop release line(s).
Older release lines are end-of-life and do not receive security updates; the
remedy for a vulnerability in an old line is to upgrade. Refer to the
[Apache Hadoop release and download policy](https://hadoop.apache.org/releases.html)
for which lines are currently maintained. A report MUST be reproducible against a
maintained release or the current `trunk` branch.

## The Hadoop Threat Model

In the Hadoop threat model there are **trusted elements**. Vulnerabilities that
require the compromise of these trusted elements are outside the scope of the
model:

- **Cluster Administrators are trusted.**
- **DNS is trusted.**
- **The Kerberos authentication infrastructure is trusted.** Active Directory,
  FreeIPA, or whichever other Key Distribution Center (KDC) is in use is trusted
  and required to be well-configured — including synchronized clocks (NTP/chrony)
  across the KDC, services, and clients, within the Kerberos clock-skew window.
  Authentication failures caused by clock drift are operational bugs, not
  vulnerabilities.
- **The network perimeter is trusted to keep the public internet out, but the
  wire is not assumed confidential.** The perimeter does not authenticate callers —
  Kerberos authentication does that at the service level; the perimeter's job is to
  keep the cluster off the public internet (Hadoop clusters are never web-facing).
  Within that, Hadoop may run with optional wire encryption (RPC `privacy` QOP;
  HDFS block-transfer encryption). Running without encryption is by design and not
  a vulnerability; but when encryption is enabled, a failure to actually protect
  traffic — no-op encryption, silent downgrade, or MITM bypass — is in scope.
- **Any hosting cloud or infrastructure provider is trusted, as is the
  underlying hardware.** This includes the CPU, memory, storage, and network
  hardware, even on shared/multi-tenant cloud systems where that hardware is
  physically shared with other tenants. Attacks that require malicious or
  compromised hardware, hypervisor escape, or cross-tenant side channels
  (speculative-execution, Rowhammer, and similar) are the responsibility of the
  hardware and infrastructure provider, and are out of scope.
- **The underlying operating system is trusted.** Hadoop relies on OS process
  isolation, file permissions, and (where required) OS-level disk encryption.
  An attack that first requires the OS to be compromised or misconfigured is out
  of scope.
- **Valid site configuration is trusted.** We expect `core-site.xml`,
  `hdfs-site.xml`, `yarn-site.xml` and the rest of the site configuration to be
  valid and to be writable only by trusted administrators. If an attacker can
  manipulate the site configuration, the game is already over — that is out of
  scope.
- **The classpath is trusted.** We expect no malicious JAR files to be on the classpath.
  If an attacker can add a malicious JAR to the classpath, then it is the ability to add the
  JAR to a process launched by a higher-privilege which is the exploit.
  Launching a process as the current principal with a malicious JAR is not an exploit,
  nor is any attack which makes the ability to manipulate the classpath a pre-requisite.

Within that model, the boundary Hadoop **defends** is **privilege escalation
across an authenticated boundary within a Kerberos-secured cluster**.
Examples of in-scope issues are:

- A user acting as another user, as a service, or as a superuser without the
  authorization to do so.
- Bypassing service-level authorization / ACLs
  (see [Service Level Authorization](hadoop-common-project/hadoop-common/src/site/markdown/ServiceLevelAuth.md)).
- Forging, leaking, or improperly reusing delegation tokens.
- Defeating the constraints on proxy/superuser impersonation
  (see [Proxy user - Superusers Acting On Behalf Of Other Users](hadoop-common-project/hadoop-common/src/site/markdown/Superusers.md)).
- Failure of clients to detect and reject malicious service endpoints/MITM attacks.

Further properties of the model:

- **Hadoop clusters are never web-facing.** They are deployed behind a network
  perimeter; network rules are expected to keep the cluster off the public
  internet. The perimeter does not authenticate callers — Kerberos does that at
  the service level. A report which assumes a cluster is directly exposed to the
  public internet is not in scope.
- **Wire encryption is optional and controlled by site configuration.** Network
  traffic between Hadoop components may or may not be encrypted, depending on the
  deployment's configuration. The absence of wire encryption when it has not been
  enabled is not a vulnerability.

Relevant operational security documentation:

- [Hadoop in Secure Mode](hadoop-common-project/hadoop-common/src/site/markdown/SecureMode.md)
- [Service Level Authorization](hadoop-common-project/hadoop-common/src/site/markdown/ServiceLevelAuth.md)
- [Authentication for Hadoop HTTP web-consoles](hadoop-common-project/hadoop-common/src/site/markdown/HttpAuthentication.md)
- [Proxy user - Superusers](hadoop-common-project/hadoop-common/src/site/markdown/Superusers.md)
- [Credential Provider API](hadoop-common-project/hadoop-common/src/site/markdown/CredentialProviderAPI.md)
- [YARN Application Security](hadoop-yarn-project/hadoop-yarn/hadoop-yarn-site/src/site/markdown/YarnApplicationSecurity.md)
- [Transparent Encryption in HDFS](hadoop-hdfs-project/hadoop-hdfs/src/site/markdown/TransparentEncryption.md)

## Deployment Threat Model

Hadoop is deployed in a number of ways, with different security boundaries.

### Standalone (Insecure) Mode

In its standalone configuration Hadoop performs no real authentication. Anyone with
network access to the cluster has full access to its data and can submit work.

This mode is *intended* to run only on a trusted, network-isolated host or
network. It is insecure **by design**. "The unsecured cluster has no security" is
not a vulnerability, and arbitrary data access against a non-Kerberos cluster is
inherent in security being disabled.

It should only be used for standalone development/test environments, with firewalls preventing remote access.
It can then be used to test Hadoop and applications running on top of it.

### Secure (Kerberos) Clusters

This is the deployment the security model defends, as described in
[The Hadoop threat model](#the-hadoop-threat-model) above: Kerberos
authentication, service-level authorization, delegation tokens, and constrained
proxy/superuser impersonation.

It is the expected deployment of production physical clusters.
1. A trusted Kerberos system is used to authenticate principals.
2. Services have been issued with credentials (keytabs) in files, secured on the physical hosts via OS file permissions.
3. Users of the cluster all authenticate with the Kerberos system for their access.
4. Access to the cluster may be via a proxy mechanism.
5. The HDFS filesystem uses Kerberos to authenticate HDFS nodes and services themselves, other cluster services (YARN, Apache ZooKeeper etc) and callers.
6. HDFS block tokens are issued by the HDFS Name Node to grant data access to authenticated principals;
   the possessor of a token may access a block of data on a data node with the permissions in that token,
   without the need to supply any further authentication information.

Hadoop services issue _delegation tokens_: an authenticated principal obtains a token directly from a service such as HDFS, Apache HBase, Apache Hive, Apache Knox and more.
YARN distributes these tokens to an application's containers and renews them on the application's behalf, so tasks can authenticate to those services without holding Kerberos credentials themselves.
These tokens have an independent life from the Kerberos credentials
* They have a limited lifespan of a number of hours.
* They can be cancelled: the issuing service MUST then reject requests using them as authentication.
* They can be renewed: before their lifespan expires the renewer requests the issuing service to extend their lifespan.

The details of these tokens or how issuing, cancellation and renewal are managed are not covered in this document.
Hadoop and applications MUST safely marshall and store these tokens; if they are published in any form then permissions are being leaked.

### Transient Cloud Deployments

Hadoop is frequently deployed as a transient cluster in a cloud environment:

- Cloud credentials are supplied to the deployment by the hosting infrastructure
  — for example AWS IAM roles attached to the VMs/containers, or equivalent
  mechanisms on other clouds. **These supplied credentials, and the access they
  grant, are the trust boundary.** Using credentials provided to the VM or
  container the code runs in is not a vulnerability.
- The cluster is **transient** and typically single-tenant: it is created for a
  workload and destroyed afterwards.
- **Network rules prevent access by untrusted principals.** As with on-premises
  clusters, the deployment is not web-facing; the network perimeter is part of
  the model.

Hadoop clusters MUST NOT be deployed in cloud without network rules to isolate them from the public internet.

### Client-only Deployments

The hadoop client libraries can be used as part of an application communicating with remote
kerberos-authenticated services or to cloud infrastructure:

- No service endpoints are created.
- HTTP, HTTPS and IPC connections are set up to communicate with remote services.
- Credentials are stored on the client, either for direct authentication,
  or via authentication services such as Kerberos or OAuth 2, services which provide shorter-lived credentials.
- The shorter-lived credentials are used for communication with the remote services themselves.

In client-side use, the following is trusted
- The underlying operating system and its configuration.
- The principal and the host administrator are trusted.
- The application classpath.
- The client-side configuration and CLI arguments.

Whether or not the network is trusted to the extent that DNS is trusted and network encryption is mandatory for HDFS, cluster service and cloud service communication
along with TLS where appropriate, is a matter for client configuration and out of scope of this security model.

## Data at Rest and Temporary Files

- **Persisting data in the cluster filesystem encrypted requires HDFS encryption** (see
  [Transparent Encryption in HDFS](hadoop-hdfs-project/hadoop-hdfs/src/site/markdown/TransparentEncryption.md)).
  Where encryption has been configured, a failure of the code to actually
  encrypt the persisted data **is a vulnerability** and should be reported.
- **Temporary data** is written to local-filesystem temporary directories. The
  requirement is that the operating system secures and, where required, encrypts
  these directories — this is part of the trusted-OS assumption. Within that:
  - Code that creates temporary files and directories **MUST create them and set
    their permissions atomically.** Creating a file or directory with permissive
    defaults and then narrowing the permissions in a later step leaves a window
    in which another local principal can act on it.
  - A failure to create files/directories and set their permissions atomically
    **is an issue** and should be reported.

## Logging

Logs are expected to be kept private.

That is, users and administrators are _expected_ not to share them with untrusted entities.
This is consistent with the [Log4j Security Model](https://logging.apache.org/security.html),
which is the logging framework Hadoop uses.

Services MUST restrict access to logs.
- Non-administrator principals MUST NOT be able to access logs of applications launched by other principals.
- Non-administrator principals MUST NOT be able to access logs of services.

Preventing log overflow attacks is a matter of configuring logging, and out of scope.
Default/example log configurations SHOULD define a rollover policy which limits the size of log files created.

Limiting log size may permit failed attempts to authenticate with a service to be logged.
Log4J does support log aggregation across systems; services can be configured to feed selective logs
to central services.

Services SHOULD assist central security logging by providing specific logs for reporting authentication
failures/privilege rejections.

### Secrets and Logging

Leaking secrets into logs is [CWE-532: Insertion of Sensitive Information into
Log File](https://cwe.mitre.org/data/definitions/532.html).
The following rules apply to Hadoop code:

- Secrets *SHOULD NOT* be logged.
- **Persistent secrets, long-lived credentials, and encryption secrets (keys,
  key material, passwords) *MUST NOT* be logged at any level.**
- **Transient secrets** (for example short-lived tokens) *MUST NOT* be logged at
  `INFO`, `WARN`, or `ERROR` level, and *SHOULD NOT* be logged at `DEBUG` or
  `TRACE` level.

Transient secrets are called out specifically because secrets sometimes surface in HTTP/web
request logs (URLs, headers, query parameters) and are visible
when third-party components including JDK classes are configured to log at TRACE.
Preventing logging of these is best-effort.

## Development and CI Threat Model

The project is built on developer systems and in CI systems, and **we do care
about attacks on these.**
Development and CI are explicitly in scope.

- We trust the `javac` compiler and other tools from Oracle.
- We trust the Linux [docker images](dev-support/docker) we use to build our releases,
  including `gcc`, `gpg` and other applications installed.
- We trust our build tools, especially Apache Maven.
- We trust other ASF projects, and for their manual release-vote process
  to provide a buffer against package-ecosystem worms. We do not require
  a cool-down period after a release of an ASF artifact before upgrading our dependency.

See [Important Security Information for GitHub Actions](.github/workflow-security.md)
for the detailed CI/workflow security guidance. In summary:

- All inputs from external pull requests — titles, comments, author metadata, and
  code — *SHALL* be considered untrusted, and *MUST NOT* be fed directly or
  indirectly to shell commands without sanitization.
- Upstream dependencies from non-ASF projects *MAY* be subverted by supply-chain
  attacks; a cooldown period *MUST* be observed before adopting a new or
  updated dependency.
- Maven plugins, and third-party libraries that production code compiles against,
  execute code on developer and CI systems (the latter during testing). Their
  security *MUST* be evaluated before adoption.
- IDE trust mechanisms are sensitive: for example a
  [VS Code trusted workspace](https://code.visualstudio.com/docs/editing/workspaces/workspace-trust)
  allows files in the tree (`.env`, `tasks.json`, etc.) to declare executables.
  PRs that add such code-execution mechanisms *SHALL* be rejected.
- **CI build output is publicly visible.** Unobfuscated logging of any cloud
  credentials or other secrets provided to CI runs is therefore in scope.
- GitHub Actions hardening:
  - Actions *MUST* be pinned by commit SHA (with the version as a comment so
    Dependabot can track them), not by tag.
  - PR triggers *MUST NOT* be ones which grant unrestricted GitHub tokens to the
    workflow — there *MUST NOT* be `pull_request_target`, `workflow_run`, or
    `issue_comment` triggers used in that way.
  - Workflows *SHALL* follow GitHub's
    [secure use](https://docs.github.com/en/actions/reference/security/secure-use)
    guidelines, in particular using intermediate environment variables to safely
    process untrusted input, and *SHALL* be audited with [Zizmor](https://zizmor.sh/).

Test resource files such as `auth-keys.xml` (and any equivalent under a
module's test tree) may contain live secrets. They *MUST NOT* be read out, printed,
or committed to version control.

### Reporting Vulnerabilities in CI Pipelines

Reporters MUST e-mail security@infra.apache.org and only CC security@hadoop.apache.org.

If the threat is credible, the ASF infrastructure team will disable the affected workflow and notify the project.

## Not in the Threat Model

The following are explicitly **not** vulnerabilities:

- Any attack which accesses arbitrary data against an HDFS cluster for which
  Kerberos is not enabled.
- Any attack against cloud storage where the credentials are supplied by the
  VM/container within which Hadoop is executing.
- **Job submission executing user-supplied code** — this is the core of YARN
  and MapReduce.
- Any attack which requires the user to manually enter credentials or
  configuration options.
- Any attack which requires the operating system to be misconfigured or
  configured in a non-standard way as a step in the process.
- Any attack which requires a Hadoop site reconfiguration.
- Any attack which requires the download and execution of external binaries.
- Any attack which makes an assumption about artifacts on the classpath that is
  not true in production deployments.
- Vulnerabilities in dependencies (transitive CVEs) without a reproducer against
  the current `trunk` branch.
- Any attack which grants more privileges accessing remote storage or HDFS than
  the client already holds through its credentials and/or Kerberos principal.
- **Denial of service or resource exhaustion at scale** from authenticated use.
- Performance regressions — file these as normal bugs.
- Any vulnerability which cannot be reproduced on the latest maintained release,
  or on a build of the `trunk` branch.
- Bugs in test runs which do not leak secrets — file these as normal bugs.

## Scanner Calibration Rules

A scanner (or AI tool) SHOULD treat a finding as higher-confidence only if it
plausibly shows one of the following:

- Exposure of a secret or delegated credential to a new audience.
- The creation of a new unauthorized capability in a component owned by this
  project.
- The existence of the issue on `HEAD` of the `trunk` branch.

A finding SHOULD be downgraded or rejected by default if it instead depends
primarily on:

- Malformed-input robustness or denial-of-service behaviour.
- A malicious external service, catalog, or metastore.
- A principal who already has equivalent power through legitimate
  authentication, write, or maintenance capabilities.
- A vulnerability that only exists in previous releases.

## Special Topics

### Client-Side Resilience to Malicious Services

Clients are expected to be correctly configured to talk to correct service endpoints, including any web proxies.
Lack of resilience to malformed responses SHALL be considered bugs, not CVEs.
This applies to cloud storage services and FTP and HTTP URLs used as filesystem paths.

If a malicious remote service can trigger RCE on a client, this SHALL be considered a CVE.

### Compressors

Do report "decompression bombs" as bugs rather than security issues, as they generally lead to service
disruption, especially that of the specific worker thread reading untrusted data.

### Concurrency

There's a lot of concurrency in the Hadoop codebase; it can often be a source of bugs.
If a race condition leads to privilege escalation or other exploit within the security model,
it is a CVE.

### Configuration: `org.apache.hadoop.conf.Configuration`

#### Restricted Configurations

The class `org.apache.hadoop.conf.Configuration` has the option to restrict system properties;
it can be set/unset on an instance by a call to `setRestrictSystemProperties(boolean)`.
When true, all references to system properties or environment variables within a configuration string
MUST NOT resolve.
There is a static method to change the default on new instances
```java
public static void setRestrictSystemPropertiesDefault(boolean val);
```
After a call `Configuration.setRestrictSystemPropertiesDefault(true)`, all new `Configuration()` instances
SHALL be restricted by default, unless and until `setRestrictSystemPropertiesDefault(false)` is invoked.

This is used in some services to restrict configuration loading for specific jobs/users from accessing
information about the deployed system.

If system properties or environment variables are resolvable via `${}` and `${env.}` references within
a restricted `Configuration` instance, this is a security vulnerability.

#### Passwords in Configurations

It is possible to store secrets, such as cloud credentials, in Hadoop XML configuration files.
If this is explicitly done, it SHALL NOT be considered a vulnerability.

The correct way to store such secrets is through a JCEKS file or other credentials provider service.
Application code reading in secrets from configurations MUST use `Configuration.getPassword()` to ensure
these can be used as a store of secrets.


### `hadoop-client` Libraries

The `hadoop-client-api` and `hadoop-client-runtime` artifacts (and the aggregate
`hadoop-client`/`hadoop-client-minicluster` modules under `hadoop-client-modules/`) are
**shaded, relocated aggregates** of many third-party dependencies. The classes they bundle —
and the embedded copies of those libraries' metadata — belong to those upstream projects, not
to Hadoop itself.

As a consequence:

- If a scanner reports a CVE "in `hadoop-client-runtime`" (or another `hadoop-client-*`
  artifact), the finding is almost always against a **third-party artifact** that has been
  shaded into it, not against Hadoop code. Such findings are handled as third-party
  dependencies — see [Third Party Modules](#third-party-modules) — and the remedy is a
  dependency upgrade in a Hadoop release, not a Hadoop CVE.
- These artifacts exist for **client-side** use. **Server-side vulnerabilities** in bundled
  dependencies such as Netty, Apache HttpClient, or a Thrift IPC protocol implementation are
  **not direct issues for the `hadoop-client` libraries**, because the vulnerable server-side
  code paths are not the ones a Hadoop client exercises. A reproducer MUST show the issue being
  reachable through Hadoop client usage on the current `trunk` branch.

### IPC

Hadoop supports an IPC protocol with multiple transport options.
It is used by Hadoop and applications which run on it, including Apache HBase and Apache Tez.

Hadoop's current RPC uses protobuf 3.x, shaded as `org.apache.hadoop.thirdparty.protobuf`
and shipped via `hadoop-thirdparty`.
To provide wire-compatibility with old releases, the legacy
`org.apache.hadoop.ipc.ProtobufRpcEngine` still compiles against the protobuf 2.5.0 API
(`com.google.protobuf`); that 2.5.0 artifact is `provided`-scope and **not packaged** with
Hadoop, so a deployment needing the legacy engine must supply it.
A scanner flagging protobuf 2.5.0 is therefore flagging an un-shipped dependency.

Hadoop services expose IPC endpoints for remote access.

In a secure cluster IPC endpoints SHALL support authentication/authorization using Kerberos and MAY
support service-specific delegation tokens.
If they support delegation tokens they SHOULD support token cancellation and SHOULD support token renewal.

In a secure cluster access to service endpoints SHALL be restricted to callers by an ACL site configuration option or similar mechanism.
If the service is an application launched as a YARN application, the authentication SHOULD be restricted
to the principal who submitted the job. An ACL configuration option MAY provide extra access.

As IPC endpoints are not exposed to the internet, the security model does not need to be resilient
to the full scope of attacks a public-facing endpoint may experience, especially resilience to DoS/DDoS
attacks.
The key threats to defend against are:
- Unauthorized access: log and reject.
- Privilege escalation: log and reject.
- Service vulnerability to failures from malformed messages.
  Logging and rejection of malformed messages is considered the correct handling of such messages.

Client applications communicate with IPC endpoints as configured or through explicit user configurations, such
as on the CLI.
There is an implicit trust boundary.
Clients SHOULD be resilient to malformed responses from service endpoints; a failure to do so SHALL be considered a bug, not a security issue.

### Native Code

Anything which can trigger failures in Hadoop's own native code in supported operating systems SHALL be
considered for CVEs.
This includes
- pointer, memory and stack issues.
- OS calls
- Process launch and cross-process IPC (especially native OS mechanisms).
- JNI binding
As per the policy on third-party libraries, issues with native libraries such as libssl are out of scope unless it is actually how hadoop invokes the native code which is at fault.

### Resource Exhaustion

Anything which consumes resources such as memory, threads, file handles or disk storage, SHALL be considered bugs.

There is a special case: rapid resource exhaustion which can be triggered on HDFS and YARN services
by remote requests from non-administrative callers.
Where a single low-cost request forces a disproportionate resource cost on a service and so degrades
it for other users (an amplification attack), this SHALL be considered a vulnerability rather than a
bug, and reported as such — even though general resilience to denial-of-service is not part of the
threat model.

### Test Code

Test code is expected to be executed in controlled environments.
- Dedicated developer systems.
- CI containers/VMs.

All reports of vulnerabilities in test code SHALL be downgraded to bugs or treated as BY-DESIGN/
WONTFIX issues except when secrets are logged or if it is possible for malicious code to be executed in CI/CD workflows.

### TLS/SSL

Hadoop services support TLS of service hosts; certificate management is out of scope.

Validation of TLS certificates is in scope, including verifying the hostname matches the certificate, along with the entire certificate chain.

### Writable: `org.apache.hadoop.io.Writable`

`Writable` is very similar to `java.io.Serializable`, but unlike Java's built-in serialization mechanism,
marshalling and unmarshalling is explicitly performed in the interface's implementation.

LLMs often conflate the two and consider their use an immediate vulnerability.
For any CVE related to Writable to be accepted, the submitter MUST show a valid exploit chain using
extant gadgets in the classpath of Hadoop or the latest versions of major applications built on top
of it, from the entry point to the outcome.

Implementing new `Writable` classes as part of an exploit *does not count*.

### Tokens 

Hadoop Tokens are used for authenticating IPC channels; these use Writables to marshall data, and
are are unmarshalled before callers are authenticated.

It is *critical* that implementations of `org.apache.hadoop.security.token.TokenIdentifier` in
production code and declared in a metadata resource file
`hadoop-tools/hadoop-aws/src/main/resources/META-INF/services/org.apache.hadoop.security.token.TokenIdentifier`
are resilient to attack by malicious clients, such that denial-of-service attacks are possible or authentication can be bypassed/spoofed.

### Java Serialization Attacks in Downstream Applications

Hadoop IPC does *not* use Java Serialization; it uses `Writable` and protobuf.

Some applications and services which include the libraries do use Java Serialization.

We do care if any `org.apache.hadoop` class that is not a shaded third-party class can be used
as a "gadget" in an attack on Java object deserialization.

Please notify the security team if you discover such a vulnerability even if it is not part of
our own threat model.
