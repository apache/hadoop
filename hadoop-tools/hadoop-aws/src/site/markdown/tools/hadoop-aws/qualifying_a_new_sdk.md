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

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL
NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and
"OPTIONAL" in this document are to be interpreted as described in
RFC 2119.

# Qualifying an AWS SDK


> The AWS SDKs and CLI are designed for usage with official AWS services.
> We may introduce and enable new features by default, such as these new default integrity protections,
> prior to them being supported or otherwise handled by third-party service implementations.

That is a quote from an
[announcement of a somewhat incompatible change](https://github.com/aws/aws-sdk-java-v2/discussions/5802)
which shipped in v2.30.0 of the AWS SDK.

It highlights the SDK team's point of view: their job is to work on the SDK to support AWS's own services.
Compatibility with third-party services is not their problem, and they do not test against such stores.

This makes sense from their perspective: if someone implements their own S3 store, then it is
their task to make it compatible with AWS S3, even as that is a moving target with no public
formal API specification.

The S3A connector is one of the most popular of S3 connectors used to connect
JVM-hosted big-data applications to AWS S3 *and to other S3-compatible stores*.
We do not have the luxury of saying "third-party stores are not our problem", so have to make
sure that our release works with all stores.

And because of that broad adoption, we need to make sure that it works in
different deployment scenarios, with different configurations even within AWS.

The task of qualifying an AWS SDK is a lot more than just incrementing a number in a maven POM file:
it is determining whether the SDK is safe to adopt, and, if safe,
identifying and making any changes in our code that are needed to migrate.


## Introduction

The S3A connector is utterly dependent upon the AWS SDK; even a minor change can have serious consequences.
That is: changing a single number in a maven file can bring new features and needed bug fixes.
It can also cause a lot of damage, albeit unintentionally.

Some example regressions encountered previously include:
* The SDK printing a warning message telling developers off every time a specific object in the SDK is instantiated
  This breaks all tests which look for specific output strings and runs a risk of generating support calls asking "why is my application telling me off?"
* A change in the semantics of calling `abort()` on a stream.
  This was a valid design decision — however, it was unexpected.
  And again the warning message printed every time the stream was closed prematurely flooded application logs.
* Instabilities in the shading of third-party libraries (slf4j, etc.), with consequences such as the inability to enable any form of logging.
* The shaded library continuing to declare dependencies which are redundant due to the shading.

Third-party store support can also be trouble as it is not something tested by the AWS SDK team themselves (why would they?). This means our code may be one of the first contact points between an update of the SDK and third-party stores.

The core semantics of the S3A/SDK integration can be reasonably well tested simply by running the S3A integration test suite with all the optional features covered:
* KMS encryption
* Versioned bucket support
* AWS access points
* STS session tokens
* Third-party storage


The challenge when qualifying an SDK is to make sure that the following condition holds:

> After an upgrade to the SDK, it is still possible to read and write data to all classes of AWS S3 store,
> and all third-party stores which were previously supported.
> This condition must hold for the command line and downstream applications; the hadoop-aws
> test suites are necessary but not sufficient.

From the outset, assume that there is a regression and that your challenge is to find it.
That is, rather than the qualification being a process of "run some automated and manual tests to show that all is well",
the task has to be approached as one of "find out what has broken, where and why".
Then we can worry about how to fix.

The test process then:

1. Run the usual integration test with as many of the optional features covered.
Do not simply verify that everything appears to have worked:
you must also look through all the log output to make sure there are no new warning messages being printed
indicating a mismatch between how the S3A code is using the library and the library expects to be used.
2. Build a binary release and test through the command line.
3. Build and test as many downstream applications as you can.

> What happens if a regression does surface and the qualification process that did not find it
> - and now the SDK upgrade has been applied?

We revert. Immediately. Then the process for identifying and trying to remedy the issue surfaces.
If the library has already shipped, then this is harder.
Here as well as identifying the root cause we need to assess what is the impact of this in production deployment.
We may need to issue a new Hadoop release.
This is time-consuming, painful, and needless hard work. This is why it is so important that we get it right.

> What happens if I absolutely need a new feature in the latest SDK?

Congratulations! You have just taken on the task of qualifying the SDK release!


## Stop! Is This a Last-Minute Action Before a Release?

If so: _it is too late_.

We need at least two weeks of stabilization to see if other developers
encounter problems related to their own set-ups: endpoints, networks,
credentials - as well as applications built on top of it.

If it is for a feature: postpone the release, or don't do the update.
Is it for a critical fix? Postpone the release.

Either way: we need that time to find things before shipping.

## Choosing an SDK

Use an SDK which has been out for two or more weeks.

* If we do need a specific release for a fix: go with that one or later.
* If it is a feature we need, always try for a slightly later build.

Features always take time to stabilize, so let others find the problems and
AWS engineers the solutions.

1. Look at [announcements](https://github.com/aws/aws-sdk-java-v2/discussions/categories/announcements)
   to see if there is a recent announcement related to S3 or core authentication.
2. Look at [V2 SDK issues](https://github.com/aws/aws-sdk-java-v2/issues) to see what
   recently reported issues are which may cause problems.
   Look at the discussion, and if it is relevant, subscribe.
   Consider also examining our code to see if there is any actual exposure.
   Do not just look at the open issues: look at all recent issues as there may be
   recently closed bugs whose fixes must be picked up; this
   search identifies them.

## Test Setup

To be confident the upgraded SDK works in many deployment configurations, we
need to validate it with as many of the different configuration options and
store types we can.

This is done through a combination of different S3 implementations,
and by having a complex test matrix of different configurations
for a very small set of S3 test buckets in AWS and elsewhere

### Test Buckets

Submitter MUST have the following buckets:
* `B1`:
 - S3 standard
 - SSE-KMS at bucket level
 - SHOULD S3 server logging to B2.
* `B2`:
 - S3 standard
 - MUST: versioned (with versions configured to delete after 7 days)
 - MUST: configured with path style access.
 - MUST: configured to buffer writes into an array.
   (Note, one `testMultiObjectDeleteLargeNumKeys` may OOM here, don't worry about it)
 - SHOULD have an access point defined.
   In this document`B2AP` is the bucket configuration
   to access it via the AP.
* `B3`: S3 Express
 - MAY: Using CSE-KMS
* `B4`: S3 standard  (i.e. if you test in us-west-2, put this in us-east-1)
 - S3 standard
 - us-east-1
 - long-long distance link to the test system.
   If you are testing remotely, this is implicit.
   If you are testing within AWS infrastructure, it MUST be a different region.
* `B5`:  third-party store.
  - SHOULD: Use Google GCS as documented in [third-party stores](./third_party_stores.html).
  - MAY: Any other third-party store you can access. Ideally one with bulk delete support, which
 Google's S3 endpoint lacks. This is needed to verify regressions related to MD-5 signing
 of bulk deletes haven't failed.

Testing with at least one third-party store is critical, as is an S3 Express store.
Ideally, test with multiple third-party stores.


| Id     | Class             | Config                                                    |
|--------|-------------------|-----------------------------------------------------------|
| `B1`   | S3 standard       | SSE-KMS; Has S3 server logging to B2                      |
| `B2`   | S3 standard       | Path style access, versioned, MUST BE same region as B1.  |
| `B2AP` | Access Point      | Access Point to B2 (TLS 1.3+ only)                        |
| `B3`   | S3 Express        | Default configurations                                    |
| `B4`   | S3 standard       | Long haul link in US and access point access              |
| `B5`   | Third-party store | Google GCS or other third-party store                     |

These are the core storage class/configurations which are used in production,
hence are part of the qualification process.

One of the buckets B1-B4 MUST be in a region for which there is a FIPS endpoint,
so that it can be configured to use it for access.
That bucket must therefore be within a US region.

A third-party store *MUST* be tested.

*Note* in the XML below, replace `B1`, `B2`, etc. with the names of your test buckets.

```xml
<property>
  <name>fs.s3a.bucket.B4.endpoint.fips</name>
  <value>true</value>
</property>
```

All buckets which support lifecycle policies SHOULD be set to abort all pending uploads
after 24h, delete all files after 7d.

### Test Host

We also need two test hosts to validate behaviour in the two key scenarios: in AWS and outside of it.

You can start with a single host, but you will need to validate the behaviour of the SDK in both scenarios.

Within AWS
1. EC2/kerberos deployment outside us-east-1 and within a VPC whose network rules can be configured to not allow access to us-east-1.
   The build can be done without that rule (needed for the artifact download), but a test run must be one locked down. This is to validate local region resolution.
2. On a remote host, with any config for the AWS CLI (temporarily) renamed from `~/.aws/config` to something else.
   This is needed to make sure the SDK isn't reading region/endpoint info from that file, as
   it can do - and which can therefore accidentally hide regressions.
   Note: renaming your config file before running CLI testing may be enough for this.


### Configuration and Extra Services

Submitter MUST have the extra AWS setup for:
* KMS encryption
* Assumed role for session token tests.
* An access point.

This may seem a lot of preparation but it is needed for full test coverage.

These configuration SHOULD go into an XInclude-able configuration file which can be referenced absolutely,
for example in a directory `~/config/auth-keys.xml`, which can be
referenced both from hadoop-aws tests, and in full distributions you have
built up.

Ideally, both `hadoop-tools/hadoop-aws/src/test/resources/auth-keys.xml`
and `etc/hadoop/core-site.xml` will look identical

```xml
<configuration>
  <include xmlns="http://www.w3.org/2001/XInclude"
    href="///users/alice/config/auth-keys.xml">
  </include>
</configuration>
```

Keeping these out of the hadoop source tree used to be to avoid accidentally committing secrets.
It is now critical as a way to stop AI tools scanning the files and including the secrets
when it generates code for the project.

#### Recommendations

Initialize that `~/config/` directory as a *local* github repo, it is easier to
see what you've broken.
Obviously you MUST NOT push it to any remote repo if it contains
your AWS secrets.

Have a separate XInclude file for the test-related settings for each endpoint, to
make switching between them easier.

### Test Configuration Files

Base test environment setup
This MUST define the test name in the properties
`test.fs.s3a.name` and `fs.contract.test.fs.s3a`.

When testing different buckets the option `test.fs.s3a.name` will need to be
set to the name of the specific bucket.

```xml

<configuration>
  <property>
    <name>test.fs.s3a.name</name>
    <value>B1</value>
  </property>

  <property>
    <name>fs.contract.test.fs.s3a</name>
    <value>${test.fs.s3a.name}</value>
  </property>

  <property>
    <name>fs.s3a.access.key</name>
    <value>${YOUR_ACCESS_KEY}</value>
  </property>

  <property>
    <name>fs.s3a.secret.key</name>
    <value>${YOUR_SECRET_KEY}</value>
  </property>

  <property>
    <name>fs.s3a.scale.test.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>fs.iostatistics.logging.level</name>
    <value>info</value>
  </property>

</configuration>
```
*Tip* You can create a set of properties for `test.fs.s3a.name`, one for each bucket,
and comment out all but the active one.
Unfortunately, S3 Express bucket names cannot be used within XML comments as the `--` sequence is forbidden.

It is simpler to prefix inactive entries with an X.

#### B1

Test bucket `B1` with configurations as below.
This ensures:
* Assumed role enabled - required for `ITestAssumeRole` tests
* Encryption set to SSE-KMS
* Scale tests enabled
* Contract tests enabled
* No region set - ensures region resolution works

```xml

<configuration>
  <property>
    <name>test.fs.s3a.name</name>
    <value>${B1}</value>
  </property>

  <property>
    <name>fs.s3a.scale.test.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>fs.s3a.assumed.role.arn</name>
    <value>${ROLE_ARN}</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B1.assumed.role.external.id</name>
    <value>test-id</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B1.assumed.role.sts.endpoint</name>
    <value>${STS_ENDPOINT}</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B1.assumed.role.sts.endpoint.region</name>
    <value>${REGION}</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B1.encryption.algorithm</name>
    <value>SSE-KMS</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B1.encryption.key</name>
    <value>${KMS_KEY}</value>
  </property>

</configuration>
```
#### B2

Test bucket `B2` with configuration as below.
This ensures:
* Analytics stream is used for all reading
* Path style access is enabled
* Versioned bucket with change detection by version id
* Scale tests enabled
* (Write) Rate limiting

```xml

<configuration>
  <property>
    <name>test.fs.s3a.name</name>
    <value>B2</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B2.endpoint.region</name>
    <value>${B2_REGION}</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B2.path.style.access</name>
    <value>true</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B2.input.stream.type</name>
    <value>analytics</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B2.accesspoint.arn</name>
    <value>${ACCESS_POINT_ARN}</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B2.accesspoint.required</name>
    <value>true</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B2.change.detection.source</name>
    <value>versionid</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B2.fast.upload.buffer</name>
    <value>array</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B2.multipart.size</name>
    <value>32M</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B2.multipart.threshold</name>
    <value>${fs.s3a.bucket.B2.multipart.size}</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B2.io.rate.limit</name>
    <value>1000</value>
  </property>
</configuration>
```

#### B3. S3 Express

Configure your S3 Express bucket with configurations as below:

```xml

<configuration>
  <property>
    <name>test.fs.s3a.name</name>
    <value>B3</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B3.endpoint.region</name>
    <value>${B3_REGION}</value>
  </property>

  <property>
    <name>test.fs.s3a.create.storage.class.enabled</name>
    <value>false</value>
  </property>
</configuration>
```

#### B4. Long Haul, FIPS, [Object Lock]

Test your long-haul bucket $B4, with configuration as below. This ensures:
* CSE-KMS is enabled
* FIPS enabled, assuming your long haul bucket is within a US region.
* This would be a good setup to test Object Lock with.

```xml

<configuration>

  <property>
    <name>fs.s3a.bucket.B4.endpoint.region</name>
    <value>${B4_REGION}</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B4.encryption.key</name>
    <value>${ENCRYPTION_KEY_ARN}</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B4.encryption.algorithm</name>
    <value>CSE-KMS</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B4.encryption.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B4.endpoint.fips</name>
    <value>true</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B4.create.checksum.algorithm</name>
    <value>CRC32C</value>
  </property>

</configuration>
```
#### B5. Third-Party Bucket

Test bucket `B5` with a third-party store.

Use whatever settings are needed to connect to the store.

```xml

<configuration>
  <!-- === *** switches for third-party tests *** === -->
  <property>
    <name>test.fs.s3a.encryption.enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>test.fs.s3a.create.storage.class.enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>test.fs.s3a.create.acl.enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>test.fs.s3a.list.v1.enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>fs.s3a.scale.test.csvfile</name>
    <value>
    </value>
    <description>file used in scale tests</description>
  </property>

  <property>
    <name>test.fs.s3a.sts.enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>test.fs.s3a.content.encoding.enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>test.fs.s3a.performance.enabled</name>
    <value>false</value>
    <description>Stores with stricter-than-S3 semantics fail the performance-mode tests.</description>
  </property>

  <property>
    <name>fs.s3a.ext.test.multipart.commit.consumes.upload.id</name>
    <value>true</value>
    <description>Set if a completed MPU commit consumes the upload ID so it is no
    longer listable and abort reports NoSuchUploadException.</description>
  </property>

  <property>
    <name>test.fs.s3a.requester.pays.file</name>
    <value></value>
  </property>

  <property>
    <name>fs.s3a.bucket.B5.endpoint.region</name>
    <value>${B5_REGION}</value>
    <description>Make something up, do not use "ec2", "auto", or "sdk"</description>
  </property>

  <property>
    <name>fs.s3a.bucket.B5.connection.expect.continue</name>
    <value>false</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B5.multipart.uploads.enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B5.committer.magic.enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>fs.s3a.bucket.B5.checksum.calculation.enabled</name>
    <value>false</value>
    <description>Calculate and attach a message checksum on every operation. (default: true)</description>
  </property>

  <property>
    <name>test.fs.s3a.create.storage.class.enabled</name>
    <value>false</value>
  </property>

</configuration>

```

#### Testing Open SSL

On any test system other than an ARM-based MacBook, use openssl for one of the buckets other than B1.
An EC2 x86 instance is ideal for this.

```xml
<property>
  <name>fs.s3a.bucket.B2.ssl.channel.mode</name>
  <value>openssl</value>
</property>
```

As `wildfly.jar` doesn't include the ARM64 native libraries - just skip it there.
See [wildfly-openssl](https://github.com/wildfly-security/wildfly-openssl/tree/main) for details.


#### Set Up the Env Vars B1 to B5 as the Names of the Buckets


```bash
export BUCKETNAME=example-bucket-name
export BUCKET=s3a://$BUCKETNAME

export B1=s3a://bucket-1
export B2=s3a://bucket-2

# needs a bucket config to match
export B2AP=s3a://bucket-2-access-point

...etc
```


## Preflight: Before You Upgrade

### Create a "Notes" Document to Track Your Work, Including Timing Information From Test Runs

### Seek Help From Others

Getting others to help in the qualification process can help in multiple ways:
1. Splits the work testing the upgrade, which can be done with different buckets.
2. Different development setups helps identify different deployment issues, such
   as in-AWS versus out-AWS clients, where bandwidth and latency are very different.


### Create Two JIRAs: One for the POM, One for Any Code Changes

First, create the upgrade JIRA.

Use the title "S3A: Upgrade AWS V2 SDK"; once a specific version has
been selected it must be renamed to that version.

In the JIRA include references to any AWS issues you have identified which can
require code changes.

Then create a "Test failures while qualifying AWS SDK upgrade"

This is where test failures can be listed,
and against which a commit can be created fixing
those which are minor ones due to failure to remove per-bucket settings
(most common) or some other failure which is simply related to assertions
which are only now surfacing.

All stack traces MUST be attached as comments here.
This may include real failures, which will need to be addressed
separately, if more than single-line fixes of production code.

### Create the PR Branch

1. Check out `trunk`
2. Create a new branch for the update.
3. Get ready to upgrade!

### Is Everything Currently OK?


Kick off the initial build with that chosen SDK release with the target bucket you use for normal building and testing.

```bash
# whole project
mvn -T 1C

# hadoop-aws
mvn -T 1C integration-test -Dmaven.plugin.validation=none -Dparallel-tests -DtestsThreadCount=9 -Dscale --pl hadoop-tools/hadoop-aws
```

If it compiles and the tests work, this is the first good sign.

### Do a Full Hadoop Release Build

```sh
mvn clean package -Pdist -DskipTests -Dmaven.javadoc.skip=true -DskipShade
```

Move this to a path outside the hadoop source tree.

```sh
mkdir ../Releases
mv hadoop-dist/target/hadoop-3.6.0-SNAPSHOT/ ../Releases/before-update
```

Copy into its `etc/hadoop` dir the `core-site.xml` config referencing your
separate `auth-keys.xml` file.

This is now your reference "before the upgrade" release build.
If you see what may be a regression during manual qualification,
you can try with this release to see if the problem holds there
too.
If it does: file a bug report independent of the qualification
JIRA, and cross-link with a "testing discovered" relation


### Create a Full Release for Manual Testing

Create a release binary
```bash
mvn -T 1C clean package -Pdist -DskipTests -Dmaven.javadoc.skip=true
```

move it somewhere.

```bash
mv hadoop-dist/target/hadoop-3.6.0-SNAPSHOT/ ../Releases/preflight
```

### Check Out the AWS SDK

Get the AWS SDK from  [github/aws/aws-sdk-java-v2](https://github.com/aws/aws-sdk-java-v2),
into a new directory.
For example, using the github `gh` cli:

```bash
mkdir ~/External
cd ~/External
gh repo clone aws/aws-sdk-java-v2
cd aws-sdk-java-v2
```

This will add a new directory `~/External/aws-sdk-java-v2` with the repository.
If you have already got this directory or an equivalent, update it.

Look in the [tag list](https://github.com/aws/aws-sdk-java-v2/tags) for the tag of the release and check this out.
For release 2.30.27, the command would be

```sh
git checkout tag/2.30.27
```

This is for identifying what has changed in this release,
including what has changed near code which is now failing in tests,
as well as how major changes are affecting classes we use.
Creating a new project in your IDE can assist here.

## Qualification

Allocate a whole week for this, _including preparing your test buckets and other storage details_

This is not just for the overhead of the test setup, and execution,
it assumes that there will be regressions and they will need fixing and retesting.


### Clean Up All Buckets

From your preflight release, clean out the buckets.
```sh
bin/hadoop fs -rm -r $B1/\*
bin/hadoop fs -rm -r $B2/\*
bin/hadoop fs -rm -r $B3/\*
bin/hadoop fs -rm -r $B4/\*
bin/hadoop fs -rm -r $B5/\*
```
This helps verify that every test bucket is well-configured.

### Update the SDK in Your Local Branch

In a new branch off trunk

Update the value of `aws-java-sdk-v2.version` in `hadoop-project/pom.xml` to the new SDK version.
```
<aws-java-sdk-v2.version>2.30.27</aws-java-sdk-v2.version>
```
In `LICENSE-binary` update the line declaring the version of the `bundle.jar` artifact included
in full distributions.

For example:
```
software.amazon.awssdk:bundle:2.30.27
```

### Do a Clean Build and Create the PR if All Is Good

If it compiles:
1. Commit the change, including the version number in the title
2. Push to github
3. Create a PR - don't include the version there yet.

After this, leave Yetus to do its work.

As you continue your work, place test results and stack traces into the PR, making it visible to all.
Anyone who is collaborating should do the same.

### Do a Clean Build and Test With Your Normal Bucket

In `hadoop-aws` directory
1. Run `mvn verify`
2. Run the `ILoadTest*` load tests from your IDE or via maven through
   `mvn verify -Dtest=skip -Dit.test=ILoadTest\* -Dscale`
   Look for regressions in performance as much as failures.
3. Create the site with `mvn site -DskipTests`; look in `target/site` for the report.
4. Review *every single `-output.txt` file in `hadoop-tools/hadoop-aws/target/failsafe-reports`,
  paying particular attention to
  `org.apache.hadoop.fs.s3a.scale.ITestS3AInputStreamPerformance-output.txt`,
  as that is where changes in stream close/abort logic will surface.

*Important*: reviewing the output may seem needless work but it has been where
problems logged by the SDK have been found.
If these are only found later, even though they were in the logs, it will be a sign
that you didn't do enough due diligence - your credibility will decrease.


### Testing All the Buckets

Run the `ITests` against the other buckets.
This is the most time consuming parts of the process,
~20 minutes for each run and the setup time, assuming they actually work.

What's the best order?
* Start with your normal development bucket, as changes in behavior will be more obvious there.
* S3 Express, as it is different enough and under-tested in normal development cycles.
* Proceed to the third-party store, as that is the most likely to have problems.
* After that: whatever is most convenient.

## Manual, Exploratory Testing


It is critical to manually go through the CLI to see if there have been changes there
which cause problems, especially whether new log messages have surfaced,
whether some packaging change breaks the CLI, or whether odd performance problems surface.

It would be straightforward to automate a sequence of commands,
but we do not want to because actually having you use the command
line from a terminal window is part of the qualification process, as you may identify issues
which the automated tests might not notice.

* Does it work?
* Does it suddenly pause for long periods of time?
* Are AWS SDK libraries printing warning messages? hadoop-aws code?
* Has some other change in the code base, unrelated to the SDK, started printing new warning messages or stopping things from working?

These are things we need to know before end users find out.

The commands below list the _minimum_ set of commands to run; any more you can think of will be wonderful.

In fact, an ideal outcome of qualifying an upgrade is that you have some new commands to add to this list.

In particular, we could benefit from a lot more fault injection to see how well the SDK recovers from problems.
This is often hard to test because S3 has such great reliability and because we
developers working with cloud storage have fast and reliable networks.
In production enough requests are made to S3 through our code every day that
many applications will actually encounter transient failures of the S3 end points,
all of which need to be recovered from.

Furthermore, people running this code are often doing it remotely, including through proxy, which
adds a whole new way for things to fail.

It is always interesting when doing the CLI testing to enable IOStatistics reporting:
```xml
<property>
  <name>fs.iostatistics.logging.level</name>
  <value>info</value>
</property>
```

From the root of the project, create a command line release

```sh
mvn package -Pdist -DskipTests -Dmaven.javadoc.skip=true -DskipShade
```

1. Change into the `hadoop-dist/target/hadoop-x.y.z-SNAPSHOT` dir.
2. Prepare and then copy a `core-site.xml` file into `etc/hadoop`.
   This file SHALL reference the same XInclude settings as used on the module testings,
   so as to ensure the CLI tests are working with the same options.
3. Set the `HADOOP_OPTIONAL_TOOLS` env var on the command line or `~/.hadoop-env`.

```bash
export HADOOP_OPTIONAL_TOOLS="hadoop-aws"
```
### Build Cloudstore

The cloudstore diagnostics and utilities tool is used in the CLI qualification.

1. Check out https://github.com/apache/hadoop-cloudstore/
2. Build it against the new SDK:

   ```bash
   mvn clean package -Dhadoop.version=3.6.0-SNAPSHOT
   ```

3. Set the `CLOUDSTORE` env var to point to the JAR created.


### CLI Commands

Now run some basic hadoop CLI operations.
The list here is derived from the list of [hadoop filesystem commands](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html),
with some extras from cloudstore.

Most are just file create/delete/upload/rename work, just to see if everything
is good and scales well.
Some are actually based on bugs seen in the wild, such as confusion about
trailing / signs,

They MUST be run against all buckets, as different behaviors may surface.
Running them against different stores will make you more alert to changes.
Changing the environment variables should suffice.

Consider any new logged message an error.
If it comes from a changed part of hadoop itself, other than hadoop-aws and hadoop-common
track the cause down and file a related JIRA.
Those aren't necessarily blockers, but as not enough people run manual CLI commands before the release phase
you may be the first person to notice it.

If it is from `hadoop-common` or `hadoop-aws` then it may be a regression in these
libraries.

Example [HADOOP-19514. SecretManager logs at INFO in bin/hadoop calls](https://issues.apache.org/jira/browse/HADOOP-19514).

Many of these tests check for success/failure.
Checking for this status is easy when your shell prompt flags any non-zero result,
such as through [fish prompt](https://fishshell.com/docs/current/prompt.html).
Otherwise you need to `echo $?`(bash/zsh) or `echo $status` (fish) to see the outcome.

```bash

export BUCKETNAME=$B1
export BUCKET=s3a://$BUCKETNAME

# fish equivalents
# set -gx BUCKETNAME example-bucket-name
# set -gx BUCKET s3a://$BUCKETNAME

# verify this is a valid URL
echo $BUCKET

bin/hadoop s3guard bucket-info $BUCKET

# should be empty; if there are some they are left over from test failures
#
# What the output looks like when there are leftovers:
#
# Listing uploads under path ""
# dir ABPnzm4LxSpC5K-A9MJ7ncqKyqEOnGbtDZ0enFNpeiUGcIR3B56s1wx00ZvUtCpSP9oajGBe
# dir ABPnzm6zBHlLoohyBkVmHcqif6jY-ydTbdCG1bfreX57uF7exxwHINLFLF-WWHWfxhxIFHx1
# Total 2 uploads found.
# ---
# Note: the command succeeds even if the multipart uploads are disabled.

bin/hadoop s3guard uploads $BUCKET

# repeat twice, once with "no" and once with "yes" as responses
# if there were any uploads, they must be reported as deleted

bin/hadoop s3guard uploads -abort $BUCKET

# MUST be empty
bin/hadoop s3guard uploads $BUCKET

# cloudstore diagnostics and IO tests
bin/hadoop jar $CLOUDSTORE storediag -w $BUCKET

# ---------------------------------------------------
# root filesystem operations
# ---------------------------------------------------

#
bin/hadoop fs -ls $BUCKET/

# expect: No such file or directory
bin/hadoop fs -ls $BUCKET/file


# exit code of 0 even when path doesn't exist
bin/hadoop fs -rm -R -f $BUCKET/dir-no-trailing
bin/hadoop fs -rm -R -f $BUCKET/dir-trailing/

# expect "Is a directory"
bin/hadoop fs -rm $BUCKET/

# expect success
bin/hadoop fs -touchz $BUCKET/file

# error "Is a directory"
bin/hadoop fs -touchz $BUCKET

# error: S3A: Cannot delete the root directory; will print Input/output error
bin/hadoop fs -rm -r $BUCKET/

# succeeds
bin/hadoop fs -rm -r $BUCKET/\*

# ---------------------------------------------------
# File operations
# ---------------------------------------------------

bin/hadoop fs -mkdir $BUCKET/dir-no-trailing
bin/hadoop fs -mkdir $BUCKET/dir-trailing/
bin/hadoop fs -touchz $BUCKET/file

# expect the two directories and the file
bin/hadoop fs -ls $BUCKET/

# expect success
bin/hadoop fs -mv $BUCKET/file $BUCKET/file2

# expect  File exists
bin/hadoop fs -mv $BUCKET/file $BUCKET/file2

# expect "No such file or directory"
bin/hadoop fs -stat $BUCKET/file

# expect success and a timestamp to be printed
bin/hadoop fs -stat $BUCKET/file2

# not an error to repeat this.
bin/hadoop fs -touchz $BUCKET/file2

# expect No such file or directory
bin/hadoop fs -mv $BUCKET/file $BUCKET/file3

# expect success
bin/hadoop fs -mv $BUCKET/file2 $BUCKET/dir-no-trailing

# expect success and a timestamp to be printed
bin/hadoop fs -stat $BUCKET/dir-no-trailing/file2

# same timestamp is printed
bin/hadoop fs -stat $BUCKET/dir-no-trailing/file2/

# lists the file -the timestamp matches that from the previous command
bin/hadoop fs -ls $BUCKET/dir-no-trailing/file2/
bin/hadoop fs -ls $BUCKET/dir-no-trailing

# repeated stat calls: expect the timestamp to increase on the invocations
# because directories don't really exist
bin/hadoop fs -stat $BUCKET/dir-no-trailing
bin/hadoop fs -stat $BUCKET/dir-no-trailing
bin/hadoop fs -stat $BUCKET/dir-no-trailing

# expect success
bin/hadoop fs -test -d $BUCKET/dir-no-trailing

# expect failure
bin/hadoop fs -test -d $BUCKET/dir-no-trailing/file2

# will return NONE unless bucket has checksums enabled. If it does, an etag is printed
bin/hadoop fs -checksum $BUCKET/dir-no-trailing/file2

# expect "etag" + a long string except on a store without etags
bin/hadoop fs -D fs.s3a.etag.checksum.enabled=true -checksum $BUCKET/dir-no-trailing/file2

# expect NONE
bin/hadoop fs -D fs.s3a.etag.checksum.enabled=false -checksum $BUCKET/dir-no-trailing/file2

# expect success
bin/hadoop fs -expunge -immediate -fs $BUCKET

# ---------------------------------------------------
# Delegation Token support
# ---------------------------------------------------

# failure unless delegation tokens are enabled
# ERROR: Failed to fetch token from ...
bin/hdfs fetchdt --webservice $BUCKET secrets.bin

# success, even on third-party stores
# expect "Created S3A Delegation Token: Kind: S3ADelegationToken/Full"
bin/hdfs fetchdt -D fs.s3a.delegation.token.binding=org.apache.hadoop.fs.s3a.auth.delegation.FullCredentialsTokenBinding --webservice $BUCKET secrets.bin

# prints "Token (S3ATokenIdentifier{S3ADelegationToken/Full"...
bin/hdfs fetchdt -print secrets.bin

# expect: WARN  token.Token (Token.java:getRenewer(478)) - No TokenRenewer defined for token kind S3ADelegationToken/Full
bin/hdfs fetchdt -renew secrets.bin

# ---------------------------------------------------
# Copy to/from local of a 500+ MB file.
# this is much larger than the usual hadoop-aws test file sizes
# if any time out it may be a sign of timeout settings too low,
# which, if these are the default values, is a problem.
# ---------------------------------------------------

bin/hadoop fs -mkdir $BUCKET/uploads

# expect successful upload of a file which can be found
bin/hadoop fs -stat share/hadoop/tools/lib/*bundle*jar
time bin/hadoop fs -copyFromLocal -t 10  share/hadoop/tools/lib/*bundle*jar $BUCKET/uploads


# expect bundle.jar to be listed
# expect the iostatistics object_list_request value to be O(directories)
bin/hadoop fs -ls -R $BUCKET/uploads

# expect this size to be over 600 MB
bin/hadoop fs -du -h -s $BUCKET/

# create/recreate a local dir
rm -r downloads
mkdir downloads

# download
time bin/hadoop fs -Dfs.iostatistics.logging.level=info -copyToLocal -t 10  $BUCKET/uploads/\*bundle\* downloads

# rename.
# expect success, and on s3 standard, a slow O(data) operation.
# on other stores, the speed may be much faster.
time bin/hadoop fs -mv $BUCKET/uploads/ $BUCKET/renamed

# verify the rename worked
bin/hadoop fs -ls -R $BUCKET/renamed

# big distcp up
time bin/hadoop distcp -numListstatusThreads 10  -overwrite -skipcrccheck -direct -m 8 share/hadoop/common/lib $BUCKET/common
```


### Cloudstore CLI

The cloudstore commands help test lower-level aspects of the system, load generation
operations and more. They also print a lot more diagnostics on failure than
the hadoop fs commands.

#### Storediag

A key command to test is storediag with the `-debug` option, to verify that the low level SDK and httpclient logs can be set to log at debug.
```
bin/hadoop jar $CLOUDSTORE storediag -debug -w s3a://alice--usw2-az1--x-s3/path
```

The `-debug` statement causes the relevant log4J logs to log at debug, so verifies that the shading within the library has kept the binding to the unshaded SLF4J API.
The log should be long, verbose and full of debug statements including from httpclient classes, and those of `awssdk`
```
2025-10-24 16:57:24,823 [main] DEBUG impl.DeleteOperation (DeleteOperation.java:execute(196)) - Delete path s3a://alice--usw2-az1--x-s3/path - recursive true
2025-10-24 16:57:24,823 [main] DEBUG impl.DeleteOperation (DeleteOperation.java:execute(197)) - Type = Empty Directory
2025-10-24 16:57:24,823 [main] DEBUG impl.DeleteOperation (DeleteOperation.java:execute(205)) - delete: Path is a directory: s3a://alice--usw2-az1--x-s3/path
2025-10-24 16:57:24,823 [main] DEBUG impl.DeleteOperation (DeleteOperation.java:execute(225)) - deleting empty directory s3a://alice--usw2-az1--x-s3/path
2025-10-24 16:57:24,823 [main] DEBUG impl.DeleteOperation (DeleteOperation.java:deleteObjectAtPath(381)) - delete: dir marker path/
2025-10-24 16:57:24,823 [main] DEBUG s3a.Invoker (DurationInfo.java:<init>(80)) - Starting: delete
2025-10-24 16:57:24,823 [main] DEBUG impl.S3AStoreImpl (DurationInfo.java:<init>(80)) - Starting: deleting path/
2025-10-24 16:57:24,824 [main] DEBUG impl.LoggingAuditor (LoggingAuditor.java:modifyHttpRequest(435)) - [1] 3f275bbb-83b4-4289-85ca-39891b39c48c-00000026 Executing op_delete with {object_delete_request 'path/' size=1, mutating=true}; https://audit.example.org/hadoop/1/op_delete/3f275bbb-83b4-4289-85ca-39891b39c48c-00000026/?op=op_delete&p1=s3a://alice--usw2-az1--x-s3/path&pr=stevel&ps=648f56b4-9eb3-4618-8819-94a4306a88ed&ks=1&cm=StoreDiag&id=3f275bbb-83b4-4289-85ca-39891b39c48c-00000026&t0=1&fs=3f275bbb-83b4-4289-85ca-39891b39c48c&t1=1&ts=1761321444455
2025-10-24 16:57:24,824 [main] DEBUG awssdk.request (LoggerAdapter.java:debug(105)) - Sending Request: DefaultSdkHttpFullRequest(httpMethod=DELETE, protocol=https, host=alice--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com, encodedPath=/path/, headers=[amz-sdk-invocation-id, Referer, User-Agent], queryParameters=[])
2025-10-24 16:57:24,825 [main] DEBUG protocol.RequestAddCookies (RequestAddCookies.java:process(123)) - CookieSpec selected: default
2025-10-24 16:57:24,825 [main] DEBUG protocol.RequestAuthCache (RequestAuthCache.java:process(77)) - Auth cache not set in the context
2025-10-24 16:57:24,825 [main] DEBUG conn.PoolingHttpClientConnectionManager (PoolingHttpClientConnectionManager.java:requestConnection(267)) - Connection request: [route: {s}->https://alice--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com:443][total available: 2; route allocated: 2 of 512; total allocated: 2 of 512]
2025-10-24 16:57:24,825 [main] DEBUG conn.PoolingHttpClientConnectionManager (PoolingHttpClientConnectionManager.java:leaseConnection(312)) - Connection leased: [id: 2][route: {s}->https://alice--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com:443][total available: 1; route allocated: 2 of 512; total allocated: 2 of 512]
2025-10-24 16:57:24,825 [main] DEBUG conn.DefaultManagedHttpClientConnection (LoggingManagedHttpClientConnection.java:setSocketTimeout(88)) - http-outgoing-2: set socket timeout to 15000
2025-10-24 16:57:24,825 [main] DEBUG conn.DefaultManagedHttpClientConnection (LoggingManagedHttpClientConnection.java:setSocketTimeout(88)) - http-outgoing-2: set socket timeout to 15000
2025-10-24 16:57:24,825 [main] DEBUG execchain.MainClientExec (MainClientExec.java:execute(255)) - Executing request DELETE /path/ HTTP/1.1
2025-10-24 16:57:24,825 [main] DEBUG execchain.MainClientExec (MainClientExec.java:execute(266)) - Proxy auth state: UNCHALLENGED
2025-10-24 16:57:24,825 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(133)) - http-outgoing-2 >> DELETE /path/ HTTP/1.1
2025-10-24 16:57:24,826 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(136)) - http-outgoing-2 >> Host: alice--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com
2025-10-24 16:57:24,826 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(136)) - http-outgoing-2 >> amz-sdk-invocation-id: 29bf3bad-6a45-9a0a-195a-18ea15289282
2025-10-24 16:57:24,826 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(136)) - http-outgoing-2 >> amz-sdk-request: attempt=1; max=3
2025-10-24 16:57:24,826 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(136)) - http-outgoing-2 >> Authorization: AWS4-HMAC-SHA256 Credential=AKIASHFDIJDQIQLMQDVQ/20251024/us-west-2/s3express/aws4_request, SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;host;referer;x-amz-content-sha256;x-amz-date, Signature=14a5bd504a4d3623a8a118cc7d0f0a4a75f30bca12471ac73ecd09f138726fb5
2025-10-24 16:57:24,826 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(136)) - http-outgoing-2 >> Referer: https://audit.example.org/hadoop/1/op_delete/3f275bbb-83b4-4289-85ca-39891b39c48c-00000026/?op=op_delete&p1=s3a://alice--usw2-az1--x-s3/path&pr=stevel&ps=648f56b4-9eb3-4618-8819-94a4306a88ed&ks=1&cm=StoreDiag&id=3f275bbb-83b4-4289-85ca-39891b39c48c-00000026&t0=1&fs=3f275bbb-83b4-4289-85ca-39891b39c48c&t1=1&ts=1761321444455
2025-10-24 16:57:24,826 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(136)) - http-outgoing-2 >> User-Agent: Hadoop 3.6.0-SNAPSHOT aws-sdk-java/2.35.4 md/io#sync md/http#Apache ua/2.1 api/S3#2.35.x os/Mac_OS_X#15.7.1 lang/java#1.8.0_362 md/OpenJDK_64-Bit_Server_VM#25.362-b09 md/vendor#Azul_Systems__Inc. md/en_GB m/F,G hll/cross-region
2025-10-24 16:57:24,826 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(136)) - http-outgoing-2 >> x-amz-content-sha256: UNSIGNED-PAYLOAD
2025-10-24 16:57:24,826 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(136)) - http-outgoing-2 >> X-Amz-Date: 20251024T155724Z
2025-10-24 16:57:24,826 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(136)) - http-outgoing-2 >> Connection: Keep-Alive
2025-10-24 16:57:24,827 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 >> "DELETE /path/ HTTP/1.1[\r][\n]"
2025-10-24 16:57:24,827 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 >> "Host: alice--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com[\r][\n]"
2025-10-24 16:57:24,827 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 >> "amz-sdk-invocation-id: 29bf3bad-6a45-9a0a-195a-18ea15289282[\r][\n]"
2025-10-24 16:57:24,827 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 >> "amz-sdk-request: attempt=1; max=3[\r][\n]"
2025-10-24 16:57:24,827 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 >> "Authorization: AWS4-HMAC-SHA256 Credential=AKIASHFDIJDQIQLMQDVQ/20251024/us-west-2/s3express/aws4_request, SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;host;referer;x-amz-content-sha256;x-amz-date, Signature=14a5bd504a4d3623a8a118cc7d0f0a4a75f30bca12471ac73ecd09f138726fb5[\r][\n]"
2025-10-24 16:57:24,827 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 >> "Referer: https://audit.example.org/hadoop/1/op_delete/3f275bbb-83b4-4289-85ca-39891b39c48c-00000026/?op=op_delete&p1=s3a://alice--usw2-az1--x-s3/path&pr=stevel&ps=648f56b4-9eb3-4618-8819-94a4306a88ed&ks=1&cm=StoreDiag&id=3f275bbb-83b4-4289-85ca-39891b39c48c-00000026&t0=1&fs=3f275bbb-83b4-4289-85ca-39891b39c48c&t1=1&ts=1761321444455[\r][\n]"
2025-10-24 16:57:24,827 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 >> "User-Agent: Hadoop 3.6.0-SNAPSHOT aws-sdk-java/2.35.4 md/io#sync md/http#Apache ua/2.1 api/S3#2.35.x os/Mac_OS_X#15.7.1 lang/java#1.8.0_362 md/OpenJDK_64-Bit_Server_VM#25.362-b09 md/vendor#Azul_Systems__Inc. md/en_GB m/F,G hll/cross-region[\r][\n]"
2025-10-24 16:57:24,827 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 >> "x-amz-content-sha256: UNSIGNED-PAYLOAD[\r][\n]"
2025-10-24 16:57:24,827 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 >> "X-Amz-Date: 20251024T155724Z[\r][\n]"
2025-10-24 16:57:24,827 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 >> "Connection: Keep-Alive[\r][\n]"
2025-10-24 16:57:24,828 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 >> "[\r][\n]"
2025-10-24 16:57:25,003 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 << "HTTP/1.1 204 No Content[\r][\n]"
2025-10-24 16:57:25,004 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 << "server: AmazonS3[\r][\n]"
2025-10-24 16:57:25,004 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 << "x-amz-request-id: 014a62cccf00019a16f067530509837b99ef0372[\r][\n]"
2025-10-24 16:57:25,004 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 << "x-amz-id-2: Qco6wT6qri9zH[\r][\n]"
2025-10-24 16:57:25,004 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 << "date: Fri, 24 Oct 2025 15:57:25 GMT[\r][\n]"
2025-10-24 16:57:25,004 [main] DEBUG http.wire (Wire.java:wire(73)) - http-outgoing-2 << "[\r][\n]"
2025-10-24 16:57:25,004 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(122)) - http-outgoing-2 << HTTP/1.1 204 No Content
2025-10-24 16:57:25,004 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(125)) - http-outgoing-2 << server: AmazonS3
2025-10-24 16:57:25,005 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(125)) - http-outgoing-2 << x-amz-request-id: 014a62cccf00019a16f067530509837b99ef0372
2025-10-24 16:57:25,005 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(125)) - http-outgoing-2 << x-amz-id-2: Qco6wT6qri9zH
2025-10-24 16:57:25,005 [main] DEBUG http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(125)) - http-outgoing-2 << date: Fri, 24 Oct 2025 15:57:25 GMT
2025-10-24 16:57:25,005 [main] DEBUG execchain.MainClientExec (MainClientExec.java:execute(285)) - Connection can be kept alive for 60000 MILLISECONDS
2025-10-24 16:57:25,005 [main] DEBUG conn.PoolingHttpClientConnectionManager (PoolingHttpClientConnectionManager.java:releaseConnection(344)) - Connection [id: 2][route: {s}->https://alice--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com:443] can be kept alive for 60.0 seconds
2025-10-24 16:57:25,005 [main] DEBUG conn.DefaultManagedHttpClientConnection (LoggingManagedHttpClientConnection.java:setSocketTimeout(88)) - http-outgoing-2: set socket timeout to 0
2025-10-24 16:57:25,005 [main] DEBUG conn.PoolingHttpClientConnectionManager (PoolingHttpClientConnectionManager.java:releaseConnection(351)) - Connection released: [id: 2][route: {s}->https://alice--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com:443][total available: 2; route allocated: 2 of 512; total allocated: 2 of 512]
2025-10-24 16:57:25,006 [main] DEBUG awssdk.request (LoggerAdapter.java:debug(105)) - Received successful response: 204, Request ID: 014a62cccf00019a16f067530509837b99ef0372, Extended Request ID: Qco6wT6qri9zH
```

#### Cloudstore Operations

```bash

# ---------------------------------------------------
# Cloudstore
# ---------------------------------------------------

# bucket metadata; may return null values for third-party stores
bin/hadoop jar $CLOUDSTORE bucketmetadata $BUCKET

# stresses upload speed, and that the pool and timeout settings work
# a store without multipart uploads (i.e. google gcs) will block for a very long time in close().
# other stores should queue work as soon as it reached a 64M block size.
# Note that for long close() operations, callbacks during the close() are critical.
# distcp relies on these progress callbacks for heartbeats, and if its workers don't report in on time
# the worker process is killed. This is why rename-by-copy is so toxic here: no callbacks to make
# and big file renames can trigger timeout.
time bin/hadoop jar $CLOUDSTORE bandwidth 512M $BUCKET/testfile

# repeat for analytics policy
time bin/hadoop jar $CLOUDSTORE bandwidth -policy analytics -rename 512M $BUCKET/testfile

# repeat then close during the download (not the upload, we know that can hang)
time bin/hadoop jar $CLOUDSTORE bandwidth -policy analytics -rename 128M $BUCKET/testfile

# bulk upload command, optimized for cloud storage.
# Expect a fast parallelized upload of all the libraries; bundle.jar file is the slow one
time bin/hadoop jar $CLOUDSTORE cloudup share/hadoop/tools/lib/ $BUCKET/cloudup

# expect many files
bin/hadoop fs -ls -R $BUCKET/cloudup

# ---------------------------------------------------
# Cloudstore Bulk delete uses the hadoop bulkdelete API;
# the listing of files to delete must first be
# generated.
# ---------------------------------------------------

# list the files
bin/hadoop fs -ls -C $BUCKET/cloudup > downloads/listing.txt

# verify it has the listing of paths only
cat downloads/listing.txt

# edit out any log messages which have crept in
# (left as an exercise for the reader)

# then issue the bulk delete.
# this will be faster on stores with bulk delete than those without.

time bin/hadoop jar $CLOUDSTORE bulkdelete -verbose -page 5 $BUCKET/ downloads/listing.txt
```


### Semantics of Incomplete Uploads

#### Skip This Section on Stores Without Multipart Upload

Amazon S3 Express may return directories with incomplete uploads pending underneath; other stores should not.

The behavior can be tested by creating an incomplete file write, which is done on the command
line by replicating the operations of a job running under the Magic S3A committer.

```bash
bin/hadoop fs -mkdir -p $BUCKET/incomplete/__magic_job-0/__base/subdir
bin/hadoop fs -mkdir -p $BUCKET/incomplete/tail

echo "magic file" > localfile
# expected output to include # "File incomplete/subdir/magic2.txt will be visible when the job is committed"
bin/hadoop fs -put -d localfile $BUCKET/incomplete/__magic_job-0/__base/subdir/magic.txt

bin/hadoop fs -ls -R -h $BUCKET/incomplete/

```

The output of the listing operation is the interesting one, as on S3 Express a message will be printed about how a missing directory "incomplete/subdir" is being ignored.
```
drwxrwxrwx   - stevel stevel 0 2025-10-16 15:55 s3a://alice--usw2-az1--x-s3/incomplete/tail
drwxrwxrwx   - stevel stevel 0 2025-10-16 15:55 s3a://alice--usw2-az1--x-s3/incomplete/__magic_job-0
drwxrwxrwx   - stevel stevel 0 2025-10-16 15:55 s3a://alice--usw2-az1--x-s3/incomplete/__magic_job-0/__base
drwxrwxrwx   - stevel stevel 0 2025-10-16 15:55 s3a://alice--usw2-az1--x-s3/incomplete/__magic_job-0/__base/subdir
-rw-rw-rw-   1 stevel stevel 0 2025-10-16 15:41 s3a://alice--usw2-az1--x-s3/incomplete/__magic_job-0/__base/subdir/magic.txt
-rw-rw-rw-   1 stevel stevel 3724 2025-10-16 15:41 s3a://alice--usw2-az1--x-s3/incomplete/__magic_job-0/__base/subdir/magic.txt.pending
drwxrwxrwx   - stevel stevel 0 2025-10-16 15:55 s3a://alice--usw2-az1--x-s3/incomplete/subdir
2025-10-16 15:55:40,264 [main] INFO  fs.FileUtil (FileUtil.java:maybeIgnoreMissingDirectory(2108)) - Ignoring missing directory s3a://alice--usw2-az1--x-s3/incomplete/subdir
```

This signifies that the LIST operation returned the prefix `incomplete/subdir`, but when the treewalking list algorithm attempted to list the path,
an empty listing was returned by the store, which was mapped into a `FileNotFoundException` by the S3A code.
The store's path capabilities declare that it may be inconsistent, so the ls treewalk code knows to log and continue at this point.

Note also that the ordering of the list results is not alphabetical; this is presumably an aspect of the `ls` command.

To verify that the store considers its listings consistent or inconsistent, use the cloudstore `pathcapability` command
```bash
bin/hadoop jar $CLOUDSTORE pathcapability fs.capability.directory.listing.inconsistent $BUCKET/incomplete
echo $?
```

This returns 0 for the capability being found; and -1/255 for it being absent.

A nonrecursive list may return them in a different order and will not print any warning about ignoring a directory;
this listing does not inspect the subdirectories, and on S3 Express does not yet know that "subdir" isn't present
```bash
> bin/hadoop fs -ls $BUCKET/incomplete/

Found 3 items
drwxrwxrwx   - stevel stevel 0 2025-10-16 15:57 s3a://alice--usw2-az1--x-s3/incomplete/__magic_job-0
drwxrwxrwx   - stevel stevel 0 2025-10-16 15:57 s3a://alice--usw2-az1--x-s3/incomplete/subdir
drwxrwxrwx   - stevel stevel 0 2025-10-16 15:57 s3a://alice--usw2-az1--x-s3/incomplete/tail
```

Explicitly listing the path will raise an error (rather than have it swallowed).

```bash
# exit code will be 1
bin/hadoop fs -ls -R $BUCKET/incomplete/subdir
```
This prints an error on all stores.
```
ls: `s3a://alice--usw2-az1--x-s3//incomplete/subdir': No such file or directory
```

Cloudstore offers commands to get more detail on objects in the store

The `list` does a deep `listFiles(path)` call, this will return all objects underneath - but the S3A code then strips out directory markers.

*not supported for v1 buckets*
```bash
bin/hadoop jar $CLOUDSTORE list $BUCKET/incomplete
```

The result is the same on all stores: the zero byte "magic.txt" file and a json "magic.txt.pending" file
```

1. Listing files under s3a://alice--usw2-az1--x-s3/incomplete
==============================================================

2025-10-16 16:04:01,227 [main] INFO  commands.ListFiles (StoreDurationInfo.java:<init>(91)) - Starting: Directory list
2025-10-16 16:04:01,228 [main] INFO  commands.ListFiles (StoreDurationInfo.java:<init>(91)) - Starting: First listing
2025-10-16 16:04:03,266 [main] INFO  commands.ListFiles (StoreDurationInfo.java:close(200)) - Duration of First listing: 00:00:02.038
[0001]  s3a://alice--usw2-az1--x-s3/incomplete/__magic_job-0/__base/subdir/magic.txt   0 (0 bytes) 0 stevel  stevel  [encrypted]
[0002]  s3a://alice--usw2-az1--x-s3/incomplete/__magic_job-0/__base/subdir/magic.txt.pending   3,724   (3 KB)  3724 stevel  stevel  [encrypted]
2025-10-16 16:04:03,300 [main] INFO  commands.ListFiles (StoreDurationInfo.java:close(200)) - Duration of Directory list: 00:00:02.074

Found 2 files, 1,037 milliseconds per file
```

```bash
bin/hadoop jar $CLOUDSTORE listobjects $BUCKET/incomplete
```

The `listobjects` call returns all objects under a path.
It gives the real view of the store, without any attempts to put a directory tree metaphor
atop it.

```

1. Listing objects under s3a://alice--usw2-az1--x-s3/incomplete
================================================================

[00001] "incomplete/tail/"   size: [0] 2025-10-16T14:54:00Z tag: "1f0a9fe3c227498881ac13f373563882"
[00002] "incomplete/__magic_job-0/__base/subdir/" size: [0] 2025-10-16T14:41:17Z tag: "33ccdc239a3443a7a400ac9e18eaf22e"
[00003] "incomplete/__magic_job-0/__base/subdir/magic.txt"   size: [0] 2025-10-16T14:41:31Z tag: "62a493dfeed6493292606c6fe632ffc3"
[00004] "incomplete/__magic_job-0/__base/subdir/magic.txt.pending"   size: [3724] 2025-10-16T14:41:32Z tag: "ad02da963cc0435e8036cd3c83e34ec3"

Found 4 objects with total size 3724 bytes


2. Marker count: 2
==================

incomplete/tail/
incomplete/__magic_job-0/__base/subdir/
```

A call to `getfattr` to list all attributes will show that the magic.txt file has the attribute, `header.x-hadoop-s3a-magic-data-length`.
That declares what the final length of the file will be.

```bash
bin/hadoop fs -getfattr -d $BUCKET/incomplete/__magic_job-0/__base/subdir/magic.txt
```

Spark reads this so its progress indicators correctly reflect the amount of data
generated.
```
# file: s3a://alice--usw2-az1--x-s3/incomplete/__magic_job-0/__base/subdir/magic.txt
header.Content-Length="0"
header.Content-Type="application/octet-stream"
header.ETag=""62a493dfeed6493292606c6fe632ffc3""
header.Last-Modified="Thu Oct 16 15:41:31 BST 2025"
header.x-amz-server-side-encryption="AES256"
header.x-amz-storage-class="EXPRESS_ONEZONE"
header.x-hadoop-s3a-magic-data-length="11"
```
The other headers are all those published by the store.
The x-amz headers are custom to Amazon S3; they may or may not be replicated by other stores.
The `header.ETag` header is one which is used for etag queries; stores which do not return this
lack the ability to reject GET requests made with different etag versions,
preventing S3A from detecting and failing on any changes in files updated while being read.

The cloudstore `locatefiles` command uses the same listing classes as is used for scanning
directory trees in MapReduce queries and Spark jobs where the set of input files is not
already known.

```bash
bin/hadoop jar $CLOUDSTORE locatefiles $BUCKET/incomplete
```

Again, against an S3 Express bucket, we expect the "Ignoring missing directory".
This MUST NOT be seen against any classic store.
```
1. Locating files under s3a://alice--usw2-az1--x-s3/incomplete with thread count 4
===================================================================================

2025-10-16 17:23:30,794 [main] INFO  commands.LocateFiles (StoreDurationInfo.java:<init>(91)) - Starting: List located files
2025-10-16 17:23:30,795 [main] INFO  commands.LocateFiles (StoreDurationInfo.java:<init>(91)) - Starting: LocateFileStatus execution
2025-10-16 17:23:34,350 [GetFileInfo #3] INFO  fs.FileUtil (FileUtil.java:maybeIgnoreMissingDirectory(2108)) - Ignoring missing directory s3a://alice--usw2-az1--x-s3/incomplete/subdir
Fetched by: LocatedFileStatusFetcher[...]
2025-10-16 17:23:34,359 [main] INFO  commands.LocateFiles (StoreDurationInfo.java:close(200)) - Duration of List located files: 00:00:03.566

Found 0 files, 0 milliseconds per file
Data size 0 bytes, 0 bytes per file
```


```bash

bin/hadoop s3guard uploads -list $BUCKET
bin/hadoop s3guard uploads -abort -force $BUCKET
# final cleanup
bin/hadoop fs -rm -R $BUCKET/incomplete/
```

#### Final Commands

Add any other commands you can think of!

#### Don't Forget to Clean Up!

Clean up all objects, _and all pending uploads_.
That really matters for stores which don't support lifecycle policies.

```bash

bin/hadoop fs -rm -r $BUCKET/\*
bin/hadoop s3guard uploads -list $BUCKET
bin/hadoop s3guard uploads -abort -force $BUCKET
rm -r downloads
```

## More Testing

* Whatever applications you have which use S3A: build and run them before the upgrade,
then see if they complete successfully in roughly the same time once the upgrade is applied.
* Test any third-party endpoints you have access to.
* Try different regions (especially a v4 only region), and encryption settings.
* Any performance tests you have can identify slowdowns, which can be a sign
  of changed behavior in the SDK (especially on stream reads and writes).
* If you can, try to test in an environment where a proxy is needed to talk to AWS services.
* Get other people, especially anyone with their own endpoints,
  apps or different deployment environments, to run their own tests.
* Run the load tests, especially `ILoadTestS3ABulkDeleteThrottling`.


## What if There Are Failures?

Be prepared to roll-back, re-iterate or code your way out of a regression.

There may be some problems which surface with wider use, which can get
fixed in a new AWS release, rolling back to an older one,
or just worked around [HADOOP-14596](https://issues.apache.org/jira/browse/HADOOP-14596).

Don't be surprised if this happens, don't worry too much, and,
while that rollback option is there to be used, ideally try to work forwards.

If the problem is with the SDK, file issues with the
 [AWS V2 SDK Bug tracker](https://github.com/aws/aws-sdk-java-v2/issues).

If it is some minor test code failure only now surfacing with a broader
test of different endpoints, plan to fix it in the "test failures"
JIRA if it is small enough.

Test failures against third-party stores are a special case.
If the test turns out to be using an AWS-only feature, then the
test must be skippable, and test configurations for third-party stores
declare this.

If the failure is related to a new production-side feature (such as conditional overwrite)
which is not available on all third-party stores, then that feature must
be something which can be disabled in production, the state exported
as a path capability, and the tests written to automatically skip the
test if the capability is not present.

That change is not something which should be fixed in the upgrade branch.

### Deprecated APIs and New Features in the SDK

A Yetus run should tell you if there are new deprecations.
If so, you should think about how to deal with them.

Moving to methods and APIs which weren't in the previous SDK release makes it
harder to roll back if there is a problem; but there may be good reasons
for the deprecation.

At the same time, there may be good reasons for staying with the old code.

* AWS have embraced the builder pattern for new operations; note that objects
constructed this way often have their (existing) setter methods disabled; this
may break existing code.
* New versions of S3 calls (list v2, bucket existence checks, bulk operations)
may be better than the previous HTTP operations & APIs, but they may not work with
third-party endpoints, so can only be adopted if made optional, which then
adds a new configuration option (with docs, testing, ...). A change like that
must be done in its own patch, with its new tests which compare the old
vs new operations.


### What to Do if There Is an SDK Regression?

Obviously, the PR cannot be merged until resolved.
The cause has to be identified, then fixed.

The default assumption should be "our assumptions about the behaviour of the SDK proved to be incorrect".
Identifying what has gone wrong, and being aware that those assumptions were made, means that we can fix it ourselves.
Although this does require engineering effort,
it doesn't guarantee that we can get a fix in without waiting for any changes from the AWS SDK developers.

### Tracking Down a Test Failure

What to do if a test starts failing?

**First**, add the stack traces to the PRs as a comment.
As well as warning everyone of problems, it generates a searchable trace for the future.

**Next**, *do not assume that this is a bug in the tests*.
Assume that the test has identified a regression in production code.
Hopefully it is just a test failure due to minor changes in SDK behavior - that is the best case scenario.
Assuming it is a test failure and so disabling the test case/assertion is a mistake.
The root cause is still out there, waiting to surface again in production.

_only disable a test case/assert once you are confident it is not a production-time issue_.

### Process for Fixing an Emergency Facility/Bug in Our Code

Treat it as any other bug in the code.
1. Create a new JIRA ticket.
2. Link to the SDK upgrade JIRA as a Blocker.

As usual, it needs a test to replicate it and a fix.
If the change can be done independently of the SDK update,
then it can go into the code base before the version is updated.

What about changes which require the SDK in first?
If it is a small change, especially a low risk test one, include it in the SDK update.

If it is a large change:
1. Create a Hadoop PR to update the SDK only.
2. Create a feature branch with the commit of #1 at the bottom.
3. Get all the code reviewed by the normal process *but do not merge it once approved*
4. The final merge should be done with a merge of the SDK in first, with that commit
   message declaring it must be followed by the big patch (state the JIRA and PR IDs).
5. Apply the big patch immediately after the SDK update PR is merged, with a mention
   of that JIRA/PR ID in the commit message body.


### What if It Is a Bug in the AWS SDK Itself?

This is a problem; the seriousness depends on the nature of the issue.

#### Create a Hadoop JIRA

1. Create a JIRA with as much information as you can.
2. Describe the problem replicated, the consequences.
3. Check out the relevant SDK release and see if you can identify the root cause.
   It's complex enough that this is unlikely, but gaining familiarity with
   the SDK is a good investment.
4. See if you can replicate it reliably manually or as a JUnit test.
   A JUnit test is ideal as it makes regression testing straightforward - though such tests are
   rare, as most of the recent regressions have been related to service failures or jobs of an
   hour or more.

#### Look for an Existing SDK Issue

Search [AWS v2 SDK issues](https://github.com/aws/aws-sdk-java-v2/issues) for
reports of the topic.
Include recently closed issues, as it may have been fixed.

If the issue has been reported and is still open:
* Add a comment stating you are also affected, and the impact on our code.
* Subscribe to the issue
* Link the HADOOP JIRA to it.

#### Create an AWS SDK GitHub Issue if Needed

Create a matching issue for AWS, providing the same information in as much detail as you can.
Cross-link with the hadoop JIRA and vice versa.

Then try and come up with a workaround.
This may take a significant amount of effort.
Note that the class `org.apache.hadoop.fs.s3a.impl.AwsSdkWorkarounds` is a place for workarounds...
logging is already in there.

This class has its own ITests. Ideally these tests should fail when the underlying issue is fixed
-this highlights when a workaround can be removed.

If one cannot be found then we are essentially blocked from upgrading the AWS SDK at all.
We have had to do exactly this with problems related to library shading.

You cannot expect a bug report to the SDK team to result in any urgent fixes.
This is "unfortunate", especially given the petabytes of data which
is passed through the S3A connector to and from S3 every day.

[//]: # (we suspect that there is some Conway's law-structure at work here.)

[//]: # (Whoever maintains the SDK is not the same people who run the S3 service, and that priority does not appear to extend to this sort of library, even though many of their premium customers are using our code.)

If this all seems a bit negative: do not panic.
Most of the upgrades are straightforward and do not appear to cause any problems.


## Declaring the PR Ready to Merge


Storediag output of all stores is attached. For B1, diagnostics through an access point are also attached.

Then we have a set of attestations
[ ] I have run the S3 ITests against B1;  no failures were observed.
[ ] I have run the ITests against B2; no failures were observed.
[ ] I have used distcp to collect the audit logs from B2 - logs spanning the timespan of the tests.
[ ] I have run the ITests against B3; no failures were observed.
[ ] I have run the ITests against B4; no failures were observed.
[ ] If available, I have run the ITests against B5; no failures were observed.
[ ] I have compared the logs of the before and after runs; no differences were observed. (maybe we should provide a log4j format which logs at info and doesn't include time and thread IDs?)
[ ] I have run the CLI tests against all buckets, no failures or changes in logs were observed
[ ] I have added one or more new CLI tests to run; they are included in this PR. (forces submitter to think of new tests rather than set as "complete")
[ ] The formatted test results are attached as a single .tar file containing a subdir for each test bucket.

Then:

1. Measure the execution time of the before/after runs to see if there is any slowdown vs the previous version.
A simple `time mvn -T 1C verify -Dscale -Dparallel` of the hadoop-aws dir after just having done a `mvn clean install` to take that out of the timing would be enough.
This is to identify major changes. Ideally this should be done on an EC2 VM, so there are no network-related issues.

2. Check out the relevant tag of the aws-sdk-v2 and use ripgrep to count the # of matches of the pattern `\.warn\(` in those modules we care about.
For files where there's a change, open them, get the history, see what has changed.
This is not just to identify where we are being told off in a way which makes for noisy clients,
it is to see if there are potentially things we are getting wrong and which we should fix.

The PR submitter then has to make some commitments to followup on regressions.

If there is a regression identified by anyone
* I understand that the immediate action is a revert of the PR until addressed.
* I will collaborate with others to identify and replicate the problem.
* If a fix is needed in our code, I will collaborate on fixing the issue including writing automated/manual tests, and running them
* If the regression is in AWS code, I will file the AWS issue.
  Furthermore, if I'm an AWS engineer: file an internal one and emphasise to the SDK team that this matters to you
* If a workaround is needed to fix the SDK problem, I will collaborate with others to design and implement the workaround.

The key point here is to
1. Make clear that whoever provides the update owns a lot of the upgrade problem, rather than expect others to handle it.
2. Highlight that regressions are blockers on upgrades.

## Committing the Work

Throughout the work on the upgrade branch, the SDK and license upgrade MUST be kept in
an isolated commit, and any changes needed to fix test failures MUST go into a
separate commit in the same branch.

When merging to the trunk branch, the SDK update and any code changes MUST go in as
separate commits, to isolate the changes better.
IF there are no code changes - excellent!

Critical: include the AWS SDK version in the title of the commits, and update the JIRA
title to the version number actually used.

1. Commit the library change PR, note in comments that it requires the follow-on patch.
2. Code fix PR: If there have been multiple code changes to fix compatibility with the releases,
   merge the commits together, into the single "code fixes" commit.
   Commit that, referencing the SDK update.


## Backporting

Cherrypick the upgrade and code fix patches in order.
This is also the time to review the commit messages to see if they
are correct.

* MUST: rerun the `hadoop-aws` integration tests against AWS and third-party stores (at least B1, B3, B4 and B5)
* MUST: do a release build and try out some of the commands against one AWS and one third-party stores.

Do not assume that just because it worked in trunk it'll work in a backport: the further back you go, the more the codebase diverges, the more likelihood of problems.

It may be necessary to cherrypick other patches before the SDK upgrade goes through.
If so: do it.

# Appendices

## Fish Functions for a Happier Maven

Fish is [the command shell for the 1990s](https://fishshell.com/) and makes for a far better
testing environment than bash, zsh etc!

Here are some fish functions which provide useful shortcuts
to common operations.
All must go into `~/.config/fish/functions/`, in filenames matching
the function names.

```sh

# ~/.config/fish/functions/mci.fish
function mci --description 'Maven clean install; no tests or shading'
  mvn -T 1C clean install -DskipTests -DskipShade $argv
end

#  ~/.config/fish/functions/mi.fish
function mi --description 'Maven install -no tests or shading'
  mvn -T 1C  install -DskipTests -DskipShade -Dmaven.plugin.validation=none $argv
end

# ~/.config/fish/functions/mvndep.fish
function mvndep --description 'mvn dependency:tree -Dverbose'
  mvn -T 1C dependency:tree -Dverbose $argv
end

# ~/.config/fish/functions/mvit.fish
function mvit --description 'mvn integration test'
  mvn -T 1C integration-test $argv
end

# ~/.config/fish/functions/mvt.fish
function mvt --description 'mvn test'
  mvn -T 1C test $argv
end
```

Note: while the `-T 1C` improves parallelism, sometimes maven
has the odd concurrency-related failure, usually surfacing as lock acquisition
or the whole build actually hanging.

```
[ERROR] Failed to execute goal org.apache.maven.plugins:maven-remote-resources-plugin:1.5:process (default) on project hadoop-aws: Execution default of goal org.apache.maven.plugins:maven-remote-resources-plugin:1.5:process failed: Could not acquire lock(s) -> [Help 1]
```

Do remember that these functions all set the `-T 1C` and, if builds
show problems - stop using them.

## Effective Logging

Here is a log4j file which silences unrelated logs, prints output of different levels in different colors,
and has commented out entries for low-level debugging.

```properties
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# log4j configuration used during build and unit tests

log4j.debug=false

#log4j.rootLogger=info,stdout
log4j.rootLogger=INFO,StdoutErrorFatal,StdoutWarn,StdoutInfo,StdoutDebug,StdoutTrace
log4j.threshold=ALL

log4j.appender.StdoutErrorFatal=org.apache.log4j.ConsoleAppender
log4j.appender.StdoutErrorFatal.layout=org.apache.log4j.PatternLayout
log4j.appender.StdoutErrorFatal.layout.conversionPattern=\u001b[31;1m%d{ISO8601} [%t] %-5p %c{2} (%F:%M(%L)) - %m%n
log4j.appender.StdoutErrorFatal.threshold=ERROR
log4j.appender.StdoutErrorFatal.Target=System.err

log4j.appender.StdoutWarn=org.apache.log4j.ConsoleAppender
log4j.appender.StdoutWarn.layout=org.apache.log4j.PatternLayout
log4j.appender.StdoutWarn.layout.conversionPattern=\u001b[33;1m%d{ISO8601} [%t] %-5p %c{2} (%F:%M(%L)) - %m%n
log4j.appender.StdoutWarn.threshold=WARN
log4j.appender.StdoutWarn.filter.filter1=org.apache.log4j.varia.LevelRangeFilter
log4j.appender.StdoutWarn.filter.filter1.levelMin=WARN
log4j.appender.StdoutWarn.filter.filter1.levelMax=WARN
log4j.appender.StdoutWarn.Target=System.err

log4j.appender.StdoutInfo=org.apache.log4j.ConsoleAppender
log4j.appender.StdoutInfo.layout=org.apache.log4j.PatternLayout
log4j.appender.StdoutInfo.layout.conversionPattern=\u001b[0m%d{ISO8601} [%t] %-5p %c{2} (%F:%M(%L)) - %m%n
log4j.appender.StdoutInfo.threshold=INFO
log4j.appender.StdoutInfo.filter.filter1=org.apache.log4j.varia.LevelRangeFilter
log4j.appender.StdoutInfo.filter.filter1.levelMin=INFO
log4j.appender.StdoutInfo.filter.filter1.levelMax=INFO
log4j.appender.StdoutInfo.Target=System.err

log4j.appender.StdoutDebug=org.apache.log4j.ConsoleAppender
log4j.appender.StdoutDebug.layout=org.apache.log4j.PatternLayout
log4j.appender.StdoutDebug.layout.conversionPattern=\u001b[0;36m%d{ISO8601} [%t] %-5p %c{2} (%F:%M(%L)) - %m%n
log4j.appender.StdoutDebug.threshold=DEBUG
log4j.appender.StdoutDebug.filter.filter1=org.apache.log4j.varia.LevelRangeFilter
log4j.appender.StdoutDebug.filter.filter1.levelMin=DEBUG
log4j.appender.StdoutDebug.filter.filter1.levelMax=DEBUG
log4j.appender.StdoutDebug.Target=System.err

log4j.appender.StdoutTrace=org.apache.log4j.ConsoleAppender
log4j.appender.StdoutTrace.layout=org.apache.log4j.PatternLayout
log4j.appender.StdoutTrace.layout.conversionPattern=\u001b[0;30;1m%d{ISO8601} [%t] %-5p %c{2} (%F:%M(%L)) - %m%n
log4j.appender.StdoutTrace.threshold=TRACE
log4j.appender.StdoutTrace.filter.filter1=org.apache.log4j.varia.LevelRangeFilter
log4j.appender.StdoutTrace.filter.filter1.levelMin=TRACE
log4j.appender.StdoutTrace.filter.filter1.levelMax=TRACE
log4j.appender.StdoutTrace.Target=System.err

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{ISO8601} [%t] %-5p %c{2} (%F:%M(%L)) - %m%n


# log hadoop at info
log4j.logger.org.apache.hadoop=INFO

# silence unrelated logs
log4j.logger.org.apache.hadoop.http=WARN
log4j.logger.org.apache.hadoop.ipc=WARN
log4j.logger.org.apache.hadoop.ipc.Server=WARN
log4j.logger.org.apache.hadoop.metrics2=ERROR
log4j.logger.org.apache.hadoop.net.NetworkTopology=WARN
log4j.logger.org.apache.hadoop.security.authentication.server.AuthenticationFilter=WARN
log4j.logger.org.apache.hadoop.security.token.delegation=WARN
log4j.logger.org.apache.hadoop.util.NativeCodeLoader=ERROR
log4j.logger.org.apache.hadoop.util.GSet=WARN
log4j.logger.org.apache.hadoop.util.JvmPauseMonitor=WARN
log4j.logger.org.apache.hadoop.util.HostsFileReader=WARN
log4j.logger.org.apache.commons.beanutils=WARN

# MiniDFS clusters can be noisy
log4j.logger.org.apache.hadoop.hdfs.server=ERROR
log4j.logger.org.apache.hadoop.hdfs.shortcircuit.DomainSocketFactory=ERROR
log4j.logger.org.apache.hadoop.hdfs.StateChange=WARN
log4j.logger.BlockStateChange=WARN
log4j.logger.org.apache.hadoop.hdfs.DFSUtil=WARN

## YARN can be noisy too
log4j.logger.org.apache.hadoop.mapred.IndexCache=WARN
log4j.logger.org.apache.hadoop.mapred.ShuffleHandler=WARN
log4j.logger.org.apache.hadoop.yarn.event=WARN
log4j.logger.org.apache.hadoop.yarn.server.nodemanager=WARN
log4j.logger.org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor=WARN
log4j.logger.org.apache.hadoop.yarn.server.resourcemanager.scheduler=WARN
log4j.logger.org.apache.hadoop.yarn.server.resourcemanager.security=WARN
log4j.logger.org.apache.hadoop.yarn.util.ResourceCalculatorPlugin=ERROR
log4j.logger.org.apache.hadoop.yarn.util.AbstractLivelinessMonitor=WARN
log4j.logger.org.apache.hadoop.yarn.webapp.WebApps=WARN

# log shell at debug for stack traces
log4j.logger.org.apache.hadoop.fs.shell=DEBUG

# for debugging low level S3a operations, uncomment this line
#log4j.logger.org.apache.hadoop.fs.s3a=DEBUG


# a bit too noisy when logging at debug
log4j.logger.org.apache.hadoop.fs.s3a.S3AStorageStatistics=INFO


# TLS info
#log4j.logger.software.amazon.awssdk.thirdparty.org.apache.http.conn.ssl.SSLConnectionSocketFactory=DEBUG

# Low-level trace of HTTP requests.
# log4j.logger.software.amazon.awssdk.request=DEBUG
# log4j.logger.software.amazon.awssdk.thirdparty.org.apache.http=DEBUG

# Set to trace for detail metrics to be printed in
# LoggingMetricPublisher; set that to INFO for the output to be visible.
# log4j.logger.org.apache.hadoop.fs.s3a.DefaultS3ClientFactory=TRACE


```