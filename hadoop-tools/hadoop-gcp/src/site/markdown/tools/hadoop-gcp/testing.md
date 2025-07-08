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

# Testing the GCS filesystem client and its features

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

This module includes both unit tests, which can run in isolation without
connecting to the GCS service, and integration tests, which require a working
connection to GCS to interact with a bucket.  Unit test suites follow the naming
convention `Test*.java`.  Integration tests follow the naming convention
`ITest*.java`.

## <a name="setting-up"></a> Setting up the tests

To integration test the GCS filesystem client, you need to provide
`auth-keys.xml` which passes in authentication details to the test runner.

It is a Hadoop XML configuration file, which must be placed into
`hadoop-tools/hadoop-gcp/src/test/resources`.

### File `core-site.xml`

This file pre-exists and sources the configurations created
under `auth-keys.xml`.

For most purposes you will not need to edit this file unless you
need to apply a specific, non-default property change during the tests.

### File `auth-keys.xml`

The presence of this file triggers the testing of the GCS classes.

Without this file, *none of the integration tests in this module will be
executed*.

* `fs.contract.test.fs.gs` : the URL of the bucket for GCS filesystem contract tests

Example:

```xml
<configuration>
    <property>
      <name>fs.gs.auth.type</name>
      <value>SERVICE_ACCOUNT_JSON_KEYFILE</value>
    </property>
    <property>
      <name>fs.gs.auth.service.account.json.keyfile</name>
      <value>YOUR_JSON_KEY_FILE</value>
    </property>
    <property>
      <name>fs.gs.project.id</name>
      <value>YOUR_PROJECT_ID_HERE</value>
    </property>
    <property>
      <name>fs.contract.test.fs.gs</name>
      <value>gs://your_bucket</value>
    </property>
</configuration>
```

## <a name="running"></a> Running the Tests

After completing the configuration, execute the test run through Maven.

```bash
mvn clean verify
```
