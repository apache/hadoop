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

# S3 Access Grants

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

S3 Access Grants is a credential vending service for S3 data. More information:
* https://aws.amazon.com/s3/features/access-grants/

In S3A, S3 Access Grants Plugin is used to support S3 Access Grants. More information:
* https://github.com/aws/aws-s3-accessgrants-plugin-java-v2/



## How to enable S3 Access Grants in S3A

1. Add the `hadoop-aws` JAR on your classpath.

1. Add the `aws-java-sdk-bundle.jar` JAR to your classpath, the minimum version is v2.23.7.

2. Add the `aws-s3-accessgrants-java-plugin-2.0.0.jar` JAR to your classpath.
3. Add the `caffeine.jar` JAR to your classpath.

1. Add configurations to enable S3 Access Grants in `core-site.xml`



Example:

```xml
<configuration>
...
    <property>
        <name>fs.s3a.accessgrants.enabled</name>
        <value>true</value>
        <description>Enable S3 Access Grants or not</description>
    </property>
    <property>
        <name>fs.s3a.accessgrants.fallbacktoiam</name>
        <value>false</value>
        <description>Enable IAM Policy as fallback or not</description>
    </property>
...
</configuration>
```




