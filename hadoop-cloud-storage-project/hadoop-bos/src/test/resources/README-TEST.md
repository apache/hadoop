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

# BOS FileSystem Testing Guide

This document provides guidance on running and understanding the BOS FileSystem test suite.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Test Configuration](#test-configuration)
3. [Running Tests](#running-tests)
4. [Test Structure](#test-structure)
5. [Troubleshooting](#troubleshooting)

## Prerequisites

### Required

- Java 8 or higher
- Maven 3.3 or higher
- Access to a Baidu BOS bucket for testing
- BOS credentials (Access Key ID and Secret Access Key)

### BOS Bucket Setup

1. Create a BOS bucket for testing (or use an existing one)
2. Ensure the bucket is accessible with your credentials
3. Note: Tests will create and delete files under `/test` directory in the bucket

## Test Configuration

### Step 1: Create Test Configuration File

Copy the template configuration file:

```bash
cd src/test/resources
cp contract-test-options.xml.template contract-test-options.xml
```

### Step 2: Configure BOS Credentials

Edit `contract-test-options.xml` and set your BOS configuration:

```xml
<property>
    <name>fs.contract.test.fs.bos</name>
    <value>bos://your-bucket-name/test</value>
</property>

<property>
    <name>fs.bos.endpoint</name>
    <value>http://bd.bcebos.com</value>
</property>
```

### Step 3: Set Authentication

**Recommended: Use Environment Variables**

```bash
export BOS_ACCESS_KEY_ID=your_access_key_id
export BOS_SECRET_ACCESS_KEY=your_secret_access_key
```

Ensure the credentials provider is set to `EnvironmentVariableCredentialsProvider`:

```xml
<property>
    <name>fs.bos.credentials.provider</name>
    <value>org.apache.hadoop.fs.bos.credentials.EnvironmentVariableCredentialsProvider</value>
</property>
```

**Alternative: Configuration-based (Not Recommended)**

You can also configure credentials directly in `contract-test-options.xml`, but this is **NOT recommended** for security reasons:

```xml
<property>
    <name>fs.bos.access.key</name>
    <value>YOUR_ACCESS_KEY</value>
</property>

<property>
    <name>fs.bos.secret.key</name>
    <value>YOUR_SECRET_KEY</value>
</property>
```

**⚠️ SECURITY WARNING**: Never commit `contract-test-options.xml` with credentials to version control!

## Running Tests

### Run All Tests

```bash
mvn test
```

### Run Contract Tests Only

```bash
mvn test -Dtest="**/contract/*Test.java"
```

### Run Specific Contract Test Suite

```bash
# Test file creation
mvn test -Dtest=TestBosContractCreate

# Test rename operations
mvn test -Dtest=TestBosContractRename

# Test delete operations
mvn test -Dtest=TestBosContractDelete
```

### Run Integration Tests

```bash
mvn test -Dtest="**/integration/*Test.java"
```

### Run with Specific Bucket Type

For namespace-enabled bucket:
```bash
mvn test -Dfs.contract.test.fs.bos=bos://namespace-bucket/test
```

For flat (non-namespace) bucket:
```bash
mvn test -Dfs.contract.test.fs.bos=bos://flat-bucket/test
```

### Skip Tests

```bash
mvn package -DskipTests
```

## Test Structure

### Contract Tests (`org.apache.hadoop.fs.bos.contract`)

These tests verify that BOS FileSystem conforms to the Hadoop FileSystem contract:

- **TestBosContractCreate**: File creation operations
- **TestBosContractOpen**: File open and read operations
- **TestBosContractDelete**: File and directory deletion
- **TestBosContractMkdir**: Directory creation
- **TestBosContractRename**: File and directory rename
- **TestBosContractSeek**: Random access (seek) operations
- **TestBosContractGetFileStatus**: Metadata operations
- **TestBosContractRootDir**: Root directory operations (disabled by default)
- **TestBosContractContentSummary**: Content summary operations

### Integration Tests (`org.apache.hadoop.fs.bos.integration`)

These tests verify BOS-specific functionality:

- **TestBosIntegrationIO**: Input/Output stream operations
- **TestBosIntegrationChecksum**: Checksum calculation and verification
- **TestBosIntegrationMultipart**: Multi-part upload/download (if applicable)

### Credential Tests (`org.apache.hadoop.fs.bos.credentials`)

- **TestConfigurationCredentialsProvider**: Configuration-based authentication

## Test Features and Limitations

### Supported Features

- ✅ File create, read, delete
- ✅ Directory operations (mkdir, delete)
- ✅ Rename (via copy + delete)
- ✅ Random access (seek)
- ✅ Content summary
- ✅ Checksum verification

### Unsupported Features (Tests Will Skip)

- ❌ Append to files
- ❌ File concatenation
- ❌ File truncation
- ❌ Symbolic links
- ❌ Extended attributes (xattr)
- ❌ Full Unix permissions (limited support in namespace mode)

## Troubleshooting

### Authentication Failures

**Problem**: Tests fail with authentication errors

**Solutions**:
1. Verify credentials are correct
2. Check environment variables are set: `echo $BOS_ACCESS_KEY_ID`
3. Ensure credentials provider class is correctly configured
4. Verify network connectivity to BOS endpoint

### Bucket Not Found

**Problem**: `FileNotFoundException` or bucket access errors

**Solutions**:
1. Verify bucket name is correct in `contract-test-options.xml`
2. Ensure bucket exists in BOS
3. Check bucket region matches endpoint
4. Verify credentials have access to the bucket

### Test Timeouts

**Problem**: Tests hang or timeout

**Solutions**:
1. Check network connectivity to BOS
2. Verify firewall settings
3. Try different BOS endpoint
4. Increase timeout values in test configuration

### Inconsistent Test Results

**Problem**: Tests pass sometimes but fail other times

**Causes**:
- BOS is eventually consistent; some operations may have delays
- Network issues causing intermittent failures
- Concurrent test runs interfering with each other

**Solutions**:
1. Run tests sequentially: `mvn test -DforkCount=1`
2. Add retry logic for flaky tests
3. Ensure proper test cleanup

### Contract Test Failures

**Problem**: Contract tests fail or are skipped

**Expected Behavior**:
- Some tests may be skipped if features are not supported (e.g., append, concat)
- Check `contract/bos.xml` for feature flags

**Debugging**:
1. Enable debug logging: `mvn test -X`
2. Check test output for specific failure reasons
3. Verify BOS behavior matches Hadoop expectations

## Performance Testing

### Enable Scale Tests

Scale tests are disabled by default. To enable:

```xml
<property>
    <name>scale.test.enabled</name>
    <value>true</value>
</property>
```

**Warning**: Scale tests may take a long time and incur costs!

### Configure Scale Test Parameters

```xml
<property>
    <name>scale.test.operation.count</name>
    <value>1000</value>
</property>
```

## Best Practices

1. **Use Dedicated Test Bucket**: Don't use production buckets for testing
2. **Clean Up**: Tests should clean up after themselves, but verify manually
3. **Network**: Run tests in the same region as your BOS bucket for better performance
4. **Credentials**: Always use environment variables for credentials
5. **Version Control**: Never commit `contract-test-options.xml` to git

## Getting Help

- Check Hadoop FileSystem specification: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/
- Review Baidu BOS documentation
- Check project README.md
- File issues on project issue tracker

## Contributing Tests

When adding new tests:

1. Follow Hadoop contract test patterns
2. Add appropriate documentation
3. Ensure tests clean up resources
4. Handle both namespace and non-namespace modes
5. Add tests to appropriate package (contract vs integration)
6. Update this README with new test information

## CI/CD Integration

For automated testing in CI/CD pipelines:

```bash
# Example GitHub Actions / GitLab CI
export BOS_ACCESS_KEY_ID=${{ secrets.BOS_ACCESS_KEY_ID }}
export BOS_SECRET_ACCESS_KEY=${{ secrets.BOS_SECRET_ACCESS_KEY }}
mvn test
```

Ensure secrets are properly configured in your CI/CD system.