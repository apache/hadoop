/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_KEY;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test continuation token which has equal sign.
 */
public final class ITestAbfsClient extends AbstractAbfsIntegrationTest {
  private static final int LIST_MAX_RESULTS = 500;
  private static final int LIST_MAX_RESULTS_SERVER = 5000;

  public ITestAbfsClient() throws Exception {
    super();
  }

  @Ignore("HADOOP-16845: Invalid continuation tokens are ignored by the ADLS "
      + "Gen2 service, so we are disabling this test until the service is fixed.")
  @Test
  public void testContinuationTokenHavingEqualSign() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    AbfsClient abfsClient =  fs.getAbfsClient();

    try {
      AbfsRestOperation op = abfsClient.listPath("/", true, LIST_MAX_RESULTS, "===========");
      Assert.assertTrue(false);
    } catch (AbfsRestOperationException ex) {
      Assert.assertEquals("InvalidQueryParameterValue", ex.getErrorCode().getErrorCode());
    }
  }

  @Ignore("Enable this to verify the log warning message format for HostNotFoundException")
  @Test
  public void testUnknownHost() throws Exception {
    // When hitting hostName not found exception, the retry will take about 14 mins until failed.
    // This test is to verify that the "Unknown host name: %s. Retrying to resolve the host name..." is logged as warning during the retry.
    AbfsConfiguration conf = this.getConfiguration();
    String accountName = this.getAccountName();
    String fakeAccountName = "fake" + UUID.randomUUID() + accountName.substring(accountName.indexOf("."));

    String fsDefaultFS = conf.get(FS_DEFAULT_NAME_KEY);
    conf.set(FS_DEFAULT_NAME_KEY, fsDefaultFS.replace(accountName, fakeAccountName));
    conf.set(FS_AZURE_ACCOUNT_KEY + "." + fakeAccountName, this.getAccountKey());

    intercept(AbfsRestOperationException.class,
            "UnknownHostException: " + fakeAccountName,
            () -> FileSystem.get(conf.getRawConfiguration()));
  }

  @Test
  public void testListPathWithValidListMaxResultsValues()
      throws IOException, ExecutionException, InterruptedException {
    final int fileCount = 10;
    final String directory = "testWithValidListMaxResultsValues";
    createDirectoryWithNFiles(directory, fileCount);
    final int[] testData = {fileCount + 100, fileCount + 1, fileCount,
        fileCount - 1, 1};
    for (int i = 0; i < testData.length; i++) {
      int listMaxResults = testData[i];
      setListMaxResults(listMaxResults);
      int expectedListResultsSize =
          listMaxResults > fileCount ? fileCount : listMaxResults;
      Assertions.assertThat(listPath(directory)).describedAs(
          "AbfsClient.listPath result should contain %d items when "
              + "listMaxResults is %d and directory contains %d items",
          expectedListResultsSize, listMaxResults, fileCount)
          .hasSize(expectedListResultsSize);
    }
  }

  @Test
  public void testListPathWithValueGreaterThanServerMaximum()
      throws IOException, ExecutionException, InterruptedException {
    setListMaxResults(LIST_MAX_RESULTS_SERVER + 100);
    final String directory = "testWithValueGreaterThanServerMaximum";
    createDirectoryWithNFiles(directory, LIST_MAX_RESULTS_SERVER + 200);
    Assertions.assertThat(listPath(directory)).describedAs(
        "AbfsClient.listPath result will contain a maximum of %d items "
            + "even if listMaxResults >= %d or directory "
            + "contains more than %d items", LIST_MAX_RESULTS_SERVER,
        LIST_MAX_RESULTS_SERVER, LIST_MAX_RESULTS_SERVER)
        .hasSize(LIST_MAX_RESULTS_SERVER);
  }

  @Test
  public void testListPathWithInvalidListMaxResultsValues() throws Exception {
    for (int i = -1; i < 1; i++) {
      setListMaxResults(i);
      intercept(AbfsRestOperationException.class, "Operation failed: \"One of "
          + "the query parameters specified in the request URI is outside" + " "
          + "the permissible range.", () -> listPath("directory"));
    }
  }

  private List<ListResultEntrySchema> listPath(String directory)
      throws IOException {
    return getFileSystem().getAbfsClient()
        .listPath(directory, false, getListMaxResults(), null).getResult()
        .getListResultSchema().paths();
  }

  private int getListMaxResults() throws IOException {
    return getFileSystem().getAbfsStore().getAbfsConfiguration()
        .getListMaxResults();
  }

  private void setListMaxResults(int listMaxResults) throws IOException {
    getFileSystem().getAbfsStore().getAbfsConfiguration()
        .setListMaxResults(listMaxResults);
  }

  private void createDirectoryWithNFiles(String directory, int n)
      throws ExecutionException, InterruptedException {
    final List<Future<Void>> tasks = new ArrayList<>();
    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < n; i++) {
      final Path fileName = new Path("/" + directory + "/test" + i);
      tasks.add(es.submit(() -> {
        touch(fileName);
        return null;
      }));
    }
    for (Future<Void> task : tasks) {
      task.get();
    }
    es.shutdownNow();
  }
}
