/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;

public class ITestAzureBlobFileSystemStore extends AbstractAbfsIntegrationTest {

  private static final String TRUE_STR = "true";
  private static final String FALSE_STR = "false";

  public ITestAzureBlobFileSystemStore() throws Exception {
    super();
  }

  @Test
  public void testShouldMakeServerCallForHnsCheckWhenConfNotPresent()
      throws IOException {
    unsetAndAssert();
  }

  @Test
  public void testShouldMakeServerCallForHnsCheckWhenConfIsPresent()
      throws IOException {
    unsetAndAssert();
    setValidValueAndAssert(TRUE_STR.toUpperCase());
    unsetAndAssert();
    setValidValueAndAssert(FALSE_STR.toUpperCase());
    unsetAndAssert();
    setValidValueAndAssert(TRUE_STR.toLowerCase());
    unsetAndAssert();
    setValidValueAndAssert(FALSE_STR.toLowerCase());
    unsetAndAssert();
  }

  @Test
  public void testShouldMakeServerCallForHnsCheckWhenInvalidConfIsPresent()
      throws IOException {
    unsetAndAssert();
    setInvalidValueAndAssert("Invalid conf value");
    unsetAndAssert();
    setInvalidValueAndAssert(" ");
    unsetAndAssert();
  }

  private void unsetAndAssert() throws IOException {
    final AzureBlobFileSystemStore abfsStore = getFileSystem().getAbfsStore();
    abfsStore.getAbfsConfiguration()
        .setIsNamespaceEnabledAccount(FS_AZURE_ACCOUNT_IS_HNS_ENABLED);
    Assertions.assertThat(abfsStore.isNameSpaceEnabledSetFromConfig())
        .describedAs(
            "isNameSpaceEnabledSetFromConfig should return false when the"
                + " conf is not present").isFalse();
  }

  private void setValidValueAndAssert(String validConf) throws IOException {
    final AzureBlobFileSystemStore abfsStore = getFileSystem().getAbfsStore();
    abfsStore.getAbfsConfiguration().setIsNamespaceEnabledAccount(validConf);
    Assertions.assertThat(abfsStore.isNameSpaceEnabledSetFromConfig())
        .describedAs(
            "isNameSpaceEnabledSetFromConfig should return true when valid"
                + " conf is present").isTrue();
  }

  private void setInvalidValueAndAssert(String invalidConf) throws IOException {
    final AzureBlobFileSystemStore abfsStore = getFileSystem().getAbfsStore();
    abfsStore.getAbfsConfiguration().setIsNamespaceEnabledAccount(invalidConf);
    Assertions.assertThat(abfsStore.isNameSpaceEnabledSetFromConfig())
        .describedAs("isNameSpaceEnabledSetFromConfig should return false when"
            + " conf present is invalid").isFalse();
  }

}
