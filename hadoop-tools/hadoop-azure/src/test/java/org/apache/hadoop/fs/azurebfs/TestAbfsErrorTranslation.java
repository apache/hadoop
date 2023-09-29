/*
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

import java.io.FileNotFoundException;
import java.net.HttpURLConnection;
import java.nio.file.AccessDeniedException;

import org.junit.Test;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem.checkException;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.AUTHORIZATION_PERMISSION_MISS_MATCH;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.PATH_ALREADY_EXISTS;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.PATH_NOT_FOUND;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test suite to verify exception conversion, filtering etc.
 */
public class TestAbfsErrorTranslation extends AbstractHadoopTestBase {

  public static final Path PATH = new Path("abfs//store/path");

  @Test
  public void testConvert403ToAccessDenied() throws Throwable {
    assertTranslated(HttpURLConnection.HTTP_FORBIDDEN,
        AUTHORIZATION_PERMISSION_MISS_MATCH,
        AccessDeniedException.class,
        AUTHORIZATION_PERMISSION_MISS_MATCH.getErrorCode());
  }

  @Test
  public void testConvert404ToFNFE() throws Throwable {
    assertTranslated(HttpURLConnection.HTTP_NOT_FOUND,
        PATH_NOT_FOUND,
        FileNotFoundException.class,
        PATH_NOT_FOUND.getErrorCode());
  }

  @Test
  public void testConvert409ToFileAlreadyExistsException() throws Throwable {
    assertTranslated(HttpURLConnection.HTTP_CONFLICT,
        PATH_ALREADY_EXISTS,
        FileAlreadyExistsException.class,
        PATH_ALREADY_EXISTS.getErrorCode());
  }

  /**
   * Assert that for a given status code and AzureServiceErrorCode, a specific
   * exception class is raised.
   * @param <E> type of exception
   * @param httpStatus http status code
   * @param exitCode AzureServiceErrorCode
   * @param clazz class of raised exception
   * @param expectedText text to expect in the exception
   * @throws Exception any other exception than the one expected
   */
  private <E extends Throwable> void assertTranslated(
      int httpStatus, AzureServiceErrorCode exitCode,
      Class<E> clazz, String expectedText) throws Exception {
    AbfsRestOperationException ex =
        new AbfsRestOperationException(httpStatus, exitCode.getErrorCode(),
            "", null);
    intercept(clazz, expectedText, () -> {
      checkException(PATH, ex);
      return "expected exception translation from " + ex;
    });
  }

}
