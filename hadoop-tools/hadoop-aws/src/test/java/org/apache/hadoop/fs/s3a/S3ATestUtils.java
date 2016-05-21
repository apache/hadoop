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

package org.apache.hadoop.fs.s3a;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.junit.internal.AssumptionViolatedException;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;

public class S3ATestUtils {

  public static S3AFileSystem createTestFileSystem(Configuration conf) throws
      IOException {
    String fsname = conf.getTrimmed(TestS3AFileSystemContract.TEST_FS_S3A_NAME, "");


    boolean liveTest = !StringUtils.isEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals(Constants.FS_S3A);
    }
    if (!liveTest) {
      // This doesn't work with our JUnit 3 style test cases, so instead we'll
      // make this whole class not run by default
      throw new AssumptionViolatedException(
          "No test filesystem in " + TestS3AFileSystemContract.TEST_FS_S3A_NAME);
    }
    S3AFileSystem fs1 = new S3AFileSystem();
    //enable purging in tests
    conf.setBoolean(Constants.PURGE_EXISTING_MULTIPART, true);
    conf.setInt(Constants.PURGE_EXISTING_MULTIPART_AGE, 0);
    fs1.initialize(testURI, conf);
    return fs1;
  }

  public static FileContext createTestFileContext(Configuration conf) throws
      IOException {
    String fsname = conf.getTrimmed(TestS3AFileSystemContract.TEST_FS_S3A_NAME, "");

    boolean liveTest = !StringUtils.isEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals(Constants.FS_S3A);
    }
    if (!liveTest) {
      // This doesn't work with our JUnit 3 style test cases, so instead we'll
      // make this whole class not run by default
      throw new AssumptionViolatedException(
          "No test filesystem in " + TestS3AFileSystemContract.TEST_FS_S3A_NAME);
    }
    FileContext fc = FileContext.getFileContext(testURI,conf);
    return fc;
  }

  /**
   * Repeatedly attempt a callback until timeout or a {@link FailFastException}
   * is raised. This is modeled on ScalaTests {@code eventually(Closure)} code.
   * @param timeout timeout
   * @param callback callback to invoke
   * @throws FailFastException any fast-failure
   * @throws Exception the exception which caused the iterator to fail
   */
  public static void eventually(int timeout, Callable<Void> callback)
      throws Exception {
    Exception lastException;
    long endtime = System.currentTimeMillis() + timeout;
    do {
      try {
        callback.call();
        return;
      } catch (FailFastException e) {
        throw e;
      } catch (Exception e) {
        lastException = e;
      }
      Thread.sleep(500);
    } while (endtime > System.currentTimeMillis());
    throw lastException;
  }

  /**
   * The exception to raise so as to exit fast from
   * {@link #eventually(int, Callable)}.
   */
  public static class FailFastException extends Exception {
    public FailFastException() {
    }

    public FailFastException(String message) {
      super(message);
    }

    public FailFastException(String message, Throwable cause) {
      super(message, cause);
    }

    public FailFastException(Throwable cause) {
      super(cause);
    }
  }

  /**
   * Verify the class of an exception. If it is not as expected, rethrow it.
   * Comparison is on the exact class, not subclass-of inference as
   * offered by {@code instanceof}.
   * @param clazz the expected exception class
   * @param ex the exception caught
   * @return the exception, if it is of the expected class
   * @throws Exception the exception passed in.
   */
  public static Exception verifyExceptionClass(Class clazz,
      Exception ex)
      throws Exception {
    if (!(ex.getClass().equals(clazz))) {
      throw ex;
    }
    return ex;
  }

}
