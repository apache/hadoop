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

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_TIMEOUT;

/**
 * Base class for any ABFS test with timeouts & named threads.
 * This class does not attempt to bind to Azure.
 */
public class AbstractAbfsTestWithTimeout extends Assert {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractAbfsTestWithTimeout.class);

  /**
   * The name of the current method.
   */
  @Rule
  public TestName methodName = new TestName();
  /**
   * Set the timeout for every test.
   * This is driven by the value returned by {@link #getTestTimeoutMillis()}.
   */
  @Rule
  public Timeout testTimeout = new Timeout(getTestTimeoutMillis());

  /**
   * Name the junit thread for the class. This will overridden
   * before the individual test methods are run.
   */
  @BeforeClass
  public static void nameTestThread() {
    Thread.currentThread().setName("JUnit");
  }

  /**
   * Name the thread to the current test method.
   */
  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit-" + methodName.getMethodName());
  }

  /**
   * Override point: the test timeout in milliseconds.
   * @return a timeout in milliseconds
   */
  protected int getTestTimeoutMillis() {
    return TEST_TIMEOUT;
  }

  /**
   * Describe a test in the logs.
   *
   * @param text text to print
   * @param args arguments to format in the printing
   */
  protected void describe(String text, Object... args) {
    LOG.info("\n\n{}: {}\n",
        methodName.getMethodName(),
        String.format(text, args));
  }

  /**
   * Validate Contents written on a file in Abfs.
   *
   * @param fs                AzureBlobFileSystem
   * @param path              Path of the file
   * @param originalByteArray original byte array
   * @return if content is validated true else, false
   * @throws IOException
   */
  protected boolean validateContent(AzureBlobFileSystem fs, Path path,
      byte[] originalByteArray)
      throws IOException {
    int pos = 0;
    int lenOfOriginalByteArray = originalByteArray.length;

    try (FSDataInputStream in = fs.open(path)) {
      byte valueOfContentAtPos = (byte) in.read();

      while (valueOfContentAtPos != -1 && pos < lenOfOriginalByteArray) {
        if (originalByteArray[pos] != valueOfContentAtPos) {
          assertEquals("Mismatch in content validation at position {}", pos,
              originalByteArray[pos], valueOfContentAtPos);
          return false;
        }
        valueOfContentAtPos = (byte) in.read();
        pos++;
      }
      if (valueOfContentAtPos != -1) {
        assertEquals("Expected end of file", -1, valueOfContentAtPos);
        return false;
      }
      return true;
    }

  }

}
