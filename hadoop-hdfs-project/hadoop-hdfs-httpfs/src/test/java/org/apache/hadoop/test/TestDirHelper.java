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
package org.apache.hadoop.test;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

public class TestDirHelper implements MethodRule {

  @Test
  public void dummy() {
  }

  static {
    SysPropsForTestsLoader.init();
  }

  public static final String TEST_DIR_PROP = "test.dir";
  static String TEST_DIR_ROOT;

  private static void delete(File file) throws IOException {
    if (file.getAbsolutePath().length() < 5) {
      throw new IllegalArgumentException(
        MessageFormat.format("Path [{0}] is too short, not deleting", file.getAbsolutePath()));
    }
    if (file.exists()) {
      if (file.isDirectory()) {
        File[] children = file.listFiles();
        if (children != null) {
          for (File child : children) {
            delete(child);
          }
        }
      }
      if (!file.delete()) {
        throw new RuntimeException(MessageFormat.format("Could not delete path [{0}]", file.getAbsolutePath()));
      }
    }
  }

  static {
    try {
      TEST_DIR_ROOT = System.getProperty(TEST_DIR_PROP, new File("target").getAbsolutePath());
      if (!new File(TEST_DIR_ROOT).isAbsolute()) {
        System.err.println(MessageFormat.format("System property [{0}]=[{1}] must be set to an absolute path",
                                                TEST_DIR_PROP, TEST_DIR_ROOT));
        System.exit(-1);
      } else if (TEST_DIR_ROOT.length() < 4) {
        System.err.println(MessageFormat.format("System property [{0}]=[{1}] must be at least 4 chars",
                                                TEST_DIR_PROP, TEST_DIR_ROOT));
        System.exit(-1);
      }

      TEST_DIR_ROOT = new File(TEST_DIR_ROOT, "test-dir").getAbsolutePath();
      System.setProperty(TEST_DIR_PROP, TEST_DIR_ROOT);

      File dir = new File(TEST_DIR_ROOT);
      delete(dir);
      if (!dir.mkdirs()) {
        System.err.println(MessageFormat.format("Could not create test dir [{0}]", TEST_DIR_ROOT));
        System.exit(-1);
      }

      System.out.println(">>> " + TEST_DIR_PROP + "        : " + System.getProperty(TEST_DIR_PROP));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static ThreadLocal<File> TEST_DIR_TL = new InheritableThreadLocal<File>();

  @Override
  public Statement apply(final Statement statement, final FrameworkMethod frameworkMethod, final Object o) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        File testDir = null;
        TestDir testDirAnnotation = frameworkMethod.getAnnotation(TestDir.class);
        if (testDirAnnotation != null) {
          testDir = resetTestCaseDir(frameworkMethod.getName());
        }
        try {
          TEST_DIR_TL.set(testDir);
          statement.evaluate();
        } finally {
          TEST_DIR_TL.remove();
        }
      }
    };
  }

  /**
   * Returns the local test directory for the current test, only available when the
   * test method has been annotated with {@link TestDir}.
   *
   * @return the test directory for the current test. It is an full/absolute
   *         <code>File</code>.
   */
  public static File getTestDir() {
    File testDir = TEST_DIR_TL.get();
    if (testDir == null) {
      throw new IllegalStateException("This test does not use @TestDir");
    }
    return testDir;
  }

  private static AtomicInteger counter = new AtomicInteger();

  private static File resetTestCaseDir(String testName) {
    File dir = new File(TEST_DIR_ROOT);
    dir = new File(dir, testName + "-" + counter.getAndIncrement());
    dir = dir.getAbsoluteFile();
    try {
      delete(dir);
    } catch (IOException ex) {
      throw new RuntimeException(MessageFormat.format("Could not delete test dir[{0}], {1}",
                                                      dir, ex.getMessage()), ex);
    }
    if (!dir.mkdirs()) {
      throw new RuntimeException(MessageFormat.format("Could not create test dir[{0}]", dir));
    }
    return dir;
  }

}
