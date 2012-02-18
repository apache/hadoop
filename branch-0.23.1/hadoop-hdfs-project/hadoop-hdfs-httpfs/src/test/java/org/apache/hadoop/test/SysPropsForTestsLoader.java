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
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Properties;

public class SysPropsForTestsLoader {

  public static final String TEST_PROPERTIES_PROP = "test.properties";

  static {
    try {
      String testFileName = System.getProperty(TEST_PROPERTIES_PROP, "test.properties");
      File currentDir = new File(testFileName).getAbsoluteFile().getParentFile();
      File testFile = new File(currentDir, testFileName);
      while (currentDir != null && !testFile.exists()) {
        testFile = new File(testFile.getAbsoluteFile().getParentFile().getParentFile(), testFileName);
        currentDir = currentDir.getParentFile();
        if (currentDir != null) {
          testFile = new File(currentDir, testFileName);
        }
      }

      if (testFile.exists()) {
        System.out.println();
        System.out.println(">>> " + TEST_PROPERTIES_PROP + " : " + testFile.getAbsolutePath());
        Properties testProperties = new Properties();
        testProperties.load(new FileReader(testFile));
        for (Map.Entry entry : testProperties.entrySet()) {
          if (!System.getProperties().containsKey(entry.getKey())) {
            System.setProperty((String) entry.getKey(), (String) entry.getValue());
          }
        }
      } else if (System.getProperty(TEST_PROPERTIES_PROP) != null) {
        System.err.println(MessageFormat.format("Specified 'test.properties' file does not exist [{0}]",
                                                System.getProperty(TEST_PROPERTIES_PROP)));
        System.exit(-1);

      } else {
        System.out.println(">>> " + TEST_PROPERTIES_PROP + " : <NONE>");
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void init() {
  }

}
