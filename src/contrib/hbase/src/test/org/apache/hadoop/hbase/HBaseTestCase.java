/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Abstract base class for test cases. Performs all static initialization
 */
public abstract class HBaseTestCase extends TestCase {
  static {
    StaticTestEnvironment.initialize();
  }
  
  protected Configuration conf;
  
  protected HBaseTestCase() {
    super();
    conf = new HBaseConfiguration();
  }
  
  protected HBaseTestCase(String name) {
    super(name);
    conf = new HBaseConfiguration();
  }

  protected Path getUnitTestdir(String testName) {
    return new Path(StaticTestEnvironment.TEST_DIRECTORY_KEY, testName);
  }
}
