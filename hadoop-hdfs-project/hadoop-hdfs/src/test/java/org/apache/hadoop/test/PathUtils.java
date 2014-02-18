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

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.Path;

public class PathUtils {

  public static Path getTestPath(Class<?> caller) {
    return getTestPath(caller, true);
  }

  public static Path getTestPath(Class<?> caller, boolean create) {
    return new Path(getTestDirName(caller));
  }

  public static File getTestDir(Class<?> caller) {
    return getTestDir(caller, true);
  }
  
  public static File getTestDir(Class<?> caller, boolean create) {
    File dir =
        new File(System.getProperty("test.build.data", "target/test/data")
            + "/" + RandomStringUtils.randomAlphanumeric(10),
            caller.getSimpleName());
    if (create) {
      dir.mkdirs();
    }
    return dir;
  }

  public static String getTestDirName(Class<?> caller) {
    return getTestDirName(caller, true);
  }
  
  public static String getTestDirName(Class<?> caller, boolean create) {
    return getTestDir(caller, create).getAbsolutePath();
  }
    
}
