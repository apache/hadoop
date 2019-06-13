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

package org.apache.hadoop.fs.ozone;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * FilteredClassLoader test using mocks.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ FilteredClassLoader.class, OzoneFSInputStream.class})
public class TestFilteredClassLoader {
  @Test
  public void testFilteredClassLoader() {
    PowerMockito.mockStatic(System.class);
    when(System.getenv("HADOOP_OZONE_DELEGATED_CLASSES"))
        .thenReturn("org.apache.hadoop.fs.ozone.OzoneFSInputStream");

    ClassLoader currentClassLoader =
        TestFilteredClassLoader.class.getClassLoader();

    List<URL> urls = new ArrayList<>();
    ClassLoader classLoader = new FilteredClassLoader(
        urls.toArray(new URL[0]), currentClassLoader);

    try {
      classLoader.loadClass(
          "org.apache.hadoop.fs.ozone.OzoneFSInputStream");
      ClassLoader expectedClassLoader =
          OzoneFSInputStream.class.getClassLoader();
      assertEquals(expectedClassLoader, currentClassLoader);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
}
