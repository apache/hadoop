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
package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URL;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestFileSystemInitialization {

 /**
   * Check if FileSystem can be properly initialized if URLStreamHandlerFactory
   * is registered.
   */
  @Test
  public void testInitializationWithRegisteredStreamFactory() {
    Configuration conf = new Configuration();
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(conf));
    try {
      FileSystem.getFileSystemClass("file", conf);
    }
    catch (IOException ok) {
      // we might get an exception but this not related to infinite loop problem
      assertFalse(false);
    }
  }
}
