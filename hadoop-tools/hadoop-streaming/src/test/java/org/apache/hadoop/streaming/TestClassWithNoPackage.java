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

package org.apache.hadoop.streaming;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.URL;
import java.net.URLClassLoader;

import org.apache.hadoop.util.JarFinder;
import org.junit.jupiter.api.Test;
import org.apache.hadoop.conf.Configuration;

/**
 * Test Hadoop StreamUtil successfully returns a class loaded by the job conf
 * but has no package name.
 */
public class TestClassWithNoPackage
{
  @Test
  public void testGoodClassOrNull() throws Exception {
    String NAME = "ClassWithNoPackage";
    ClassLoader cl = TestClassWithNoPackage.class.getClassLoader();
    String JAR = JarFinder.getJar(cl.loadClass(NAME));

    // Add testjob jar file to classpath.
    Configuration conf = new Configuration();
    conf.setClassLoader(new URLClassLoader(new URL[]{new URL("file", null, JAR)}, 
                                           null));
    // Get class with no package name.
    String defaultPackage = this.getClass().getPackage().getName();
    Class c = StreamUtil.goodClassOrNull(conf, NAME, defaultPackage);
    assertNotNull(c, "Class " + NAME + " not found!");
  }
  public static void main(String[]args) throws Exception
  {
    new TestClassWithNoPackage().testGoodClassOrNull();
  }

}
