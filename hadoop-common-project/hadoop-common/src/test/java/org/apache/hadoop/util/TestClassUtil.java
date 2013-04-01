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

package org.apache.hadoop.util;

import java.io.File;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;

public class TestClassUtil {
  @Test(timeout=1000)
  public void testFindContainingJar() {
    String containingJar = ClassUtil.findContainingJar(Logger.class);
    Assert.assertNotNull("Containing jar not found for Logger", 
        containingJar);
    File jarFile = new File(containingJar);
    Assert.assertTrue("Containing jar does not exist on file system", 
        jarFile.exists());
    Assert.assertTrue("Incorrect jar file" + containingJar,  
        jarFile.getName().matches("log4j.+[.]jar"));
  }
}
