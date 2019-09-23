/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.tools;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test class to test Admin Helper.
 */
public class TestAdminHelper {

  @Test
  public void prettifyExceptionWithNpe() {
    String pretty = AdminHelper.prettifyException(new NullPointerException());
    Assert.assertTrue(
        "Prettified exception message doesn't contain the required exception "
            + "message",
        pretty.startsWith("NullPointerException at org.apache.hadoop.hdfs.tools"
            + ".TestAdminHelper.prettifyExceptionWithNpe"));
  }

  @Test
  public void prettifyException() {

    String pretty = AdminHelper.prettifyException(
        new IllegalArgumentException("Something is wrong",
            new IllegalArgumentException("Something is illegal")));

    Assert.assertEquals(
        "IllegalArgumentException: Something is wrong",
        pretty);

  }
}