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

package org.apache.hadoop.fs.azurebfs.services;

/**
 * Test-Utility class to interact with the methods of AbfsClientThrottlingIntercept
 * which are exposed to test classes.
 * */
public final class AbfsClientThrottlingInterceptTestUtil {

  private AbfsClientThrottlingInterceptTestUtil() {}

  public static AbfsClientThrottlingIntercept get() {
    return AbfsClientThrottlingIntercept.getSingleton();
  }

  /**
   * Synchronized method to set readAnalyzer field in the instance of
   * AbfsClientThrottlingIntercept object. If the object already have a readAnalyser object of
   * MockAbfsClientThrottlingAnalyzer class, the method will not update the field.<br>
   * This method has to be synchronized as its going to check and set field on a
   * singleton object. If multiple test are run in parallel, without synchronized block,
   * it would result into consistency issue.
   *
   * @return readAnalyzer object in the provided AbfsClientThrottlingIntercept object.
   * */
  public static synchronized AbfsClientThrottlingAnalyzer setReadAnalyzer(final AbfsClientThrottlingIntercept intercept,
      final AbfsClientThrottlingAnalyzer abfsClientThrottlingAnalyzer) {
    if (intercept.getReadThrottler() != null
        && intercept.getReadThrottler().getClass()
        == MockAbfsClientThrottlingAnalyzer.class) {
      return intercept.getReadThrottler();
    }
    intercept.setReadThrottler(abfsClientThrottlingAnalyzer);
    return abfsClientThrottlingAnalyzer;
  }
}
