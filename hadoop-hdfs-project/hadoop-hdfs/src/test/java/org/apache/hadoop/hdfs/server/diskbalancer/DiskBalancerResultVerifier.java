/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Helps in verifying test results.
 */
public class DiskBalancerResultVerifier
    extends TypeSafeMatcher<DiskBalancerException> {
  private final DiskBalancerException.Result expectedResult;

  DiskBalancerResultVerifier(DiskBalancerException.Result expectedResult) {
    this.expectedResult = expectedResult;
  }

  @Override
  protected boolean matchesSafely(DiskBalancerException exception) {
    return (this.expectedResult == exception.getResult());
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("expects Result: ")
        .appendValue(this.expectedResult);

  }
}
