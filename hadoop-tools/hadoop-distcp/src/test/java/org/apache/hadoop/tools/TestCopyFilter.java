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

package org.apache.hadoop.tools;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link CopyFilter}.
 */
public class TestCopyFilter {

  @Test
  public void testGetCopyFilterTrueCopyFilter() {
    Configuration configuration = new Configuration(false);
    CopyFilter copyFilter = CopyFilter.getCopyFilter(configuration);
    assertTrue("copyFilter should be instance of TrueCopyFilter",
            copyFilter instanceof TrueCopyFilter);
  }

  @Test
  public void testGetCopyFilterRegexCopyFilter() {
    Configuration configuration = new Configuration(false);
    configuration.set(DistCpConstants.CONF_LABEL_FILTERS_FILE, "random");
    CopyFilter copyFilter = CopyFilter.getCopyFilter(configuration);
    assertTrue("copyFilter should be instance of RegexCopyFilter",
            copyFilter instanceof RegexCopyFilter);
  }

  @Test
  public void testGetCopyFilterRegexpInConfigurationFilter() {
    final String filterName =
            "org.apache.hadoop.tools.RegexpInConfigurationFilter";
    Configuration configuration = new Configuration(false);
    configuration.set(DistCpConstants.CONF_LABEL_FILTERS_CLASS, filterName);
    CopyFilter copyFilter = CopyFilter.getCopyFilter(configuration);
    assertTrue("copyFilter should be instance of RegexpInConfigurationFilter",
            copyFilter instanceof RegexpInConfigurationFilter);
  }

  @Test
  public void testGetCopyFilterNonExistingClass() throws Exception {
    final String filterName =
            "org.apache.hadoop.tools.RegexpInConfigurationWrongFilter";
    Configuration configuration = new Configuration(false);
    configuration.set(DistCpConstants.CONF_LABEL_FILTERS_CLASS, filterName);
    intercept(RuntimeException.class,
        DistCpConstants.CLASS_INSTANTIATION_ERROR_MSG + filterName,
        () -> CopyFilter.getCopyFilter(configuration));
  }

  @Test
  public void testGetCopyFilterWrongClassType() throws Exception {
    final String filterName =
            "org.apache.hadoop.tools." +
                    "TestCopyFilter.FilterNotExtendingCopyFilter";
    Configuration configuration = new Configuration(false);
    configuration.set(DistCpConstants.CONF_LABEL_FILTERS_CLASS, filterName);
    intercept(RuntimeException.class,
        DistCpConstants.CLASS_INSTANTIATION_ERROR_MSG + filterName,
        () -> CopyFilter.getCopyFilter(configuration));
  }

  @Test
  public void testGetCopyFilterEmptyString() throws Exception {
    final String filterName = "";
    Configuration configuration = new Configuration(false);
    configuration.set(DistCpConstants.CONF_LABEL_FILTERS_CLASS, filterName);
    intercept(RuntimeException.class,
        DistCpConstants.CLASS_INSTANTIATION_ERROR_MSG + filterName,
        () -> CopyFilter.getCopyFilter(configuration));
  }

  private class FilterNotExtendingCopyFilter {

  }
}
