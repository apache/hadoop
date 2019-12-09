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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Interface for excluding files from DistCp.
 *
 */
public abstract class CopyFilter {

  /**
   * Default initialize method does nothing.
   */
  public void initialize() {}

  /**
   * Predicate to determine if a file can be excluded from copy.
   *
   * @param path a Path to be considered for copying
   * @return boolean, true to copy, false to exclude
   */
  public abstract boolean shouldCopy(Path path);

  /**
   * Public factory method which returns the appropriate implementation of
   * CopyFilter.
   *
   * @param conf DistCp configuration
   * @return An instance of the appropriate CopyFilter
   */
  public static CopyFilter getCopyFilter(Configuration conf) {
    String filtersFilename = conf.get(DistCpConstants.CONF_LABEL_FILTERS_FILE);

    if (filtersFilename == null) {
      return new TrueCopyFilter();
    } else {
      String filterFilename = conf.get(
          DistCpConstants.CONF_LABEL_FILTERS_FILE);
      return new RegexCopyFilter(filterFilename);
    }
  }
}
