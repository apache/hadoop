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

import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Interface for excluding files from DistCp.
 *
 */
public abstract class CopyFilter {

  private static final Logger LOG = LoggerFactory.getLogger(CopyFilter.class);

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
    String filtersClassName = conf
            .get(DistCpConstants.CONF_LABEL_FILTERS_CLASS);
    if (filtersClassName != null) {
      try {
        Class<? extends CopyFilter> filtersClass = conf
                .getClassByName(filtersClassName)
                .asSubclass(CopyFilter.class);
        filtersClassName = filtersClass.getName();
        Constructor<? extends CopyFilter> constructor = filtersClass
                .getDeclaredConstructor(Configuration.class);
        return constructor.newInstance(conf);
      } catch (Exception e) {
        LOG.error(DistCpConstants.CLASS_INSTANTIATION_ERROR_MSG +
                filtersClassName, e);
        throw new RuntimeException(
                DistCpConstants.CLASS_INSTANTIATION_ERROR_MSG +
                        filtersClassName, e);
      }
    } else {
      return getDefaultCopyFilter(conf);
    }
  }

  private static CopyFilter getDefaultCopyFilter(Configuration conf) {
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
