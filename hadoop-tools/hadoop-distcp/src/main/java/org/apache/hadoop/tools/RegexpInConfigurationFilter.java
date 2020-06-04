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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


/**
 * Implementation of regex based filter for DistCp.
 * {@link DistCpConstants#CONF_LABEL_FILTERS_CLASS} needs to be set
 * in {@link Configuration} when launching a distcp job.
 */
public class RegexpInConfigurationFilter extends CopyFilter {

  private static final Logger LOG = LoggerFactory
          .getLogger(RegexpInConfigurationFilter.class);

  /**
   * Regex which can used to filter source files.
   * {@link DistCpConstants#DISTCP_EXCLUDE_FILE_REGEX} can be set
   * in {@link Configuration} when launching a DistCp job.
   * If not set no files will be filtered.
   */
  private String excludeFileRegex;

  private List<Pattern> filters = new ArrayList<>();

  protected RegexpInConfigurationFilter(Configuration conf) {
    excludeFileRegex = conf
            .getTrimmed(DistCpConstants.DISTCP_EXCLUDE_FILE_REGEX, "");
    if (!excludeFileRegex.isEmpty()) {
      Pattern pattern = Pattern.compile(excludeFileRegex);
      filters.add(pattern);
    }
  }

  @Override
  public boolean shouldCopy(Path path) {
    for (Pattern filter : filters) {
      if (filter.matcher(path.toString()).matches()) {
        LOG.debug("Skipping {} as it matches the filter regex",
                path.toString());
        return false;
      }
    }
    return true;
  }
}
