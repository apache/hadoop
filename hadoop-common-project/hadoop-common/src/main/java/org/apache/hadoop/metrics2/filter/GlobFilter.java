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

package org.apache.hadoop.metrics2.filter;

import com.google.re2j.Matcher;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.fs.GlobPattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.re2j.Pattern;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.util.StringUtils;

/**
 * A glob pattern filter for metrics.
 *
 * The class name is used in metrics config files
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class GlobFilter extends AbstractPatternFilter {

  @Override
  public void init(SubsetConfiguration conf) {
    // since HADOOP-16925, multi metrics are array, but not string
    // When we set GlobFilter pattern like {metrica, metricb}, patternStrings
    // will return ["{metrica", "metricb}"]
    String[] patternStrings = conf.getStringArray(INCLUDE_KEY);
    if (patternStrings != null && patternStrings.length != 0) {
      setIncludePattern(compile(formatGlobPattern(patternStrings)));
    }
    patternStrings = conf.getStringArray(EXCLUDE_KEY);
    if (patternStrings != null && patternStrings.length != 0) {
      setExcludePattern(compile(formatGlobPattern(patternStrings)));
    }
    patternStrings = conf.getStringArray(INCLUDE_TAGS_KEY);
    if (patternStrings != null && patternStrings.length != 0) {
      for (String pstr : patternStrings) {
        Matcher matcher = tagPattern.matcher(pstr);
        if (!matcher.matches()) {
          throw new MetricsException("Illegal tag pattern: " + pstr);
        }
        setIncludeTagPattern(matcher.group(1), compile(matcher.group(2)));
      }
    }
    patternStrings = conf.getStringArray(EXCLUDE_TAGS_KEY);
    if (patternStrings != null && patternStrings.length != 0) {
      for (String pstr : patternStrings) {
        Matcher matcher = tagPattern.matcher(pstr);
        if (!matcher.matches()) {
          throw new MetricsException("Illegal tag pattern: " + pstr);
        }
        setExcludeTagPattern(matcher.group(1), compile(matcher.group(2)));
      }
    }
  }

  private static String formatGlobPattern(String[] patternStrings) {
    StringBuilder builder = new StringBuilder("{");
    builder.append(StringUtils.join(",", patternStrings));
    builder.append("}");
    return builder.toString();
  }

  @Override
  protected Pattern compile(String s) {
    return GlobPattern.compile(s);
  }
}
