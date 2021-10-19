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

import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.commons.configuration2.SubsetConfiguration;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsTag;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
/**
 * Base class for pattern based filters
 */
@InterfaceAudience.Private
public abstract class AbstractPatternFilter extends MetricsFilter {
  protected static final String INCLUDE_KEY = "include";
  protected static final String EXCLUDE_KEY = "exclude";
  protected static final String INCLUDE_TAGS_KEY = "include.tags";
  protected static final String EXCLUDE_TAGS_KEY = "exclude.tags";

  private Pattern includePattern;
  private Pattern excludePattern;
  private final Map<String, Pattern> includeTagPatterns;
  private final Map<String, Pattern> excludeTagPatterns;
  private final Pattern tagPattern = Pattern.compile("^(\\w+):(.*)");

  AbstractPatternFilter() {
    includeTagPatterns = Maps.newHashMap();
    excludeTagPatterns = Maps.newHashMap();
  }

  @Override
  public void init(SubsetConfiguration conf) {
    String patternString = conf.getString(INCLUDE_KEY);
    if (patternString != null && !patternString.isEmpty()) {
      setIncludePattern(compile(patternString));
    }
    patternString = conf.getString(EXCLUDE_KEY);
    if (patternString != null && !patternString.isEmpty()) {
      setExcludePattern(compile(patternString));
    }
    String[] patternStrings = conf.getStringArray(INCLUDE_TAGS_KEY);
    if (patternStrings != null && patternStrings.length != 0) {
      for (String pstr : patternStrings) {
        Matcher matcher = tagPattern.matcher(pstr);
        if (!matcher.matches()) {
          throw new MetricsException("Illegal tag pattern: "+ pstr);
        }
        setIncludeTagPattern(matcher.group(1), compile(matcher.group(2)));
      }
    }
    patternStrings = conf.getStringArray(EXCLUDE_TAGS_KEY);
    if (patternStrings != null && patternStrings.length != 0) {
      for (String pstr : patternStrings) {
        Matcher matcher = tagPattern.matcher(pstr);
        if (!matcher.matches()) {
          throw new MetricsException("Illegal tag pattern: "+ pstr);
        }
        setExcludeTagPattern(matcher.group(1), compile(matcher.group(2)));
      }
    }
  }

  void setIncludePattern(Pattern includePattern) {
    this.includePattern = includePattern;
  }

  void setExcludePattern(Pattern excludePattern) {
    this.excludePattern = excludePattern;
  }

  void setIncludeTagPattern(String name, Pattern pattern) {
    includeTagPatterns.put(name, pattern);
  }

  void setExcludeTagPattern(String name, Pattern pattern) {
    excludeTagPatterns.put(name, pattern);
  }

  @Override
  public boolean accepts(MetricsTag tag) {
    // Accept if whitelisted
    Pattern ipat = includeTagPatterns.get(tag.name());
    if (ipat != null && ipat.matcher(tag.value()).matches()) {
      return true;
    }
    // Reject if blacklisted
    Pattern epat = excludeTagPatterns.get(tag.name());
    if (epat != null && epat.matcher(tag.value()).matches()) {
      return false;
    }
    // Reject if no match in whitelist only mode
    if (!includeTagPatterns.isEmpty() && excludeTagPatterns.isEmpty()) {
      return false;
    }
    return true;
  }

  @Override
  public boolean accepts(Iterable<MetricsTag> tags) {
    // Accept if any include tag pattern matches
    for (MetricsTag t : tags) {
      Pattern pat = includeTagPatterns.get(t.name());
      if (pat != null && pat.matcher(t.value()).matches()) {
        return true;
      }
    }
    // Reject if any exclude tag pattern matches
    for (MetricsTag t : tags) {
      Pattern pat = excludeTagPatterns.get(t.name());
      if (pat != null && pat.matcher(t.value()).matches()) {
        return false;
      }
    }
    // Reject if no match in whitelist only mode
    if (!includeTagPatterns.isEmpty() && excludeTagPatterns.isEmpty()) {
      return false;
    }
    return true;
  }

  @Override
  public boolean accepts(String name) {
    // Accept if whitelisted
    if (includePattern != null && includePattern.matcher(name).matches()) {
      return true;
    }
    // Reject if blacklisted
    if ((excludePattern != null && excludePattern.matcher(name).matches())) {
      return false;
    }
    // Reject if no match in whitelist only mode
    if (includePattern != null && excludePattern == null) {
      return false;
    }
    return true;
  }

  /**
   * Compile a string pattern in to a pattern object
   * @param s the string pattern to compile
   * @return the compiled pattern object
   */
  protected abstract Pattern compile(String s);
}
