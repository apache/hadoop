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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.SubsetConfiguration;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;

public class TestPatternFilter {

  /**
   * Filters should default to accept
   */
  @Test public void emptyConfigShouldAccept() {
    SubsetConfiguration empty = new ConfigBuilder().subset("");
    shouldAccept(empty, "anything");
    shouldAccept(empty, Arrays.asList(new MetricsTag("key", "desc", "value")));
  }

  /**
   * Filters should handle white-listing correctly
   */
  @Test public void includeOnlyShouldOnlyIncludeMatched() {
    SubsetConfiguration wl = new ConfigBuilder()
        .add("p.include", "foo")
        .add("p.include.tags", "foo:f").subset("p");
    shouldAccept(wl, "foo");
    shouldAccept(wl, Arrays.asList(new MetricsTag("bar", "", ""),
                                   new MetricsTag("foo", "", "f")));
    shouldReject(wl, "bar");
    shouldReject(wl, Arrays.asList(new MetricsTag("bar", "", "")));
    shouldReject(wl, Arrays.asList(new MetricsTag("foo", "", "boo")));
  }

  /**
   * Filters should handle black-listing correctly
   */
  @Test public void excludeOnlyShouldOnlyExcludeMatched() {
    SubsetConfiguration bl = new ConfigBuilder()
        .add("p.exclude", "foo")
        .add("p.exclude.tags", "foo:f").subset("p");
    shouldAccept(bl, "bar");
    shouldAccept(bl, Arrays.asList(new MetricsTag("bar", "", "")));
    shouldReject(bl, "foo");
    shouldReject(bl, Arrays.asList(new MetricsTag("bar", "", ""),
                                   new MetricsTag("foo", "", "f")));
  }

  /**
   * Filters should accepts unmatched item when both include and
   * exclude patterns are present.
   */
  @Test public void shouldAcceptUnmatchedWhenBothAreConfigured() {
    SubsetConfiguration c = new ConfigBuilder()
        .add("p.include", "foo")
        .add("p.include.tags", "foo:f")
        .add("p.exclude", "bar")
        .add("p.exclude.tags", "bar:b").subset("p");
    shouldAccept(c, "foo");
    shouldAccept(c, Arrays.asList(new MetricsTag("foo", "", "f")));
    shouldReject(c, "bar");
    shouldReject(c, Arrays.asList(new MetricsTag("bar", "", "b")));
    shouldAccept(c, "foobar");
    shouldAccept(c, Arrays.asList(new MetricsTag("foobar", "", "")));
  }

  /**
   * Include patterns should take precedence over exclude patterns
   */
  @Test public void includeShouldOverrideExclude() {
    SubsetConfiguration c = new ConfigBuilder()
        .add("p.include", "foo")
        .add("p.include.tags", "foo:f")
        .add("p.exclude", "foo")
        .add("p.exclude.tags", "foo:f").subset("p");
    shouldAccept(c, "foo");
    shouldAccept(c, Arrays.asList(new MetricsTag("foo", "", "f")));
  }

  static void shouldAccept(SubsetConfiguration conf, String s) {
    assertTrue("accepts "+ s, newGlobFilter(conf).accepts(s));
    assertTrue("accepts "+ s, newRegexFilter(conf).accepts(s));
  }

  static void shouldAccept(SubsetConfiguration conf, List<MetricsTag> tags) {
    assertTrue("accepts "+ tags, newGlobFilter(conf).accepts(tags));
    assertTrue("accepts "+ tags, newRegexFilter(conf).accepts(tags));
  }

  static void shouldReject(SubsetConfiguration conf, String s) {
    assertTrue("rejects "+ s, !newGlobFilter(conf).accepts(s));
    assertTrue("rejects "+ s, !newRegexFilter(conf).accepts(s));
  }

  static void shouldReject(SubsetConfiguration conf, List<MetricsTag> tags) {
    assertTrue("rejects "+ tags, !newGlobFilter(conf).accepts(tags));
    assertTrue("rejects "+ tags, !newRegexFilter(conf).accepts(tags));
  }

  /**
   * Create a new glob filter with a config object
   * @param conf  the config object
   * @return the filter
   */
  public static GlobFilter newGlobFilter(SubsetConfiguration conf) {
    GlobFilter f = new GlobFilter();
    f.init(conf);
    return f;
  }

  /**
   * Create a new regex filter with a config object
   * @param conf  the config object
   * @return the filter
   */
  public static RegexFilter newRegexFilter(SubsetConfiguration conf) {
    RegexFilter f = new RegexFilter();
    f.init(conf);
    return f;
  }
}
