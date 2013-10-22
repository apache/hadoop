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
import static org.mockito.Mockito.*;

import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;
import static org.apache.hadoop.metrics2.lib.Interns.*;

public class TestPatternFilter {

  /**
   * Filters should default to accept
   */
  @Test public void emptyConfigShouldAccept() {
    SubsetConfiguration empty = new ConfigBuilder().subset("");
    shouldAccept(empty, "anything");
    shouldAccept(empty, Arrays.asList(tag("key", "desc", "value")));
    shouldAccept(empty, mockMetricsRecord("anything", Arrays.asList(
      tag("key", "desc", "value"))));
  }

  /**
   * Filters should handle white-listing correctly
   */
  @Test public void includeOnlyShouldOnlyIncludeMatched() {
    SubsetConfiguration wl = new ConfigBuilder()
        .add("p.include", "foo")
        .add("p.include.tags", "foo:f").subset("p");
    shouldAccept(wl, "foo");
    shouldAccept(wl, Arrays.asList(tag("bar", "", ""),
                                   tag("foo", "", "f")), new boolean[] {false, true});
    shouldAccept(wl, mockMetricsRecord("foo", Arrays.asList(
      tag("bar", "", ""), tag("foo", "", "f"))));
    shouldReject(wl, "bar");
    shouldReject(wl, Arrays.asList(tag("bar", "", "")));
    shouldReject(wl, Arrays.asList(tag("foo", "", "boo")));
    shouldReject(wl, mockMetricsRecord("bar", Arrays.asList(
      tag("foo", "", "f"))));
    shouldReject(wl, mockMetricsRecord("foo", Arrays.asList(
      tag("bar", "", ""))));
  }

  /**
   * Filters should handle black-listing correctly
   */
  @Test public void excludeOnlyShouldOnlyExcludeMatched() {
    SubsetConfiguration bl = new ConfigBuilder()
        .add("p.exclude", "foo")
        .add("p.exclude.tags", "foo:f").subset("p");
    shouldAccept(bl, "bar");
    shouldAccept(bl, Arrays.asList(tag("bar", "", "")));
    shouldAccept(bl, mockMetricsRecord("bar", Arrays.asList(
      tag("bar", "", ""))));
    shouldReject(bl, "foo");
    shouldReject(bl, Arrays.asList(tag("bar", "", ""),
                                   tag("foo", "", "f")), new boolean[] {true, false});
    shouldReject(bl, mockMetricsRecord("foo", Arrays.asList(
      tag("bar", "", ""))));
    shouldReject(bl, mockMetricsRecord("bar", Arrays.asList(
      tag("bar", "", ""), tag("foo", "", "f"))));
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
    shouldAccept(c, Arrays.asList(tag("foo", "", "f")));
    shouldAccept(c, mockMetricsRecord("foo", Arrays.asList(
      tag("foo", "", "f"))));
    shouldReject(c, "bar");
    shouldReject(c, Arrays.asList(tag("bar", "", "b")));
    shouldReject(c, mockMetricsRecord("bar", Arrays.asList(
      tag("foo", "", "f"))));
    shouldReject(c, mockMetricsRecord("foo", Arrays.asList(
      tag("bar", "", "b"))));
    shouldAccept(c, "foobar");
    shouldAccept(c, Arrays.asList(tag("foobar", "", "")));
    shouldAccept(c, mockMetricsRecord("foobar", Arrays.asList(
      tag("foobar", "", ""))));
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
    shouldAccept(c, Arrays.asList(tag("foo", "", "f")));
    shouldAccept(c, mockMetricsRecord("foo", Arrays.asList(
      tag("foo", "", "f"))));
  }
  
  static void shouldAccept(SubsetConfiguration conf, String s) {
    assertTrue("accepts "+ s, newGlobFilter(conf).accepts(s));
    assertTrue("accepts "+ s, newRegexFilter(conf).accepts(s));
  }

  // Version for one tag:
  static void shouldAccept(SubsetConfiguration conf, List<MetricsTag> tags) {
    shouldAcceptImpl(true, conf, tags, new boolean[] {true});
  }
  // Version for multiple tags: 
  static void shouldAccept(SubsetConfiguration conf, List<MetricsTag> tags, 
      boolean[] expectedAcceptedSpec) {
    shouldAcceptImpl(true, conf, tags, expectedAcceptedSpec);
  }

  // Version for one tag:
  static void shouldReject(SubsetConfiguration conf, List<MetricsTag> tags) {
    shouldAcceptImpl(false, conf, tags, new boolean[] {false});
  }
  // Version for multiple tags: 
  static void shouldReject(SubsetConfiguration conf, List<MetricsTag> tags, 
      boolean[] expectedAcceptedSpec) {
    shouldAcceptImpl(false, conf, tags, expectedAcceptedSpec);
  }
  
  private static void shouldAcceptImpl(final boolean expectAcceptList,  
      SubsetConfiguration conf, List<MetricsTag> tags, boolean[] expectedAcceptedSpec) {
    final MetricsFilter globFilter = newGlobFilter(conf);
    final MetricsFilter regexFilter = newRegexFilter(conf);
    
    // Test acceptance of the tag list:  
    assertEquals("accepts "+ tags, expectAcceptList, globFilter.accepts(tags));
    assertEquals("accepts "+ tags, expectAcceptList, regexFilter.accepts(tags));
    
    // Test results on each of the individual tags:
    int acceptedCount = 0;
    for (int i=0; i<tags.size(); i++) {
      MetricsTag tag = tags.get(i);
      boolean actGlob = globFilter.accepts(tag);
      boolean actRegex = regexFilter.accepts(tag);
      assertEquals("accepts "+tag, expectedAcceptedSpec[i], actGlob);
      // Both the filters should give the same result:
      assertEquals(actGlob, actRegex);
      if (actGlob) {
        acceptedCount++;
      }
    }
    if (expectAcceptList) {
      // At least one individual tag should be accepted:
      assertTrue("No tag of the following accepted: " + tags, acceptedCount > 0);
    } else {
      // At least one individual tag should be rejected: 
      assertTrue("No tag of the following rejected: " + tags, acceptedCount < tags.size());
    }
  }

  /**
   * Asserts that filters with the given configuration accept the given record.
   * 
   * @param conf SubsetConfiguration containing filter configuration
   * @param record MetricsRecord to check
   */
  static void shouldAccept(SubsetConfiguration conf, MetricsRecord record) {
    assertTrue("accepts " + record, newGlobFilter(conf).accepts(record));
    assertTrue("accepts " + record, newRegexFilter(conf).accepts(record));
  }

  static void shouldReject(SubsetConfiguration conf, String s) {
    assertTrue("rejects "+ s, !newGlobFilter(conf).accepts(s));
    assertTrue("rejects "+ s, !newRegexFilter(conf).accepts(s));
  }

  /**
   * Asserts that filters with the given configuration reject the given record.
   * 
   * @param conf SubsetConfiguration containing filter configuration
   * @param record MetricsRecord to check
   */
  static void shouldReject(SubsetConfiguration conf, MetricsRecord record) {
    assertTrue("rejects " + record, !newGlobFilter(conf).accepts(record));
    assertTrue("rejects " + record, !newRegexFilter(conf).accepts(record));
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

  /**
   * Creates a mock MetricsRecord with the given name and tags.
   * 
   * @param name String name
   * @param tags List<MetricsTag> tags
   * @return MetricsRecord newly created mock
   */
  private static MetricsRecord mockMetricsRecord(String name,
      List<MetricsTag> tags) {
    MetricsRecord record = mock(MetricsRecord.class);
    when(record.name()).thenReturn(name);
    when(record.tags()).thenReturn(tags);
    return record;
  }
}
