/*
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

package org.apache.hadoop.yarn.api.records.impl.pb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProto;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestApplicationSubmissionContextPBImpl {
  @Parameter
  @SuppressWarnings("checkstyle:visibilitymodifier")
  public ApplicationSubmissionContextPBImpl impl;

  @Test
  public void testAppTagsLowerCaseConversionDefault() {
    impl.setApplicationTags(Sets.newHashSet("ABcd", "efgH"));
    impl.getApplicationTags().forEach(s ->
        assertEquals(s, s.toLowerCase()));
  }

  @Test
  public void testAppTagsLowerCaseConversionDisabled() {
    ApplicationSubmissionContextPBImpl.setForceLowerCaseTags(false);
    impl.setApplicationTags(Sets.newHashSet("ABcd", "efgH"));
    impl.getApplicationTags().forEach(s ->
        assertNotEquals(s, s.toLowerCase()));
  }

  @Test
  public void testAppTagsLowerCaseConversionEnabled() {
    ApplicationSubmissionContextPBImpl.setForceLowerCaseTags(true);
    impl.setApplicationTags(Sets.newHashSet("ABcd", "efgH"));
    impl.getApplicationTags().forEach(s ->
        assertEquals(s, s.toLowerCase()));
  }

  @Parameters
  public static Collection<Object[]> data() {
    List<Object[]> list = new ArrayList<>();
    list.add(new Object[] {new ApplicationSubmissionContextPBImpl()});
    list.add(new Object[] {new ApplicationSubmissionContextPBImpl(
        ApplicationSubmissionContextProto.newBuilder().build())});

    return list;
  }
}
