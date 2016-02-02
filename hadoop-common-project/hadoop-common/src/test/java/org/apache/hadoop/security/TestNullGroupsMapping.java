/*
 * Copyright 2015 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test that the {@link NullGroupsMapping} really does nothing.
 */
public class TestNullGroupsMapping {
  private NullGroupsMapping ngm;

  @Before
  public void setUp() {
    this.ngm = new NullGroupsMapping();
  }

  /**
   * Test of getGroups method, of class {@link NullGroupsMapping}.
   */
  @Test
  public void testGetGroups() {
    String user = "user";
    List<String> expResult = Collections.emptyList();
    List<String> result = ngm.getGroups(user);

    assertEquals("No groups should be returned",
        expResult, result);

    ngm.cacheGroupsAdd(Arrays.asList(new String[] {"group1", "group2"}));
    result = ngm.getGroups(user);

    assertEquals("No groups should be returned",
        expResult, result);

    ngm.cacheGroupsRefresh();
    result = ngm.getGroups(user);

    assertEquals("No groups should be returned",
        expResult, result);
  }
}
