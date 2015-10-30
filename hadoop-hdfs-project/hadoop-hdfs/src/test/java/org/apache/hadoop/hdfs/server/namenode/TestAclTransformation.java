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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;
import static org.apache.hadoop.hdfs.server.namenode.AclTransformation.*;
import static org.junit.Assert.*;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.AclException;

/**
 * Tests operations that modify ACLs.  All tests in this suite have been
 * cross-validated against Linux setfacl/getfacl to check for consistency of the
 * HDFS implementation.
 */
public class TestAclTransformation {

  private static final List<AclEntry> ACL_SPEC_TOO_LARGE;
  private static final List<AclEntry> ACL_SPEC_DEFAULT_TOO_LARGE;
  static {
    ACL_SPEC_TOO_LARGE = Lists.newArrayListWithCapacity(33);
    ACL_SPEC_DEFAULT_TOO_LARGE = Lists.newArrayListWithCapacity(33);
    for (int i = 0; i < 33; ++i) {
      ACL_SPEC_TOO_LARGE.add(aclEntry(ACCESS, USER, "user" + i, ALL));
      ACL_SPEC_DEFAULT_TOO_LARGE.add(aclEntry(DEFAULT, USER, "user" + i, ALL));
    }
  }

  @Test
  public void testFilterAclEntriesByAclSpec() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
      .add(aclEntry(ACCESS, USER, "diana", READ_EXECUTE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, GROUP, "sales", READ_EXECUTE))
      .add(aclEntry(ACCESS, GROUP, "execs", READ_WRITE))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, READ))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "diana"),
      aclEntry(ACCESS, GROUP, "sales"));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, GROUP, "execs", READ_WRITE))
      .add(aclEntry(ACCESS, MASK, READ_WRITE))
      .add(aclEntry(ACCESS, OTHER, READ))
      .build();
    assertEquals(expected, filterAclEntriesByAclSpec(existing, aclSpec));
  }

  @Test
  public void testFilterAclEntriesByAclSpecUnchanged() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", ALL))
      .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .add(aclEntry(ACCESS, GROUP, "sales", ALL))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "clark"),
      aclEntry(ACCESS, GROUP, "execs"));
    assertEquals(existing, filterAclEntriesByAclSpec(existing, aclSpec));
  }

  @Test
  public void testFilterAclEntriesByAclSpecAccessMaskCalculated()
      throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ_WRITE))
      .add(aclEntry(ACCESS, OTHER, READ))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "diana"));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .build();
    assertEquals(expected, filterAclEntriesByAclSpec(existing, aclSpec));
  }

  @Test
  public void testFilterAclEntriesByAclSpecDefaultMaskCalculated()
      throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, USER, "diana", READ_WRITE))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ_WRITE))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "diana"));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, filterAclEntriesByAclSpec(existing, aclSpec));
  }

  @Test
  public void testFilterAclEntriesByAclSpecDefaultMaskPreserved()
      throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ_WRITE))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "diana", ALL))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "diana"));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "diana", ALL))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, filterAclEntriesByAclSpec(existing, aclSpec));
  }

  @Test
  public void testFilterAclEntriesByAclSpecAccessMaskPreserved()
      throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, USER, "diana", READ_WRITE))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ_WRITE))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "diana"));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, filterAclEntriesByAclSpec(existing, aclSpec));
  }

  @Test
  public void testFilterAclEntriesByAclSpecAutomaticDefaultUser()
      throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, READ_WRITE))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, filterAclEntriesByAclSpec(existing, aclSpec));
  }

  @Test
  public void testFilterAclEntriesByAclSpecAutomaticDefaultGroup()
      throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, READ_WRITE))
      .add(aclEntry(DEFAULT, GROUP, READ_WRITE))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, GROUP));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, READ_WRITE))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, filterAclEntriesByAclSpec(existing, aclSpec));
  }

  @Test
  public void testFilterAclEntriesByAclSpecAutomaticDefaultOther()
      throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, READ_WRITE))
      .add(aclEntry(DEFAULT, GROUP, READ_WRITE))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, OTHER));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, READ_WRITE))
      .add(aclEntry(DEFAULT, GROUP, READ_WRITE))
      .add(aclEntry(DEFAULT, OTHER, READ))
      .build();
    assertEquals(expected, filterAclEntriesByAclSpec(existing, aclSpec));
  }

  @Test
  public void testFilterAclEntriesByAclSpecEmptyAclSpec() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, ALL))
      .add(aclEntry(DEFAULT, OTHER, READ))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList();
    assertEquals(existing, filterAclEntriesByAclSpec(existing, aclSpec));
  }

  @Test(expected=AclException.class)
  public void testFilterAclEntriesByAclSpecRemoveAccessMaskRequired()
      throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, MASK));
    filterAclEntriesByAclSpec(existing, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testFilterAclEntriesByAclSpecRemoveDefaultMaskRequired()
      throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, ALL))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, MASK));
    filterAclEntriesByAclSpec(existing, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testFilterAclEntriesByAclSpecInputTooLarge() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    filterAclEntriesByAclSpec(existing, ACL_SPEC_TOO_LARGE);
  }

  @Test(expected = AclException.class)
  public void testFilterDefaultAclEntriesByAclSpecInputTooLarge()
      throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build();
    filterAclEntriesByAclSpec(existing, ACL_SPEC_DEFAULT_TOO_LARGE);
  }

  @Test
  public void testFilterDefaultAclEntries() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .add(aclEntry(ACCESS, GROUP, "sales", READ_EXECUTE))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, GROUP, "sales", READ_EXECUTE))
      .add(aclEntry(DEFAULT, MASK, READ_WRITE))
      .add(aclEntry(DEFAULT, OTHER, READ_EXECUTE))
      .build();
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .add(aclEntry(ACCESS, GROUP, "sales", READ_EXECUTE))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    assertEquals(expected, filterDefaultAclEntries(existing));
  }

  @Test
  public void testFilterDefaultAclEntriesUnchanged() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", ALL))
      .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .add(aclEntry(ACCESS, GROUP, "sales", ALL))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    assertEquals(existing, filterDefaultAclEntries(existing));
  }

  @Test
  public void testMergeAclEntries() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "bruce", ALL));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", ALL))
      .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    assertEquals(expected, mergeAclEntries(existing, aclSpec));
  }

  @Test
  public void testMergeAclEntriesUnchanged() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", ALL))
      .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .add(aclEntry(ACCESS, GROUP, "sales", ALL))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", ALL))
      .add(aclEntry(DEFAULT, GROUP, READ_EXECUTE))
      .add(aclEntry(DEFAULT, GROUP, "sales", ALL))
      .add(aclEntry(DEFAULT, MASK, ALL))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "bruce", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "sales", ALL),
      aclEntry(ACCESS, MASK, ALL),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "bruce", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, "sales", ALL),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE));
    assertEquals(existing, mergeAclEntries(existing, aclSpec));
  }

  @Test
  public void testMergeAclEntriesMultipleNewBeforeExisting()
      throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "diana", READ))
      .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .add(aclEntry(ACCESS, MASK, READ_EXECUTE))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, USER, "clark", READ_EXECUTE),
      aclEntry(ACCESS, USER, "diana", READ_EXECUTE));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ_EXECUTE))
      .add(aclEntry(ACCESS, USER, "clark", READ_EXECUTE))
      .add(aclEntry(ACCESS, USER, "diana", READ_EXECUTE))
      .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .add(aclEntry(ACCESS, MASK, READ_EXECUTE))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    assertEquals(expected, mergeAclEntries(existing, aclSpec));
  }

  @Test
  public void testMergeAclEntriesAccessMaskCalculated() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, USER, "diana", READ));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ_EXECUTE))
      .add(aclEntry(ACCESS, USER, "diana", READ))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ_EXECUTE))
      .add(aclEntry(ACCESS, OTHER, READ))
      .build();
    assertEquals(expected, mergeAclEntries(existing, aclSpec));
  }

  @Test
  public void testMergeAclEntriesDefaultMaskCalculated() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "bruce", READ_WRITE),
      aclEntry(DEFAULT, USER, "diana", READ_EXECUTE));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
      .add(aclEntry(DEFAULT, USER, "diana", READ_EXECUTE))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, ALL))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, mergeAclEntries(existing, aclSpec));
  }

  @Test
  public void testMergeAclEntriesDefaultMaskPreserved() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "diana", ALL))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "diana", FsAction.READ_EXECUTE));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "diana", READ_EXECUTE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ_EXECUTE))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "diana", ALL))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, mergeAclEntries(existing, aclSpec));
  }

  @Test
  public void testMergeAclEntriesAccessMaskPreserved() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, USER, "diana", READ_WRITE))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ_WRITE))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "diana", READ_EXECUTE));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, USER, "diana", READ_EXECUTE))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ_EXECUTE))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, mergeAclEntries(existing, aclSpec));
  }

  @Test
  public void testMergeAclEntriesAutomaticDefaultUser() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, READ));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, GROUP, READ_EXECUTE))
      .add(aclEntry(DEFAULT, OTHER, READ))
      .build();
    assertEquals(expected, mergeAclEntries(existing, aclSpec));
  }

  @Test
  public void testMergeAclEntriesAutomaticDefaultGroup() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, READ));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, READ_EXECUTE))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, OTHER, READ))
      .build();
    assertEquals(expected, mergeAclEntries(existing, aclSpec));
  }

  @Test
  public void testMergeAclEntriesAutomaticDefaultOther() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .add(aclEntry(DEFAULT, USER, READ_EXECUTE))
      .add(aclEntry(DEFAULT, GROUP, READ_EXECUTE))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, mergeAclEntries(existing, aclSpec));
  }

  @Test
  public void testMergeAclEntriesProvidedAccessMask() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, MASK, ALL));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ_EXECUTE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    assertEquals(expected, mergeAclEntries(existing, aclSpec));
  }

  @Test
  public void testMergeAclEntriesProvidedDefaultMask() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, ALL))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, mergeAclEntries(existing, aclSpec));
  }

  @Test
  public void testMergeAclEntriesEmptyAclSpec() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, ALL))
      .add(aclEntry(DEFAULT, OTHER, READ))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList();
    assertEquals(existing, mergeAclEntries(existing, aclSpec));
  }

  @Test(expected=AclException.class)
  public void testMergeAclEntriesInputTooLarge() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    mergeAclEntries(existing, ACL_SPEC_TOO_LARGE);
  }

  @Test(expected=AclException.class)
  public void testMergeAclDefaultEntriesInputTooLarge() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    mergeAclEntries(existing, ACL_SPEC_DEFAULT_TOO_LARGE);
  }

  @Test(expected=AclException.class)
  public void testMergeAclEntriesResultTooLarge() throws AclException {
    ImmutableList.Builder<AclEntry> aclBuilder =
      new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(ACCESS, USER, ALL));
    for (int i = 1; i <= 28; ++i) {
      aclBuilder.add(aclEntry(ACCESS, USER, "user" + i, READ));
    }
    aclBuilder
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ))
      .add(aclEntry(ACCESS, OTHER, NONE));
    List<AclEntry> existing = aclBuilder.build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "bruce", READ));
    mergeAclEntries(existing, aclSpec);
  }

  @Test(expected = AclException.class)
  public void testMergeAclDefaultEntriesResultTooLarge() throws AclException {
    ImmutableList.Builder<AclEntry> aclBuilder =
        new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(DEFAULT, USER, ALL));
    for (int i = 1; i <= 28; ++i) {
      aclBuilder.add(aclEntry(DEFAULT, USER, "user" + i, READ));
    }
    aclBuilder
    .add(aclEntry(DEFAULT, GROUP, READ))
    .add(aclEntry(DEFAULT, MASK, READ))
    .add(aclEntry(DEFAULT, OTHER, NONE));
    List<AclEntry> existing = aclBuilder.build();
    List<AclEntry> aclSpec = Lists.newArrayList(
         aclEntry(DEFAULT, USER, "bruce", READ));
    mergeAclEntries(existing, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testMergeAclEntriesDuplicateEntries() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "bruce", ALL),
      aclEntry(ACCESS, USER, "diana", READ_WRITE),
      aclEntry(ACCESS, USER, "clark", READ),
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE));
    mergeAclEntries(existing, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testMergeAclEntriesNamedMask() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, MASK, "bruce", READ_EXECUTE));
    mergeAclEntries(existing, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testMergeAclEntriesNamedOther() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, OTHER, "bruce", READ_EXECUTE));
    mergeAclEntries(existing, aclSpec);
  }

  @Test
  public void testReplaceAclEntries() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", ALL))
      .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "bruce", READ_WRITE),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "sales", ALL),
      aclEntry(ACCESS, MASK, ALL),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "bruce", READ_WRITE),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, "sales", ALL),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .add(aclEntry(ACCESS, GROUP, "sales", ALL))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ_WRITE))
      .add(aclEntry(DEFAULT, GROUP, READ_EXECUTE))
      .add(aclEntry(DEFAULT, GROUP, "sales", ALL))
      .add(aclEntry(DEFAULT, MASK, ALL))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, replaceAclEntries(existing, aclSpec));
  }

  @Test
  public void testReplaceAclEntriesUnchanged() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", ALL))
      .add(aclEntry(ACCESS, GROUP, READ_EXECUTE))
      .add(aclEntry(ACCESS, GROUP, "sales", ALL))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", ALL))
      .add(aclEntry(DEFAULT, GROUP, READ_EXECUTE))
      .add(aclEntry(DEFAULT, GROUP, "sales", ALL))
      .add(aclEntry(DEFAULT, MASK, ALL))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "bruce", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "sales", ALL),
      aclEntry(ACCESS, MASK, ALL),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "bruce", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, "sales", ALL),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE));
    assertEquals(existing, replaceAclEntries(existing, aclSpec));
  }

  @Test
  public void testReplaceAclEntriesAccessMaskCalculated() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "bruce", READ),
      aclEntry(ACCESS, USER, "diana", READ_WRITE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, READ));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ_WRITE))
      .add(aclEntry(ACCESS, OTHER, READ))
      .build();
    assertEquals(expected, replaceAclEntries(existing, aclSpec));
  }

  @Test
  public void testReplaceAclEntriesDefaultMaskCalculated() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, READ),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "bruce", READ),
      aclEntry(DEFAULT, USER, "diana", READ_WRITE),
      aclEntry(DEFAULT, GROUP, ALL),
      aclEntry(DEFAULT, OTHER, READ));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, USER, "diana", READ_WRITE))
      .add(aclEntry(DEFAULT, GROUP, ALL))
      .add(aclEntry(DEFAULT, MASK, ALL))
      .add(aclEntry(DEFAULT, OTHER, READ))
      .build();
    assertEquals(expected, replaceAclEntries(existing, aclSpec));
  }

  @Test
  public void testReplaceAclEntriesDefaultMaskPreserved() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ_WRITE))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "diana", ALL))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "bruce", READ),
      aclEntry(ACCESS, USER, "diana", READ_WRITE),
      aclEntry(ACCESS, GROUP, ALL),
      aclEntry(ACCESS, OTHER, READ));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, ALL))
      .add(aclEntry(ACCESS, MASK, ALL))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "diana", ALL))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, replaceAclEntries(existing, aclSpec));
  }

  @Test
  public void testReplaceAclEntriesAccessMaskPreserved() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, USER, "diana", READ_WRITE))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ_WRITE))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "bruce", READ),
      aclEntry(DEFAULT, GROUP, READ),
      aclEntry(DEFAULT, OTHER, NONE));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, USER, "bruce", READ))
      .add(aclEntry(ACCESS, USER, "diana", READ_WRITE))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, MASK, READ))
      .add(aclEntry(ACCESS, OTHER, READ))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, replaceAclEntries(existing, aclSpec));
  }

  @Test
  public void testReplaceAclEntriesAutomaticDefaultUser() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "bruce", READ),
      aclEntry(DEFAULT, GROUP, READ_WRITE),
      aclEntry(DEFAULT, MASK, READ_WRITE),
      aclEntry(DEFAULT, OTHER, READ));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, GROUP, READ_WRITE))
      .add(aclEntry(DEFAULT, MASK, READ_WRITE))
      .add(aclEntry(DEFAULT, OTHER, READ))
      .build();
    assertEquals(expected, replaceAclEntries(existing, aclSpec));
  }

  @Test
  public void testReplaceAclEntriesAutomaticDefaultGroup() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, READ_WRITE),
      aclEntry(DEFAULT, USER, "bruce", READ),
      aclEntry(DEFAULT, MASK, READ),
      aclEntry(DEFAULT, OTHER, READ));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .add(aclEntry(DEFAULT, USER, READ_WRITE))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, READ))
      .build();
    assertEquals(expected, replaceAclEntries(existing, aclSpec));
  }

  @Test
  public void testReplaceAclEntriesAutomaticDefaultOther() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, READ_WRITE),
      aclEntry(DEFAULT, USER, "bruce", READ),
      aclEntry(DEFAULT, GROUP, READ_WRITE),
      aclEntry(DEFAULT, MASK, READ_WRITE));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .add(aclEntry(DEFAULT, USER, READ_WRITE))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, GROUP, READ_WRITE))
      .add(aclEntry(DEFAULT, MASK, READ_WRITE))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, replaceAclEntries(existing, aclSpec));
  }

  @Test
  public void testReplaceAclEntriesOnlyDefaults() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "bruce", READ));
    List<AclEntry> expected = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, USER, "bruce", READ))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, MASK, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    assertEquals(expected, replaceAclEntries(existing, aclSpec));
  }

  @Test(expected=AclException.class)
  public void testReplaceAclEntriesInputTooLarge() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    replaceAclEntries(existing, ACL_SPEC_TOO_LARGE);
  }

  @Test(expected=AclException.class)
  public void testReplaceAclDefaultEntriesInputTooLarge() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    replaceAclEntries(existing, ACL_SPEC_DEFAULT_TOO_LARGE);
  }

  @Test(expected=AclException.class)
  public void testReplaceAclEntriesResultTooLarge() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayListWithCapacity(32);
    aclSpec.add(aclEntry(ACCESS, USER, ALL));
    for (int i = 1; i <= 29; ++i) {
      aclSpec.add(aclEntry(ACCESS, USER, "user" + i, READ));
    }
    aclSpec.add(aclEntry(ACCESS, GROUP, READ));
    aclSpec.add(aclEntry(ACCESS, OTHER, NONE));
    // The ACL spec now has 32 entries.  Automatic mask calculation will push it
    // over the limit to 33.
    replaceAclEntries(existing, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testReplaceAclEntriesDuplicateEntries() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "bruce", ALL),
      aclEntry(ACCESS, USER, "diana", READ_WRITE),
      aclEntry(ACCESS, USER, "clark", READ),
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    replaceAclEntries(existing, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testReplaceAclEntriesNamedMask() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(ACCESS, MASK, "bruce", READ_EXECUTE));
    replaceAclEntries(existing, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testReplaceAclEntriesNamedOther() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(ACCESS, OTHER, "bruce", READ_EXECUTE));
    replaceAclEntries(existing, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testReplaceAclEntriesMissingUser() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "bruce", READ_WRITE),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "sales", ALL),
      aclEntry(ACCESS, MASK, ALL),
      aclEntry(ACCESS, OTHER, NONE));
    replaceAclEntries(existing, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testReplaceAclEntriesMissingGroup() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "bruce", READ_WRITE),
      aclEntry(ACCESS, GROUP, "sales", ALL),
      aclEntry(ACCESS, MASK, ALL),
      aclEntry(ACCESS, OTHER, NONE));
    replaceAclEntries(existing, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testReplaceAclEntriesMissingOther() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(ACCESS, USER, ALL))
      .add(aclEntry(ACCESS, GROUP, READ))
      .add(aclEntry(ACCESS, OTHER, NONE))
      .build();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "bruce", READ_WRITE),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "sales", ALL),
      aclEntry(ACCESS, MASK, ALL));
    replaceAclEntries(existing, aclSpec);
  }
}
