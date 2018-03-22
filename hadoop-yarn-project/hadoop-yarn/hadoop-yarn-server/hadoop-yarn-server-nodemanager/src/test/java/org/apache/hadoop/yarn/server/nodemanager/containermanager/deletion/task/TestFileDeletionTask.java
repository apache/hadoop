/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Test the attributes of the {@link FileDeletionTask} class.
 */
public class TestFileDeletionTask {

  private static final int ID = 0;
  private static final String USER = "user";
  private static final Path SUBDIR = new Path("subdir");
  private static final Path BASEDIR = new Path("basedir");

  private List<Path> baseDirs = new ArrayList<>();
  private DeletionService deletionService;
  private FileDeletionTask deletionTask;

  @Before
  public void setUp() throws Exception {
    deletionService = mock(DeletionService.class);
    baseDirs.add(BASEDIR);
    deletionTask = new FileDeletionTask(ID, deletionService, USER, SUBDIR,
        baseDirs);
  }

  @After
  public void tearDown() throws Exception {
    baseDirs.clear();
  }

  @Test
  public void testGetUser() throws Exception {
    assertEquals(USER, deletionTask.getUser());
  }

  @Test
  public void testGetSubDir() throws Exception {
    assertEquals(SUBDIR, deletionTask.getSubDir());
  }

  @Test
  public void testGetBaseDirs() throws Exception {
    assertEquals(1, deletionTask.getBaseDirs().size());
    assertEquals(baseDirs, deletionTask.getBaseDirs());
  }

  @Test
  public void testConvertDeletionTaskToProto() throws Exception {
    DeletionServiceDeleteTaskProto proto =
        deletionTask.convertDeletionTaskToProto();
    assertEquals(ID, proto.getId());
    assertEquals(USER, proto.getUser());
    assertEquals(SUBDIR, new Path(proto.getSubdir()));
    assertEquals(BASEDIR, new Path(proto.getBasedirs(0)));
    assertEquals(1, proto.getBasedirsCount());
  }
}