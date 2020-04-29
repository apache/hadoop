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

package org.apache.hadoop.registry.client.impl;

import org.apache.curator.framework.api.CuratorEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.registry.AbstractZKRegistryTest;
import org.apache.hadoop.registry.client.impl.zk.CuratorService;
import org.apache.hadoop.registry.client.impl.zk.RegistrySecurity;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Test the curator service
 */
public class TestCuratorService extends AbstractZKRegistryTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCuratorService.class);


  protected CuratorService curatorService;

  public static final String MISSING = "/missing";
  private List<ACL> rootACL;

  @Before
  public void startCurator() throws IOException {
    createCuratorService();
  }

  @After
  public void stopCurator() {
    ServiceOperations.stop(curatorService);
  }

  /**
   * Create an instance
   */
  protected void createCuratorService() throws IOException {
    curatorService = new CuratorService("curatorService");
    curatorService.init(createRegistryConfiguration());
    curatorService.start();
    rootACL = RegistrySecurity.WorldReadWriteACL;
    curatorService.maybeCreate("", CreateMode.PERSISTENT, rootACL, true);
  }

  @Test
  public void testLs() throws Throwable {
    curatorService.zkList("/");
  }

  @Test(expected = PathNotFoundException.class)
  public void testLsNotFound() throws Throwable {
    List<String> ls = curatorService.zkList(MISSING);
  }

  @Test
  public void testExists() throws Throwable {
    assertTrue(curatorService.zkPathExists("/"));
  }

  @Test
  public void testExistsMissing() throws Throwable {
    assertFalse(curatorService.zkPathExists(MISSING));
  }

  @Test
  public void testVerifyExists() throws Throwable {
    pathMustExist("/");
  }

  @Test(expected = PathNotFoundException.class)
  public void testVerifyExistsMissing() throws Throwable {
    pathMustExist("/file-not-found");
  }

  @Test
  public void testMkdirs() throws Throwable {
    mkPath("/p1", CreateMode.PERSISTENT);
    pathMustExist("/p1");
    mkPath("/p1/p2", CreateMode.EPHEMERAL);
    pathMustExist("/p1/p2");
  }

  private void mkPath(String path, CreateMode mode) throws IOException {
    curatorService.zkMkPath(path, mode, false,
        RegistrySecurity.WorldReadWriteACL);
  }

  public void pathMustExist(String path) throws IOException {
    curatorService.zkPathMustExist(path);
  }

  @Test(expected = PathNotFoundException.class)
  public void testMkdirChild() throws Throwable {
    mkPath("/testMkdirChild/child", CreateMode.PERSISTENT);
  }

  @Test
  public void testMaybeCreate() throws Throwable {
    assertTrue(curatorService.maybeCreate("/p3", CreateMode.PERSISTENT,
        RegistrySecurity.WorldReadWriteACL, false));
    assertFalse(curatorService.maybeCreate("/p3", CreateMode.PERSISTENT,
        RegistrySecurity.WorldReadWriteACL, false));
  }

  @Test
  public void testRM() throws Throwable {
    mkPath("/rm", CreateMode.PERSISTENT);
    curatorService.zkDelete("/rm", false, null);
    verifyNotExists("/rm");
    curatorService.zkDelete("/rm", false, null);
  }

  @Test
  public void testRMNonRf() throws Throwable {
    mkPath("/rm", CreateMode.PERSISTENT);
    mkPath("/rm/child", CreateMode.PERSISTENT);
    try {
      curatorService.zkDelete("/rm", false, null);
      fail("expected a failure");
    } catch (PathIsNotEmptyDirectoryException expected) {

    }
  }

  @Test
  public void testRMRf() throws Throwable {
    mkPath("/rm", CreateMode.PERSISTENT);
    mkPath("/rm/child", CreateMode.PERSISTENT);
    curatorService.zkDelete("/rm", true, null);
    verifyNotExists("/rm");
    curatorService.zkDelete("/rm", true, null);
  }


  @Test
  public void testBackgroundDelete() throws Throwable {
    mkPath("/rm", CreateMode.PERSISTENT);
    mkPath("/rm/child", CreateMode.PERSISTENT);
    CuratorEventCatcher events = new CuratorEventCatcher();
    curatorService.zkDelete("/rm", true, events);
    CuratorEvent taken = events.take();
    LOG.info("took {}", taken);
    assertEquals(1, events.getCount());
  }

  @Test
  public void testCreate() throws Throwable {

    curatorService.zkCreate("/testcreate",
        CreateMode.PERSISTENT, getTestBuffer(),
        rootACL
    );
    pathMustExist("/testcreate");
  }

  @Test
  public void testCreateTwice() throws Throwable {
    byte[] buffer = getTestBuffer();
    curatorService.zkCreate("/testcreatetwice",
        CreateMode.PERSISTENT, buffer,
        rootACL);
    try {
      curatorService.zkCreate("/testcreatetwice",
          CreateMode.PERSISTENT, buffer,
          rootACL);
      fail();
    } catch (FileAlreadyExistsException e) {

    }
  }

  @Test
  public void testCreateUpdate() throws Throwable {
    byte[] buffer = getTestBuffer();
    curatorService.zkCreate("/testcreateupdate",
        CreateMode.PERSISTENT, buffer,
        rootACL
    );
    curatorService.zkUpdate("/testcreateupdate", buffer);
  }

  @Test(expected = PathNotFoundException.class)
  public void testUpdateMissing() throws Throwable {
    curatorService.zkUpdate("/testupdatemissing", getTestBuffer());
  }

  @Test
  public void testUpdateDirectory() throws Throwable {
    mkPath("/testupdatedirectory", CreateMode.PERSISTENT);
    curatorService.zkUpdate("/testupdatedirectory", getTestBuffer());
  }

  @Test
  public void testUpdateDirectorywithChild() throws Throwable {
    mkPath("/testupdatedirectorywithchild", CreateMode.PERSISTENT);
    mkPath("/testupdatedirectorywithchild/child", CreateMode.PERSISTENT);
    curatorService.zkUpdate("/testupdatedirectorywithchild", getTestBuffer());
  }

  @Test
  public void testUseZKServiceForBinding() throws Throwable {
    CuratorService cs2 = new CuratorService("curator", zookeeper);
    cs2.init(new Configuration());
    cs2.start();
  }

  protected byte[] getTestBuffer() {
    byte[] buffer = new byte[1];
    buffer[0] = '0';
    return buffer;
  }


  public void verifyNotExists(String path) throws IOException {
    if (curatorService.zkPathExists(path)) {
      fail("Path should not exist: " + path);
    }
  }
}
