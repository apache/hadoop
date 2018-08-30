/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.interfaces;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

/**
 * Tests Handler interface.
 */
public class TestHandler {
  @Rule
  public TestRule timeout = new Timeout(300000);

  private Configuration conf;
  private HddsDispatcher dispatcher;
  private ContainerSet containerSet;
  private VolumeSet volumeSet;
  private Handler handler;

  @Before
  public void setup() throws Exception {
    this.conf = new Configuration();
    this.containerSet = Mockito.mock(ContainerSet.class);
    this.volumeSet = Mockito.mock(VolumeSet.class);

    this.dispatcher = new HddsDispatcher(conf, containerSet, volumeSet, null);
  }

  @Test
  public void testGetKeyValueHandler() throws Exception {
    Handler kvHandler = dispatcher.getHandler(
        ContainerProtos.ContainerType.KeyValueContainer);

    Assert.assertTrue("getHandlerForContainerType returned incorrect handler",
        (kvHandler instanceof KeyValueHandler));
  }

  @Test
  public void testGetHandlerForInvalidContainerType() {
    // When new ContainerProtos.ContainerType are added, increment the code
    // for invalid enum.
    ContainerProtos.ContainerType invalidContainerType =
        ContainerProtos.ContainerType.forNumber(2);

    Assert.assertEquals("New ContainerType detected. Not an invalid " +
        "containerType", invalidContainerType, null);

    Handler handler = dispatcher.getHandler(invalidContainerType);
    Assert.assertEquals("Get Handler for Invalid ContainerType should " +
        "return null.", handler, null);
  }
}
