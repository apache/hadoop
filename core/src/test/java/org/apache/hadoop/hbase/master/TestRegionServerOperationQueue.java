/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the queue used to manage RegionServerOperations.
 * Currently RegionServerOperationQueue is untestable because each
 * RegionServerOperation has a {@link HMaster} reference.  TOOD: Fix.
 */
public class TestRegionServerOperationQueue {
  private RegionServerOperationQueue queue;
  private Configuration conf;
  private AtomicBoolean closed;

  @Before
  public void setUp() throws Exception {
    this.closed = new AtomicBoolean(false);
    this.conf = new Configuration();
    this.queue = new RegionServerOperationQueue(this.conf, this.closed);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testNothing() throws Exception {
  }
}
