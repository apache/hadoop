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

package org.apache.hadoop.service.launcher.testservices;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.launcher.AbstractLaunchableService;
import org.junit.Assert;

import java.util.List;

/**
 * Init in the constructor and make sure that it isn't inited again.
 */
public class InitInConstructorLaunchableService extends
    AbstractLaunchableService {

  public static final String NAME =
      "org.apache.hadoop.service.launcher.testservices.InitInConstructorLaunchableService";
  private final Configuration originalConf = new Configuration();

  public InitInConstructorLaunchableService() {
    super("InitInConstructorLaunchableService");
    init(originalConf);
  }

  @Override
  public void init(Configuration conf) {
    Assert.assertEquals(STATE.NOTINITED, getServiceState());
    super.init(conf);
  }

  @Override
  public Configuration bindArgs(Configuration config, List<String> args)
      throws Exception {
    Assert.assertEquals(STATE.INITED, getServiceState());
    Assert.assertTrue(isInState(STATE.INITED));
    Assert.assertNotSame(getConfig(), config);
    return null;
  }

  @Override
  public int execute() throws Exception {
    Assert.assertEquals(STATE.STARTED, getServiceState());
    Assert.assertSame(originalConf, getConfig());
    return super.execute();
  }
}
