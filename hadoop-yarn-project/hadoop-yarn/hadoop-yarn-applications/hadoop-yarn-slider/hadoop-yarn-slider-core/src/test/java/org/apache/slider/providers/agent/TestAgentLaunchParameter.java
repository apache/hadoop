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
package org.apache.slider.providers.agent;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class TestAgentLaunchParameter {
  protected static final Logger log =
      LoggerFactory.getLogger(TestAgentLaunchParameter.class);

  @Test
  public void testTestAgentLaunchParameter() throws Exception {
    AgentLaunchParameter alp = new AgentLaunchParameter("");
    Assert.assertEquals("", alp.getNextLaunchParameter("abc"));
    Assert.assertEquals("", alp.getNextLaunchParameter("HBASE_MASTER"));

    alp = new AgentLaunchParameter("a:1:2:3|b:5:6:NONE");
    Assert.assertEquals("1", alp.getNextLaunchParameter("a"));
    Assert.assertEquals("2", alp.getNextLaunchParameter("a"));
    Assert.assertEquals("3", alp.getNextLaunchParameter("a"));
    Assert.assertEquals("3", alp.getNextLaunchParameter("a"));

    Assert.assertEquals("5", alp.getNextLaunchParameter("b"));
    Assert.assertEquals("6", alp.getNextLaunchParameter("b"));
    Assert.assertEquals("", alp.getNextLaunchParameter("b"));
    Assert.assertEquals("", alp.getNextLaunchParameter("b"));
    Assert.assertEquals("", alp.getNextLaunchParameter("c"));

    alp = new AgentLaunchParameter("|a:1:3|b::5:NONE:");
    Assert.assertEquals("1", alp.getNextLaunchParameter("a"));
    Assert.assertEquals("3", alp.getNextLaunchParameter("a"));
    Assert.assertEquals("3", alp.getNextLaunchParameter("a"));

    Assert.assertEquals("", alp.getNextLaunchParameter("b"));
    Assert.assertEquals("5", alp.getNextLaunchParameter("b"));
    Assert.assertEquals("", alp.getNextLaunchParameter("b"));
    Assert.assertEquals("", alp.getNextLaunchParameter("b"));

    alp = new AgentLaunchParameter("|:");
    Assert.assertEquals("", alp.getNextLaunchParameter("b"));
    Assert.assertEquals("", alp.getNextLaunchParameter("a"));

    alp = new AgentLaunchParameter("HBASE_MASTER:a,b:DO_NOT_REGISTER:");
    Assert.assertEquals("a,b", alp.getNextLaunchParameter("HBASE_MASTER"));
    Assert.assertEquals("DO_NOT_REGISTER", alp.getNextLaunchParameter("HBASE_MASTER"));
    Assert.assertEquals("DO_NOT_REGISTER", alp.getNextLaunchParameter("HBASE_MASTER"));

    alp = new AgentLaunchParameter("HBASE_MASTER:a,b:DO_NOT_REGISTER::c:::");
    Assert.assertEquals("a,b", alp.getNextLaunchParameter("HBASE_MASTER"));
    Assert.assertEquals("DO_NOT_REGISTER", alp.getNextLaunchParameter("HBASE_MASTER"));
    Assert.assertEquals("", alp.getNextLaunchParameter("HBASE_MASTER"));
    Assert.assertEquals("c", alp.getNextLaunchParameter("HBASE_MASTER"));
    Assert.assertEquals("c", alp.getNextLaunchParameter("HBASE_MASTER"));
  }
}
