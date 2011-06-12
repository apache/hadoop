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

package org.apache.hadoop.fs.shell;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class TestCommandFactory {
  static CommandFactory factory;
  static Configuration conf = new Configuration();
  
  static void registerCommands(CommandFactory factory) {
  }
  
  @Before
  public void testSetup() {
    factory = new CommandFactory(conf);
    assertNotNull(factory);
  }
  
  @Test
  public void testRegistration() {
    assertArrayEquals(new String []{}, factory.getNames());

    factory.registerCommands(TestRegistrar.class);
    String [] names = factory.getNames();
    assertArrayEquals(new String []{"tc1", "tc2", "tc2.1"}, names);
    
    factory.addClass(TestCommand3.class, "tc3");
    names = factory.getNames();
    assertArrayEquals(new String []{"tc1", "tc2", "tc2.1", "tc3"}, names);
  }
  
  @Test
  public void testGetInstances() {
    factory.registerCommands(TestRegistrar.class);

    Command instance;
    instance = factory.getInstance("blarg");
    assertNull(instance);
    
    instance = factory.getInstance("tc1");
    assertNotNull(instance);
    assertEquals(TestCommand1.class, instance.getClass());
    assertEquals("tc1", instance.getCommandName());
    
    instance = factory.getInstance("tc2");
    assertNotNull(instance);
    assertEquals(TestCommand2.class, instance.getClass());
    assertEquals("tc2", instance.getCommandName());

    instance = factory.getInstance("tc2.1");
    assertNotNull(instance);
    assertEquals(TestCommand2.class, instance.getClass());    
    assertEquals("tc2.1", instance.getCommandName());
  }
  
  static class TestRegistrar {
    public static void registerCommands(CommandFactory factory) {
      factory.addClass(TestCommand1.class, "tc1");
      factory.addClass(TestCommand2.class, "tc2", "tc2.1");
    }
  }
  
  static class TestCommand1 extends FsCommand {}
  static class TestCommand2 extends FsCommand {}
  static class TestCommand3 extends FsCommand {}
}