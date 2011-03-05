/*
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
package org.apache.hadoop.hbase.security;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

public class TestUser {
  @Test
  public void testBasicAttributes() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    User user = User.createUserForTesting(conf, "simple", new String[]{"foo"});
    assertEquals("Username should match", "simple", user.getName());
    assertEquals("Short username should match", "simple", user.getShortName());
    // don't test shortening of kerberos names because regular Hadoop doesn't support them
  }

  @Test
  public void testRunAs() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    final User user = User.createUserForTesting(conf, "testuser", new String[]{"foo"});
    final PrivilegedExceptionAction<String> action = new PrivilegedExceptionAction<String>(){
      public String run() throws IOException {
          User u = User.getCurrent();
          return u.getName();
      }
    };

    String username = user.runAs(action);
    assertEquals("Current user within runAs() should match",
        "testuser", username);

    // ensure the next run is correctly set
    User user2 = User.createUserForTesting(conf, "testuser2", new String[]{"foo"});
    String username2 = user2.runAs(action);
    assertEquals("Second username should match second user",
        "testuser2", username2);

    // check the exception version
    username = user.runAs(new PrivilegedExceptionAction<String>(){
      public String run() throws Exception {
        return User.getCurrent().getName();
      }
    });
    assertEquals("User name in runAs() should match", "testuser", username);

    // verify that nested contexts work
    user2.runAs(new PrivilegedExceptionAction(){
      public Object run() throws IOException, InterruptedException{
        String nestedName = user.runAs(action);
        assertEquals("Nest name should match nested user", "testuser", nestedName);
        assertEquals("Current name should match current user",
            "testuser2", User.getCurrent().getName());
        return null;
      }
    });
  }
}
