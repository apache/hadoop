/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.security.access;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.SecureRpcEngine;
import org.apache.hadoop.hbase.security.User;

/**
 * Utility methods for testing security
 */
public class SecureTestUtil {
  public static void enableSecurity(Configuration conf) throws IOException {
    conf.set("hadoop.security.authorization", "false");
    conf.set("hadoop.security.authentication", "simple");
    conf.set("hbase.rpc.engine", SecureRpcEngine.class.getName());
    conf.set("hbase.coprocessor.master.classes", AccessController.class.getName());
    conf.set("hbase.coprocessor.region.classes", AccessController.class.getName());
    // add the process running user to superusers
    String currentUser = User.getCurrent().getName();
    conf.set("hbase.superuser", "admin,"+currentUser);
  }
}
