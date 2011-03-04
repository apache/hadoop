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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UnixUserGroupInformation;

public class RoundRobinUserResolver extends UserResolver {

  private int uidx = 0;
  private List<UserGroupInformation> users = Collections.emptyList();
  private final HashMap<UserGroupInformation,UserGroupInformation> usercache =
    new HashMap<UserGroupInformation,UserGroupInformation>();

  public RoundRobinUserResolver() { }

  @Override
  public synchronized boolean setTargetUsers(URI userloc, Configuration conf)
      throws IOException {
    users = parseUserList(userloc, conf);
    if (users.size() == 0) {
      throw new IOException("Empty user list");
    }
    usercache.keySet().retainAll(users);
    return true;
  }

  @Override
  public synchronized UserGroupInformation getTargetUgi(
      UserGroupInformation ugi) {
    UserGroupInformation ret = usercache.get(ugi);
    if (null == ret) {
      ret = users.get(uidx++ % users.size());
      usercache.put(ugi, ret);
    }
    return ret;
  }

}
