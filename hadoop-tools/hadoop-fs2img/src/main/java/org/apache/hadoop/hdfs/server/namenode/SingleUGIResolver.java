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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Map all owners/groups in external system to a single user in FSImage.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class SingleUGIResolver extends UGIResolver implements Configurable {

  public static final String UID   = "hdfs.image.writer.ugi.single.uid";
  public static final String USER  = "hdfs.image.writer.ugi.single.user";
  public static final String GID   = "hdfs.image.writer.ugi.single.gid";
  public static final String GROUP = "hdfs.image.writer.ugi.single.group";

  private int uid;
  private int gid;
  private String user;
  private String group;
  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    uid = conf.getInt(UID, 0);
    user = conf.get(USER);
    if (null == user) {
      try {
        user = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        user = "hadoop";
      }
    }
    gid = conf.getInt(GID, 1);
    group = conf.get(GROUP);
    if (null == group) {
      group = user;
    }

    resetUGInfo();
    addUser(user, uid);
    addGroup(group, gid);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public String user(String s) {
    return user;
  }

  @Override
  public String group(String s) {
    return group;
  }

  @Override
  public void addUser(String name) {
    // do nothing
  }

  @Override
  public void addGroup(String name) {
    // do nothing
  }
}
