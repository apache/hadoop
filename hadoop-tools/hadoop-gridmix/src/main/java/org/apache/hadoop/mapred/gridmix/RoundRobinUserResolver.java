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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class RoundRobinUserResolver implements UserResolver {
  public static final Log LOG = LogFactory.getLog(RoundRobinUserResolver.class);

  private int uidx = 0;
  private List<UserGroupInformation> users = Collections.emptyList();

  /**
   *  Mapping between user names of original cluster and UGIs of proxy users of
   *  simulated cluster
   */
  private final HashMap<String,UserGroupInformation> usercache =
      new HashMap<String,UserGroupInformation>();
  
  /**
   * Userlist assumes one user per line.
   * Each line in users-list-file is of the form &lt;username&gt;[,group]* 
   * <br> Group names are ignored(they are not parsed at all).
   */
  private List<UserGroupInformation> parseUserList(URI userUri, 
                                                   Configuration conf) 
  throws IOException {
    if (null == userUri) {
      return Collections.emptyList();
    }
    
    final Path userloc = new Path(userUri.toString());
    final Text rawUgi = new Text();
    final FileSystem fs = userloc.getFileSystem(conf);
    final ArrayList<UserGroupInformation> ugiList =
        new ArrayList<UserGroupInformation>();

    LineReader in = null;
    try {
      in = new LineReader(fs.open(userloc));
      while (in.readLine(rawUgi) > 0) {//line is of the form username[,group]*
        if(rawUgi.toString().trim().equals("")) {
          continue; //Continue on empty line
        }
        // e is end position of user name in this line
        int e = rawUgi.find(",");
        if (e == 0) {
          throw new IOException("Missing username: " + rawUgi);
        }
        if (e == -1) {
          e = rawUgi.getLength();
        }
        final String username = Text.decode(rawUgi.getBytes(), 0, e).trim();
        UserGroupInformation ugi = null;
        try {
          ugi = UserGroupInformation.createProxyUser(username,
                    UserGroupInformation.getLoginUser());
        } catch (IOException ioe) {
          LOG.error("Error while creating a proxy user " ,ioe);
        }
        if (ugi != null) {
          ugiList.add(ugi);
        }
        // No need to parse groups, even if they exist. Go to next line
      }
    } finally {
      if (in != null) {
        in.close();
      }
    }
    return ugiList;
  }

  @Override
  public synchronized boolean setTargetUsers(URI userloc, Configuration conf)
  throws IOException {
    uidx = 0;
    users = parseUserList(userloc, conf);
    if (users.size() == 0) {
      throw new IOException(buildEmptyUsersErrorMsg(userloc));
    }
    usercache.clear();
    return true;
  }

  static String buildEmptyUsersErrorMsg(URI userloc) {
    return "Empty user list is not allowed for RoundRobinUserResolver. Provided"
    + " user resource URI '" + userloc + "' resulted in an empty user list.";
  }

  @Override
  public synchronized UserGroupInformation getTargetUgi(
    UserGroupInformation ugi) {
    // UGI of proxy user
    UserGroupInformation targetUGI = usercache.get(ugi.getUserName());
    if (targetUGI == null) {
      targetUGI = users.get(uidx++ % users.size());
      usercache.put(ugi.getUserName(), targetUGI);
    }
    return targetUGI;
  }

  /**
   * {@inheritDoc}
   * <p>
   * {@link RoundRobinUserResolver} needs to map the users in the
   * trace to the provided list of target users. So user list is needed.
   */
  public boolean needsTargetUsersList() {
    return true;
  }
}