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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.LineReader;

/**
 * Maps users in the trace to a set of valid target users on the test cluster.
 */
public abstract class UserResolver {

  /**
   * Userlist assumes one UGI per line, each UGI matching
   * &lt;username&gt;,&lt;group&gt;[,group]*
   */
  protected List<UserGroupInformation> parseUserList(
      URI userUri, Configuration conf) throws IOException {
    if (null == userUri) {
      return Collections.emptyList();
    }
    final Path userloc = new Path(userUri.toString());
    final Text rawUgi = new Text();
    final FileSystem fs = userloc.getFileSystem(conf);
    final ArrayList<UserGroupInformation> ret = new ArrayList();

    LineReader in = null;
    try {
      final ArrayList<String> groups = new ArrayList();
      in = new LineReader(fs.open(userloc));
      while (in.readLine(rawUgi) > 0) {
        int e = rawUgi.find(",");
        if (e <= 0) {
          throw new IOException("Missing username: " + rawUgi);
        }
        final String username = Text.decode(rawUgi.getBytes(), 0, e);
        int s = e;
        while ((e = rawUgi.find(",", ++s)) != -1) {
          groups.add(Text.decode(rawUgi.getBytes(), s, e - s));
          s = e;
        }
        groups.add(Text.decode(rawUgi.getBytes(), s, rawUgi.getLength() - s));
        if (groups.size() == 0) {
          throw new IOException("Missing groups: " + rawUgi);
        }
        ret.add(new UnixUserGroupInformation(
              username, groups.toArray(new String[groups.size()])));
        groups.clear();
      }
    } finally {
      if (in != null) {
        in.close();
      }
    }
    return ret;
  }

  /**
   * Configure the user map given the URI and configuration. The resolver's
   * contract will define how the resource will be interpreted, but the default
   * will typically interpret the URI as a {@link org.apache.hadoop.fs.Path}
   * listing target users. The format of this file is defined by {@link
   * #parseUserList}.
   * @param userdesc URI (possibly null) from which user information may be
   * loaded per the subclass contract.
   * @param conf The tool configuration.
   * @return true if the resource provided was used in building the list of
   * target users
   */
  public abstract boolean setTargetUsers(URI userdesc, Configuration conf)
    throws IOException;

  // tmp compatibility hack prior to UGI from Rumen
  public UserGroupInformation getTargetUgi(String user)
      throws IOException {
    return getTargetUgi(new UnixUserGroupInformation(
          user, new String[] { "users" }));
  }

  /**
   * Map the given UGI to another per the subclass contract.
   * @param ugi User information from the trace.
   */
  public abstract UserGroupInformation getTargetUgi(UserGroupInformation ugi);

}
