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
package org.apache.hadoop.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Helper to configure FileSystemAccess user/group and proxyuser
 * configuration for testing using Java System properties.
 * <p/>
 * It uses the {@link SysPropsForTestsLoader} to load JavaSystem
 * properties for testing.
 */
public class HadoopUsersConfTestHelper {

  static {
    SysPropsForTestsLoader.init();
  }

  public static final String HADOOP_PROXYUSER = "test.hadoop.proxyuser";

  public static final String HADOOP_PROXYUSER_HOSTS = "test.hadoop.proxyuser.hosts";

  public static final String HADOOP_PROXYUSER_GROUPS = "test.hadoop.proxyuser.groups";

  public static final String HADOOP_USER_PREFIX = "test.hadoop.user.";

  /**
   * Returns a valid FileSystemAccess proxyuser for the FileSystemAccess cluster.
   * <p/>
   * The user is read from the Java System property
   * <code>test.hadoop.proxyuser</code> which defaults to the current user
   * (java System property <code>user.name</code>).
   * <p/>
   * This property should be set in the <code>test.properties</code> file.
   * <p/>
   * When running FileSystemAccess minicluster it is used to configure the FileSystemAccess minicluster.
   * <p/>
   * When using an external FileSystemAccess cluster, it is expected this property is set to
   * a valid proxy user.
   *
   * @return a valid FileSystemAccess proxyuser for the FileSystemAccess cluster.
   */
  public static String getHadoopProxyUser() {
    return System.getProperty(HADOOP_PROXYUSER, System.getProperty("user.name"));
  }

  /**
   * Returns the hosts for the FileSystemAccess proxyuser settings.
   * <p/>
   * The hosts are read from the Java System property
   * <code>test.hadoop.proxyuser.hosts</code> which defaults to <code>*</code>.
   * <p/>
   * This property should be set in the <code>test.properties</code> file.
   * <p/>
   * This property is ONLY used when running FileSystemAccess minicluster, it is used to
   * configure the FileSystemAccess minicluster.
   * <p/>
   * When using an external FileSystemAccess cluster this property is ignored.
   *
   * @return the hosts for the FileSystemAccess proxyuser settings.
   */
  public static String getHadoopProxyUserHosts() {
    return System.getProperty(HADOOP_PROXYUSER_HOSTS, "*");
  }

  /**
   * Returns the groups for the FileSystemAccess proxyuser settings.
   * <p/>
   * The hosts are read from the Java System property
   * <code>test.hadoop.proxyuser.groups</code> which defaults to <code>*</code>.
   * <p/>
   * This property should be set in the <code>test.properties</code> file.
   * <p/>
   * This property is ONLY used when running FileSystemAccess minicluster, it is used to
   * configure the FileSystemAccess minicluster.
   * <p/>
   * When using an external FileSystemAccess cluster this property is ignored.
   *
   * @return the groups for the FileSystemAccess proxyuser settings.
   */
  public static String getHadoopProxyUserGroups() {
    return System.getProperty(HADOOP_PROXYUSER_GROUPS, "*");
  }

  private static final String[] DEFAULT_USERS = new String[]{"user1", "user2"};
  private static final String[] DEFAULT_USERS_GROUP = new String[]{"group1", "supergroup"};

  /**
   * Returns the FileSystemAccess users to be used for tests. These users are defined
   * in the <code>test.properties</code> file in properties of the form
   * <code>test.hadoop.user.#USER#=#GROUP1#,#GROUP2#,...</code>.
   * <p/>
   * These properties are used to configure the FileSystemAccess minicluster user/group
   * information.
   * <p/>
   * When using an external FileSystemAccess cluster these properties should match the
   * user/groups settings in the cluster.
   *
   * @return the FileSystemAccess users used for testing.
   */
  public static String[] getHadoopUsers() {
    List<String> users = new ArrayList<String>();
    for (String name : System.getProperties().stringPropertyNames()) {
      if (name.startsWith(HADOOP_USER_PREFIX)) {
        users.add(name.substring(HADOOP_USER_PREFIX.length()));
      }
    }
    return (users.size() != 0) ? users.toArray(new String[users.size()]) : DEFAULT_USERS;
  }

  /**
   * Returns the groups a FileSystemAccess user belongs to during tests. These users/groups
   * are defined in the <code>test.properties</code> file in properties of the
   * form <code>test.hadoop.user.#USER#=#GROUP1#,#GROUP2#,...</code>.
   * <p/>
   * These properties are used to configure the FileSystemAccess minicluster user/group
   * information.
   * <p/>
   * When using an external FileSystemAccess cluster these properties should match the
   * user/groups settings in the cluster.
   *
   * @param user user name to get gropus.
   *
   * @return the groups of FileSystemAccess users used for testing.
   */
  public static String[] getHadoopUserGroups(String user) {
    if (getHadoopUsers() == DEFAULT_USERS) {
      for (String defaultUser : DEFAULT_USERS) {
        if (defaultUser.equals(user)) {
          return DEFAULT_USERS_GROUP;
        }
      }
      return new String[0];
    } else {
      String groups = System.getProperty(HADOOP_USER_PREFIX + user);
      return (groups != null) ? groups.split(",") : new String[0];
    }
  }

  public static Configuration getBaseConf() {
    Configuration conf = new Configuration();
    for (String name : System.getProperties().stringPropertyNames()) {
      conf.set(name, System.getProperty(name));
    }
    return conf;
  }

  public static void addUserConf(Configuration conf) {
    conf.set("hadoop.security.authentication", "simple");
    conf.set("hadoop.proxyuser." + HadoopUsersConfTestHelper.getHadoopProxyUser() + ".hosts",
             HadoopUsersConfTestHelper.getHadoopProxyUserHosts());
    conf.set("hadoop.proxyuser." + HadoopUsersConfTestHelper.getHadoopProxyUser() + ".groups",
             HadoopUsersConfTestHelper.getHadoopProxyUserGroups());

    for (String user : HadoopUsersConfTestHelper.getHadoopUsers()) {
      String[] groups = HadoopUsersConfTestHelper.getHadoopUserGroups(user);
      UserGroupInformation.createUserForTesting(user, groups);
    }
  }


}
