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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Priority;

/**
 *
 * PriorityACLConfiguration class is used to parse Application Priority ACL
 * configuration from capcity-scheduler.xml
 */
public class AppPriorityACLConfigurationParser {

  private static final Log LOG = LogFactory
      .getLog(AppPriorityACLConfigurationParser.class);

  public enum AppPriorityACLKeyType {
    USER(1), GROUP(2), MAX_PRIORITY(3), DEFAULT_PRIORITY(4);

    private final int id;

    AppPriorityACLKeyType(int id) {
      this.id = id;
    }

    public int getId() {
      return this.id;
    }
  }

  public static final String PATTERN_FOR_PRIORITY_ACL = "\\[([^\\]]+)";

  @Private
  public static final String ALL_ACL = "*";

  @Private
  public static final String NONE_ACL = " ";

  public List<AppPriorityACLGroup> getPriorityAcl(Priority clusterMaxPriority,
      String aclString) {

    List<AppPriorityACLGroup> aclList = new ArrayList<AppPriorityACLGroup>();
    Matcher matcher = Pattern.compile(PATTERN_FOR_PRIORITY_ACL)
        .matcher(aclString);

    /*
     * Each ACL group will be separated by "[]". Syntax of each ACL group could
     * be like below "user=b1,b2 group=g1 max-priority=a2 default-priority=a1"
     * Ideally this means "for this given user/group, maximum possible priority
     * is a2 and if the user has not specified any priority, then it is a1."
     */
    while (matcher.find()) {
      // Get the first ACL sub-group.
      String aclSubGroup = matcher.group(1);
      if (aclSubGroup.trim().isEmpty()) {
        continue;
      }

      /*
       * Internal storage is PriorityACLGroup which stores each parsed priority
       * ACLs group. This will help while looking for a user to priority mapping
       * during app submission time. ACLs will be passed in below order only. 1.
       * user/group 2. max-priority 3. default-priority
       */
      AppPriorityACLGroup userPriorityACL = new AppPriorityACLGroup();

      // userAndGroupName will hold user acl and group acl as interim storage
      // since both user/group acl comes with separate key value pairs.
      List<StringBuilder> userAndGroupName = new ArrayList<>();

      for (String kvPair : aclSubGroup.trim().split(" +")) {
        /*
         * There are 3 possible options for key here: 1. user/group 2.
         * max-priority 3. default-priority
         */
        String[] splits = kvPair.split("=");

        // Ensure that each ACL sub string is key value pair separated by '='.
        if (splits != null && splits.length > 1) {
          parsePriorityACLType(userPriorityACL, splits, userAndGroupName);
        }
      }

      // If max_priority is higher to clusterMaxPriority, its better to
      // handle here.
      if (userPriorityACL.getMaxPriority().getPriority() > clusterMaxPriority
          .getPriority()) {
        LOG.warn("ACL configuration for '" + userPriorityACL.getMaxPriority()
            + "' is greater that cluster max priority. Resetting ACLs to "
            + clusterMaxPriority);
        userPriorityACL.setMaxPriority(
            Priority.newInstance(clusterMaxPriority.getPriority()));
      }

      AccessControlList acl = createACLStringForPriority(userAndGroupName);
      userPriorityACL.setACLList(acl);
      aclList.add(userPriorityACL);
    }

    return aclList;
  }

  /*
   * Parse different types of ACLs sub parts for on priority group and store in
   * a map for later processing.
   */
  private void parsePriorityACLType(AppPriorityACLGroup userPriorityACL,
      String[] splits, List<StringBuilder> userAndGroupName) {
    // Here splits will have the key value pair at index 0 and 1 respectively.
    // To parse all keys, its better to convert to PriorityACLConfig enum.
    AppPriorityACLKeyType aclType = AppPriorityACLKeyType
        .valueOf(StringUtils.toUpperCase(splits[0].trim()));
    switch (aclType) {
    case MAX_PRIORITY :
      userPriorityACL
          .setMaxPriority(Priority.newInstance(Integer.parseInt(splits[1])));
      break;
    case USER :
      userAndGroupName.add(getUserOrGroupACLStringFromConfig(splits[1]));
      break;
    case GROUP :
      userAndGroupName.add(getUserOrGroupACLStringFromConfig(splits[1]));
      break;
    case DEFAULT_PRIORITY :
      int defaultPriority = Integer.parseInt(splits[1]);
      Priority priority = (defaultPriority < 0)
          ? Priority.newInstance(0)
          : Priority.newInstance(defaultPriority);
      userPriorityACL.setDefaultPriority(priority);
      break;
    default:
      break;
    }
  }

  /*
   * This method will help to append different types of ACLs keys against one
   * priority. For eg,USER will be appended with GROUP as "user2,user4 group1".
   */
  private AccessControlList createACLStringForPriority(
      List<StringBuilder> acls) {

    String finalACL = "";
    String userACL = acls.get(0).toString();

    // If any of user/group is *, consider it as acceptable for all.
    // "user" is at index 0, and "group" is at index 1.
    if (userACL.trim().equals(ALL_ACL)) {
      finalACL = ALL_ACL;
    } else if (userACL.equals(NONE_ACL)) {
      finalACL = NONE_ACL;
    } else {

      // Get USER segment
      if (!userACL.trim().isEmpty()) {
        // skip last appended ","
        finalACL = acls.get(0).toString();
      }

      // Get GROUP segment if any
      if (acls.size() > 1) {
        String groupACL = acls.get(1).toString();
        if (!groupACL.trim().isEmpty()) {
          finalACL = finalACL + " "
              + acls.get(1).toString();
        }
      }
    }

    // Here ACL will look like "user1,user2 group" in ideal cases.
    return new AccessControlList(finalACL.trim());
  }

  /*
   * This method will help to append user/group acl string against given
   * priority. For example "user1,user2 group1,group2"
   */
  private StringBuilder getUserOrGroupACLStringFromConfig(String value) {

    // ACL strings could be generate for USER or GRUOP.
    // aclList in map contains two entries. 1. USER, 2. GROUP.
    StringBuilder aclTypeName = new StringBuilder();

    if (value.trim().equals(ALL_ACL)) {
      aclTypeName.setLength(0);
      aclTypeName.append(ALL_ACL);
      return aclTypeName;
    }

    aclTypeName.append(value.trim());
    return aclTypeName;
  }
}
