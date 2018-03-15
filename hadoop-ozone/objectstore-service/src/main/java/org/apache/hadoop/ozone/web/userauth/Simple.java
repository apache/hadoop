/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.web.userauth;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.client.rest.headers.Header;
import org.apache.hadoop.ozone.web.interfaces.UserAuth;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.security.UserGroupInformation;

import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.util.List;

/**
 * Simple is an UserAuth class that is used in the insecure
 * mode of ozone. This maps more or less to the simple user scheme in
 * HDFS.
 */
@InterfaceAudience.Private
public class Simple implements UserAuth {
  /**
   * Returns the x-ozone-user or the user on behalf of, This is
   * used in volume creation path.
   *
   * @param userArgs - UserArgs
   *
   * @throws OzoneException
   */
  @Override
  public String getOzoneUser(UserArgs userArgs) throws OzoneException {
    assert userArgs != null : "userArgs cannot be null";

    HttpHeaders headers = userArgs.getHeaders();
    List<String> users = headers.getRequestHeader(Header.OZONE_USER);

    if ((users == null) || (users.size() == 0)) {
      return null;
    }
    if (users.size() > 1) {
      throw ErrorTable.newError(ErrorTable.BAD_AUTHORIZATION, userArgs);
    }
    return users.get(0).toLowerCase().trim();
  }

  /**
   * Returns the user name as a string from the URI and HTTP headers.
   *
   * @param userArgs - user args
   *
   * @throws OzoneException -- Allows the underlying system
   * to throw, that error will get propagated to clients
   */
  @Override
  public String getUser(UserArgs userArgs) throws OzoneException {
    assert userArgs != null : "userArgs cannot be null";

    HttpHeaders headers = userArgs.getHeaders();
    List<String> users = headers.getRequestHeader(HttpHeaders.AUTHORIZATION);
    if (users == null || users.size() > 1) {
      throw ErrorTable.newError(ErrorTable.BAD_AUTHORIZATION, userArgs);
    }

    if (users.size() == 0) {
      return null;
    }

    String user = users.get(0).trim();
    if (user.startsWith(Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME)) {
      user = user.replace(Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME, "");
      return user.toLowerCase().trim();
    } else {
      throw ErrorTable.newError(ErrorTable.BAD_AUTHORIZATION, userArgs);
    }
  }


  /**
   * Returns true if a user is a Admin - {root and hdfs are treated as admins}.
   *
   * @param userArgs - User Args
   *
   * @throws OzoneException -- Allows the underlying system
   * to throw, that error will get propagated to clients
   */
  @Override
  public boolean isAdmin(UserArgs userArgs) throws OzoneException {
    assert userArgs != null : "userArgs cannot be null";

    String user;
    String currentUser;
    try {
      user = getUser(userArgs);
      currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      throw ErrorTable.newError(ErrorTable.BAD_AUTHORIZATION, userArgs);
    }
    return
        (user.compareToIgnoreCase(OzoneConsts.OZONE_SIMPLE_ROOT_USER) == 0) ||
            (user.compareToIgnoreCase(OzoneConsts.OZONE_SIMPLE_HDFS_USER) == 0)
            || (user.compareToIgnoreCase(currentUser) == 0);
  }

  /**
   * Returns true if the request is Anonymous.
   *
   * @param userArgs - user Args
   *
   * @throws OzoneException -- Allows the underlying system
   * to throw, that error will get propagated to clients
   */
  @Override
  public boolean isAnonymous(UserArgs userArgs) throws OzoneException {
    assert userArgs != null : "userArgs cannot be null";

    return getUser(userArgs) == null;
  }

  /**
   * Returns true if the name is a recognizable user in the system.
   *
   * @param userName - Name of the user
   * @param userArgs - user Args
   *
   * @throws OzoneException -- Allows the underlying system
   * to throw, that error will get propagated to clients
   */
  @Override
  public boolean isUser(String userName, UserArgs userArgs)
      throws OzoneException {
    // In the simple case, all non-null users names are users :)
    return userName != null;
  }

  /**
   * Returns all the Groups that user is a member of.
   *
   * @param userArgs - User Args
   *
   * @return String Array which contains 0 or more group names
   *
   * @throws OzoneException
   */
  @Override
  public String[] getGroups(UserArgs userArgs) throws OzoneException {
    // Not implemented
    return null;
  }

}
