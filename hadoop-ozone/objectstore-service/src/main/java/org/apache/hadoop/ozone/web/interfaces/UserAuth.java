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

package org.apache.hadoop.ozone.web.interfaces;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.handlers.UserArgs;

/**
 * This interface is used by Ozone to determine user identity.
 *
 * Please see concrete implementations for more information
 */
@InterfaceAudience.Private
public interface UserAuth {
  /**
   * Returns the user name as a string from the URI and HTTP headers.
   *
   * @param userArgs - userArgs
   *
   * @return String - User name
   *
   * @throws OzoneException
   */
  String getUser(UserArgs userArgs) throws OzoneException;

  /**
   * Returns all the Groups that user is a member of.
   *
   * @param userArgs - userArgs
   *
   * @return Array of Groups
   *
   * @throws OzoneException
   */
  String[] getGroups(UserArgs userArgs) throws OzoneException;

  /**
   * Returns true if a user is a Admin.
   *
   * @param userArgs - userArgs
   *
   * @return true if Admin , false otherwise
   *
   * @throws OzoneException -- Allows the underlying system
   * to throw, that error will get propagated to clients
   */
  boolean isAdmin(UserArgs userArgs) throws OzoneException;

  /**
   * Returns true if the request is Anonymous.
   *
   * @param userArgs - userArgs
   *
   * @return true if the request is anonymous, false otherwise.
   *
   * @throws OzoneException - Will be propagated back to end user
   */
  boolean isAnonymous(UserArgs userArgs) throws OzoneException;

  /**
   * Returns true if the name is a recognizable user in the system.
   *
   * @param userName - User Name to check
   * @param userArgs - userArgs
   *
   * @return true if the username string is the name of a valid user.
   *
   * @throws OzoneException - Will be propagated back to end user
   */
  boolean isUser(String userName, UserArgs userArgs) throws OzoneException;

  /**
   * Returns the x-ozone-user or the user on behalf of, This is
   * used in Volume creation path.
   *
   * @param userArgs - userArgs
   *
   * @return a user name if it has x-ozone-user args in header.
   *
   * @throws OzoneException
   */
  String getOzoneUser(UserArgs userArgs) throws OzoneException;

}
