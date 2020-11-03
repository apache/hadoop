/*
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

package org.apache.hadoop.registry.client.binding;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.exceptions.InvalidPathnameException;
import org.apache.hadoop.registry.client.impl.zk.RegistryInternalConstants;
import org.apache.hadoop.registry.server.dns.BaseServiceRecordProcessor;
import org.apache.zookeeper.common.PathUtils;

import java.net.IDN;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Basic operations on paths: manipulating them and creating and validating
 * path elements.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegistryPathUtils {

  /**
   * Compiled down pattern to validate single entries in the path
   */
  private static final Pattern PATH_ENTRY_VALIDATION_PATTERN =
      Pattern.compile(RegistryInternalConstants.VALID_PATH_ENTRY_PATTERN);

  private static final Pattern USER_NAME =
      Pattern.compile("/users/([a-z][a-z0-9-.]*)");

  /**
   * Validate ZK path with the path itself included in
   * the exception text
   * @param path path to validate
   * @return the path parameter
   * @throws InvalidPathnameException if the pathname is invalid.
   */
  public static String validateZKPath(String path) throws
      InvalidPathnameException {
    try {
      PathUtils.validatePath(path);

    } catch (IllegalArgumentException e) {
      throw new InvalidPathnameException(path,
          "Invalid Path \"" + path + "\" : " + e, e);
    }
    return path;
  }

  /**
   * Validate ZK path as valid for a DNS hostname.
   * @param path path to validate
   * @return the path parameter
   * @throws InvalidPathnameException if the pathname is invalid.
   */
  public static String validateElementsAsDNS(String path) throws
      InvalidPathnameException {
    List<String> splitpath = split(path);
    for (String fragment : splitpath) {
      if (!PATH_ENTRY_VALIDATION_PATTERN.matcher(fragment).matches()) {
        throw new InvalidPathnameException(path,
            "Invalid Path element \"" + fragment + "\"");
      }
    }
    return path;
  }

  /**
   * Create a full path from the registry root and the supplied subdir
   * @param path path of operation
   * @return an absolute path
   * @throws InvalidPathnameException if the path is invalid
   */
  public static String createFullPath(String base, String path) throws
      InvalidPathnameException {
    Preconditions.checkArgument(path != null, "null path");
    Preconditions.checkArgument(base != null, "null path");
    return validateZKPath(join(base, path));
  }

  /**
   * Join two paths, guaranteeing that there will not be exactly
   * one separator between the two, and exactly one at the front
   * of the path. There will be no trailing "/" except for the special
   * case that this is the root path
   * @param base base path
   * @param path second path to add
   * @return a combined path.
   */
  public static String join(String base, String path) {
    Preconditions.checkArgument(path != null, "null path");
    Preconditions.checkArgument(base != null, "null path");
    StringBuilder fullpath = new StringBuilder();

    if (!base.startsWith("/")) {
      fullpath.append('/');
    }
    fullpath.append(base);

    // guarantee a trailing /
    if (!fullpath.toString().endsWith("/")) {
      fullpath.append("/");
    }
    // strip off any at the beginning
    if (path.startsWith("/")) {
      // path starts with /, so append all other characters -if present
      if (path.length() > 1) {
        fullpath.append(path.substring(1));
      }
    } else {
      fullpath.append(path);
    }

    //here there may be a trailing "/"
    String finalpath = fullpath.toString();
    if (finalpath.endsWith("/") && !"/".equals(finalpath)) {
      finalpath = finalpath.substring(0, finalpath.length() - 1);

    }
    return finalpath;
  }

  /**
   * split a path into elements, stripping empty elements
   * @param path the path
   * @return the split path
   */
  public static List<String> split(String path) {
    //
    String[] pathelements = path.split("/");
    List<String> dirs = new ArrayList<String>(pathelements.length);
    for (String pathelement : pathelements) {
      if (!pathelement.isEmpty()) {
        dirs.add(pathelement);
      }
    }
    return dirs;
  }

  /**
   * Get the last entry in a path; for an empty path
   * returns "". The split logic is that of
   * {@link #split(String)}
   * @param path path of operation
   * @return the last path entry or "" if none.
   */
  public static String lastPathEntry(String path) {
    List<String> splits = split(path);
    if (splits.isEmpty()) {
      // empty path. Return ""
      return "";
    } else {
      return splits.get(splits.size() - 1);
    }
  }

  /**
   * Get the parent of a path
   * @param path path to look at
   * @return the parent path
   * @throws PathNotFoundException if the path was at root.
   */
  public static String parentOf(String path) throws PathNotFoundException {
    List<String> elements = split(path);

    int size = elements.size();
    if (size == 0) {
      throw new PathNotFoundException("No parent of " + path);
    }
    if (size == 1) {
      return "/";
    }
    elements.remove(size - 1);
    StringBuilder parent = new StringBuilder(path.length());
    for (String element : elements) {
      parent.append("/");
      parent.append(element);
    }
    return parent.toString();
  }

  /**
   * Perform any formatting for the registry needed to convert
   * non-simple-DNS elements
   * @param element element to encode
   * @return an encoded string
   */
  public static String encodeForRegistry(String element) {
    return IDN.toASCII(element);
  }

  /**
   * Perform whatever transforms are needed to get a YARN ID into
   * a DNS-compatible name
   * @param yarnId ID as string of YARN application, instance or container
   * @return a string suitable for use in registry paths.
   */
  public static String encodeYarnID(String yarnId) {
    return yarnId.replace("container", "ctr").replace("_", "-");
  }

  /**
   * Return the username found in the ZK path.
   *
   * @param recPath the ZK recPath.
   * @return the user name.
   */
  public static String getUsername(String recPath) {
    String user = "anonymous";
    Matcher matcher = USER_NAME.matcher(recPath);
    if (matcher.find()) {
      user = matcher.group(1);
    }
    return user;
  }
}
