/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.viewfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.viewfs.InodeTree.SlashPath;

/**
 * Regex mount point is build to implement regex based mount point.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class RegexMountPoint<T> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(RegexMountPoint.class.getName());

  private InodeTree inodeTree;
  private String srcPathRegex;
  private Pattern srcPattern;
  private String dstPath;
  private String interceptorSettingsString;
  private List<RegexMountPointInterceptor> interceptorList;

  public static final String SETTING_SRCREGEX_SEP = "#.";
  public static final char INTERCEPTOR_SEP = ';';
  public static final char INTERCEPTOR_INTERNAL_SEP = ':';
  // ${var},$var
  public static final Pattern VAR_PATTERN_IN_DEST =
      Pattern.compile("\\$((\\{\\w+\\})|(\\w+))");

  // key => $key or key = > ${key}
  private Map<String, Set<String>> varInDestPathMap;

  public Map<String, Set<String>> getVarInDestPathMap() {
    return varInDestPathMap;
  }

  RegexMountPoint(InodeTree inodeTree, String sourcePathRegex,
      String destPath, String settingsStr) {
    this.inodeTree = inodeTree;
    this.srcPathRegex = sourcePathRegex;
    this.dstPath = destPath;
    this.interceptorSettingsString = settingsStr;
    this.interceptorList = new ArrayList<>();
  }

  /**
   * Initialize regex mount point.
   *
   * @throws IOException
   */
  public void initialize() throws IOException {
    try {
      srcPattern = Pattern.compile(srcPathRegex);
    } catch (PatternSyntaxException ex) {
      throw new IOException(
          "Failed to initialized mount point due to bad src path regex:"
              + srcPathRegex + ", dstPath:" + dstPath, ex);
    }
    varInDestPathMap = getVarListInString(dstPath);
    initializeInterceptors();
  }

  private void initializeInterceptors() throws IOException {
    if (interceptorSettingsString == null
        || interceptorSettingsString.isEmpty()) {
      return;
    }
    String[] interceptorStrArray =
        StringUtils.split(interceptorSettingsString, INTERCEPTOR_SEP);
    for (String interceptorStr : interceptorStrArray) {
      RegexMountPointInterceptor interceptor =
          RegexMountPointInterceptorFactory.create(interceptorStr);
      if (interceptor == null) {
        throw new IOException(
            "Illegal settings String " + interceptorSettingsString);
      }
      interceptor.initialize();
      interceptorList.add(interceptor);
    }
  }

  /**
   * Get $var1 and $var2 style variables in string.
   *
   * @param input
   * @return
   */
  public static Map<String, Set<String>> getVarListInString(String input) {
    Map<String, Set<String>> varMap = new HashMap<>();
    Matcher matcher = VAR_PATTERN_IN_DEST.matcher(input);
    while (matcher.find()) {
      // $var or ${var}
      String varName = matcher.group(0);
      // var or {var}
      String strippedVarName = matcher.group(1);
      if (strippedVarName.startsWith("{")) {
        // {varName} = > varName
        strippedVarName =
            strippedVarName.substring(1, strippedVarName.length() - 1);
      }
      varMap.putIfAbsent(strippedVarName, new HashSet<>());
      varMap.get(strippedVarName).add(varName);
    }
    return varMap;
  }

  public String getSrcPathRegex() {
    return srcPathRegex;
  }

  public Pattern getSrcPattern() {
    return srcPattern;
  }

  public String getDstPath() {
    return dstPath;
  }

  public static Pattern getVarPatternInDest() {
    return VAR_PATTERN_IN_DEST;
  }

  /**
   * Get resolved path from regex mount points.
   * @param srcPath
   * @param resolveLastComponent
   * @return
   */
  public InodeTree.ResolveResult<T> resolve(final String srcPath,
      final boolean resolveLastComponent) {
    String pathStrToResolve = srcPath;
    if (!resolveLastComponent) {
      int lastSlashIndex = srcPath.lastIndexOf(SlashPath.toString());
      if (lastSlashIndex == -1) {
        return null;
      }
      pathStrToResolve = srcPath.substring(0, lastSlashIndex);
    }
    for (RegexMountPointInterceptor interceptor : interceptorList) {
      // TODO: if the source interceptor change the pathStrToResolve
      pathStrToResolve = interceptor.interceptSource(pathStrToResolve);
    }
    LOGGER.debug("Path to resolve:" + pathStrToResolve + ",srcPattern:"
        + getSrcPathRegex());
    Matcher srcMatcher = getSrcPattern().matcher(pathStrToResolve);
    String parsedDestPath = getDstPath();
    int mappedCount = 0;
    String resolvedPathStr = "";
    while (srcMatcher.find()) {
      resolvedPathStr = pathStrToResolve.substring(0, srcMatcher.end());
      Map<String, Set<String>> varMap = getVarInDestPathMap();
      for (Map.Entry<String, Set<String>> entry : varMap.entrySet()) {
        String groupNameOrIndexStr = entry.getKey();
        String groupValue = null;
        if (groupNameOrIndexStr.matches("\\d+")) {
          // group index
          int groupIndex = Integer.parseUnsignedInt(groupNameOrIndexStr);
          if (groupIndex >= 0 && groupIndex <= srcMatcher.groupCount()) {
            groupValue = srcMatcher.group(groupIndex);
          }
        } else {
          // named group in regex
          groupValue = srcMatcher.group(groupNameOrIndexStr);
        }
        if (groupValue == null) {
          continue;
        }
        Set<String> varNameListToReplace = entry.getValue();
        for (String varName : varNameListToReplace) {
          parsedDestPath = parsedDestPath.replace(varName, groupValue);
          LOGGER.debug("parsedDestPath value is:" + parsedDestPath);
        }
      }
      ++mappedCount;
    }
    if (0 == mappedCount) {
      return null;
    }
    String remainingPathStr = srcPath.substring(resolvedPathStr.length());
    if (!remainingPathStr.startsWith("/")) {
      remainingPathStr = "/" + remainingPathStr;
    }
    Path remainingPath = new Path(remainingPathStr);
    for (RegexMountPointInterceptor interceptor : interceptorList) {
      parsedDestPath = interceptor.interceptResolvedDestPathStr(parsedDestPath);
      remainingPath =
          interceptor.interceptRemainingPath(remainingPath);
    }
    InodeTree.ResolveResult resolveResult = inodeTree
        .buildResolveResultForRegexMountPoint(InodeTree.ResultKind.EXTERNAL_DIR,
            resolvedPathStr, parsedDestPath, remainingPath);
    return resolveResult;
  }

  /**
   * Convert interceptor to string.
   *
   * @param interceptorList
   * @return
   */
  public static String convertInterceptorsToString(
      List<RegexMountPointInterceptor> interceptorList) {
    StringBuffer stringBuffer = new StringBuffer();
    for (int index = 0; index < interceptorList.size(); ++index) {
      stringBuffer.append(interceptorList.get(index).toString());
      if (index < interceptorList.size() - 1) {
        stringBuffer.append(INTERCEPTOR_SEP);
      }
    }
    return stringBuffer.toString();
  }
}
