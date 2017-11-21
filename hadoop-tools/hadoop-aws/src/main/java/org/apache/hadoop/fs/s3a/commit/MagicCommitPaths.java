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

package org.apache.hadoop.fs.s3a.commit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.BASE;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC;
import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.E_NO_MAGIC_PATH_ELEMENT;

/**
 * Operations on (magic) paths.
 */
public final class MagicCommitPaths {

  private MagicCommitPaths() {
  }

  /**
   * Take an absolute path, split it into a list of elements.
   * If empty, the path is the root path.
   * @param path input path
   * @return a possibly empty list of elements.
   * @throws IllegalArgumentException if the path is invalid -relative, empty...
   */
  public static List<String> splitPathToElements(Path path) {
    checkArgument(path.isAbsolute(), "path is relative");
    String uriPath = path.toUri().getPath();
    checkArgument(!uriPath.isEmpty(), "empty path");
    if ("/".equals(uriPath)) {
      // special case: empty list
      return new ArrayList<>(0);
    }
    List<String> elements = new ArrayList<>();
    int len = uriPath.length();
    int firstElementChar = 1;
    int endOfElement = uriPath.indexOf('/', firstElementChar);
    while (endOfElement > 0) {
      elements.add(uriPath.substring(firstElementChar, endOfElement));
      firstElementChar = endOfElement + 1;
      endOfElement = firstElementChar == len ? -1
          : uriPath.indexOf('/', firstElementChar);
    }
    // expect a possible child element here
    if (firstElementChar != len) {
      elements.add(uriPath.substring(firstElementChar));
    }
    return elements;
  }

  /**
   * Is a path in the magic tree?
   * @param elements element list
   * @return true if a path is considered magic
   */
  public static boolean isMagicPath(List<String> elements) {
    return elements.contains(MAGIC);
  }

  /**
   * Does the list of magic elements contain a base path marker?
   * @param elements element list, already stripped out
   * from the magic tree.
   * @return true if a path has a base directory
   */
  public static boolean containsBasePath(List<String> elements) {
    return elements.contains(BASE);
  }

  /**
   * Get the index of the magic path element.
   * @param elements full path element list
   * @return the index.
   * @throws IllegalArgumentException if there is no magic element
   */
  public static int magicElementIndex(List<String> elements) {
    int index = elements.indexOf(MAGIC);
    checkArgument(index >= 0, E_NO_MAGIC_PATH_ELEMENT);
    return index;
  }

  /**
   * Get the parent path elements of the magic path.
   * The list may be immutable or may be a view of the underlying list.
   * Both the parameter list and the returned list MUST NOT be modified.
   * @param elements full path element list
   * @return the parent elements; may be empty
   */
  public static List<String> magicPathParents(List<String> elements) {
    return elements.subList(0, magicElementIndex(elements));
  }

  /**
   * Get the child path elements under the magic path.
   * The list may be immutable or may be a view of the underlying list.
   * Both the parameter list and the returned list MUST NOT be modified.
   * @param elements full path element list
   * @return the child elements; may be empty
   */
  public static List<String> magicPathChildren(List<String> elements) {
    int index = magicElementIndex(elements);
    int len = elements.size();
    if (index == len - 1) {
      // empty index
      return Collections.emptyList();
    } else {
      return elements.subList(index + 1, len);
    }
  }

  /**
   * Get any child path elements under any {@code __base} path,
   * or an empty list if there is either: no {@code __base} path element,
   * or no child entries under it.
   * The list may be immutable or may be a view of the underlying list.
   * Both the parameter list and the returned list MUST NOT be modified.
   * @param elements full path element list
   * @return the child elements; may be empty
   */
  public static List<String> basePathChildren(List<String> elements) {
    int index = elements.indexOf(BASE);
    if (index < 0) {
      return Collections.emptyList();
    }
    int len = elements.size();
    if (index == len - 1) {
      // empty index
      return Collections.emptyList();
    } else {
      return elements.subList(index + 1, len);
    }
  }

  /**
   * Take a list of elements and create an S3 key by joining them
   * with "/" between each one.
   * @param elements path elements
   * @return a path which can be used in the AWS API
   */
  public static String elementsToKey(List<String> elements) {
    return StringUtils.join("/", elements);
  }

  /**
   * Get the filename of a path: the last element.
   * @param elements element list.
   * @return the filename; the last element.
   */
  public static String filename(List<String> elements) {
    return lastElement(elements);
  }

  /**
   * Last element of a (non-empty) list.
   * @param strings strings in
   * @return the last one.
   */
  public static String lastElement(List<String> strings) {
    checkArgument(!strings.isEmpty(), "empty list");
    return strings.get(strings.size() - 1);
  }

  /**
   * Get the magic subdirectory of a destination directory.
   * @param destDir the destination directory
   * @return a new path.
   */
  public static Path magicSubdir(Path destDir) {
    return new Path(destDir, MAGIC);
  }

  /**
   * Calculates the final destination of a file.
   * This is the parent of any {@code __magic} element, and the filename
   * of the path. That is: all intermediate child path elements are discarded.
   * Why so? paths under the magic path include job attempt and task attempt
   * subdirectories, which need to be skipped.
   *
   * If there is a {@code __base} directory in the children, then it becomes
   * a base for unflattened paths, that is: all its children are pulled into
   * the final destination.
   * @param elements element list.
   * @return the path
   */
  public static List<String> finalDestination(List<String> elements) {
    if (isMagicPath(elements)) {
      List<String> destDir = magicPathParents(elements);
      List<String> children = magicPathChildren(elements);
      checkArgument(!children.isEmpty(), "No path found under " +
          MAGIC);
      ArrayList<String> dest = new ArrayList<>(destDir);
      if (containsBasePath(children)) {
        // there's a base marker in the path
        List<String> baseChildren = basePathChildren(children);
        checkArgument(!baseChildren.isEmpty(),
            "No path found under " + BASE);
        dest.addAll(baseChildren);
      } else {
        dest.add(filename(children));
      }
      return dest;
    } else {
      return elements;
    }
  }

}
