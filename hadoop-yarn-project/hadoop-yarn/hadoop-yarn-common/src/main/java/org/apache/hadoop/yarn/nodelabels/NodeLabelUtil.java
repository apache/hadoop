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
package org.apache.hadoop.yarn.nodelabels;

import com.google.common.base.Strings;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class for all NodeLabel and NodeAttribute operations.
 */
public final class NodeLabelUtil {
  private NodeLabelUtil() {
  }

  private static final int MAX_LABEL_LENGTH = 255;
  private static final Pattern LABEL_OR_VALUE_PATTERN =
      Pattern.compile("^[0-9a-zA-Z][0-9a-zA-Z-_]*");
  private static final Pattern PREFIX_PATTERN =
      Pattern.compile("^[0-9a-zA-Z][0-9a-zA-Z-_\\.]*");
  private static final Pattern ATTRIBUTE_VALUE_PATTERN =
      Pattern.compile("^[0-9a-zA-Z][0-9a-zA-Z-_.]*");

  public static void checkAndThrowLabelName(String label) throws IOException {
    if (label == null || label.isEmpty() || label.length() > MAX_LABEL_LENGTH) {
      throw new IOException("label added is empty or exceeds "
          + MAX_LABEL_LENGTH + " character(s)");
    }
    label = label.trim();

    boolean match = LABEL_OR_VALUE_PATTERN.matcher(label).matches();

    if (!match) {
      throw new IOException("label name should only contains "
          + "{0-9, a-z, A-Z, -, _} and should not started with {-,_}"
          + ", now it is= " + label);
    }
  }

  public static void checkAndThrowAttributeValue(String value)
      throws IOException {
    if (value == null) {
      return;
    } else if (value.trim().length() > MAX_LABEL_LENGTH) {
      throw new IOException("Attribute value added exceeds " + MAX_LABEL_LENGTH
          + " character(s)");

    }
    value = value.trim();
    if(value.isEmpty()) {
      return;
    }

    boolean match = ATTRIBUTE_VALUE_PATTERN.matcher(value).matches();

    if (!match) {
      throw new IOException("attribute value should only contains "
          + "{0-9, a-z, A-Z, -, _} and should not started with {-,_}"
          + ", now it is= " + value);
    }
  }

  public static void checkAndThrowAttributePrefix(String prefix)
      throws IOException {
    if (prefix == null) {
      throw new IOException("Attribute prefix cannot be null.");
    }
    if (prefix.trim().length() > MAX_LABEL_LENGTH) {
      throw new IOException("Attribute value added exceeds " + MAX_LABEL_LENGTH
          + " character(s)");
    }
    prefix = prefix.trim();
    if(prefix.isEmpty()) {
      return;
    }

    boolean match = PREFIX_PATTERN.matcher(prefix).matches();

    if (!match) {
      throw new IOException("attribute value should only contains "
          + "{0-9, a-z, A-Z, -, _,.} and should not started with {-,_}"
          + ", now it is= " + prefix);
    }
  }

  /**
   * Validate if a given set of attributes are valid. Attributes could be
   * invalid if any of following conditions is met:
   *
   * <ul>
   *   <li>Missing prefix: the attribute doesn't have prefix defined</li>
   *   <li>Malformed attribute prefix: the prefix is not in valid format</li>
   * </ul>
   * @param attributeSet
   * @throws IOException
   */
  public static void validateNodeAttributes(Set<NodeAttribute> attributeSet)
      throws IOException {
    if (attributeSet != null && !attributeSet.isEmpty()) {
      for (NodeAttribute nodeAttribute : attributeSet) {
        NodeAttributeKey attributeKey = nodeAttribute.getAttributeKey();
        if (attributeKey == null) {
          throw new IOException("AttributeKey  must be set");
        }
        String prefix = attributeKey.getAttributePrefix();
        if (Strings.isNullOrEmpty(prefix)) {
          throw new IOException("Attribute prefix must be set");
        }
        // Verify attribute prefix format.
        checkAndThrowAttributePrefix(prefix);
        // Verify attribute name format.
        checkAndThrowLabelName(attributeKey.getAttributeName());
      }
    }
  }

  /**
   * Filter a set of node attributes by a given prefix. Returns a filtered
   * set of node attributes whose prefix equals the given prefix.
   * If the prefix is null or empty, then the original set is returned.
   * @param attributeSet node attribute set
   * @param prefix node attribute prefix
   * @return a filtered set of node attributes
   */
  public static Set<NodeAttribute> filterAttributesByPrefix(
      Set<NodeAttribute> attributeSet, String prefix) {
    if (Strings.isNullOrEmpty(prefix)) {
      return attributeSet;
    }
    return attributeSet.stream()
        .filter(nodeAttribute -> prefix
            .equals(nodeAttribute.getAttributeKey().getAttributePrefix()))
        .collect(Collectors.toSet());
  }
}
