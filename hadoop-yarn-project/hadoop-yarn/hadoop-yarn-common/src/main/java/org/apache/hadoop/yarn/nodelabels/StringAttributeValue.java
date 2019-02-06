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

import java.io.IOException;

/**
 * Attribute value for String NodeAttributeType.
 */
public class StringAttributeValue implements AttributeValue {
  private String value = "";

  @Override
  public boolean compareForOperation(AttributeValue other,
      AttributeExpressionOperation op) {
    if (other instanceof StringAttributeValue) {
      StringAttributeValue otherString = (StringAttributeValue) other;
      switch (op) {
      case IN:
        return value.equals(otherString.value);
      case NOTIN:
        return !value.equals(otherString.value);
      default:
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public void validateAndInitializeValue(String valueStr) throws IOException {
    NodeLabelUtil.checkAndThrowAttributeValue(valueStr);
    this.value = valueStr;
  }

  @Override
  public String getValue() {
    return value;
  }

  public String toString() {
    return getValue();
  }
}
