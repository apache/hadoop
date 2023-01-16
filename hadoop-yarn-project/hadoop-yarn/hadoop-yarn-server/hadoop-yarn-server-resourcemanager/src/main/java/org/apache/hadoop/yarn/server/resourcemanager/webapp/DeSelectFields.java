/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.webapp.BadRequestException;

/**
 * DeSelectFields make the <code>/apps</code> api more flexible.
 * It can be used to strip off more fields if there's such use case in the future.
 * You can simply extend it via two steps:
 * <br> 1. add a <code>DeSelectType</code> enum with a string literals
 * <br> 2. write your logical based on
 * the return of method contains(DeSelectType)
 */
public class DeSelectFields {
  private static final Logger LOG =
      LoggerFactory.getLogger(DeSelectFields.class.getName());

  private final Set<DeSelectType> types;

  public DeSelectFields() {
    this.types = new HashSet<DeSelectType>();
  }

  /**
   * Initial DeSelectFields with unselected fields.
   * @param unselectedFields a set of unselected field.
   */
  public void initFields(Set<String> unselectedFields) {
    if (unselectedFields == null) {
      return;
    }
    for (String field : unselectedFields) {
      if (!field.trim().isEmpty()) {
        String[] literalsArray = field.split(",");
        for (String literals : literalsArray) {
          if (literals != null && !literals.trim().isEmpty()) {
            DeSelectType type = DeSelectType.obtainType(literals);
            if (type == null) {
              LOG.warn("Invalid deSelects string " + literals.trim());
              DeSelectType[] typeArray = DeSelectType.values();
              String allSupportLiterals = Arrays.toString(typeArray);
              throw new BadRequestException("Invalid deSelects string "
                  + literals.trim() + " specified. It should be one of "
                  + allSupportLiterals);
            } else {
              this.types.add(type);
            }
          }
        }
      }
    }
  }

  /**
   * Determine to deselect type should be handled or not.
   * @param type deselected type
   * @return true if the deselect type should be handled
   */
  public boolean contains(DeSelectType type) {
    return types.contains(type);
  }

  /**
   * Deselect field type, can be boosted in the future.
   */
  public enum DeSelectType {

    /**
     * <code>RESOURCE_REQUESTS</code> is the first
     * supported type from YARN-6280.
     */
    RESOURCE_REQUESTS("resourceRequests"),
    /**
     * <code>APP_TIMEOUTS, APP_NODE_LABEL_EXPRESSION, AM_NODE_LABEL_EXPRESSION,
     * RESOURCE_INFO</code> are additionally supported parameters added in
     * YARN-6871.
     */
    TIMEOUTS("timeouts"),
    APP_NODE_LABEL_EXPRESSION("appNodeLabelExpression"),
    AM_NODE_LABEL_EXPRESSION("amNodeLabelExpression"),
    RESOURCE_INFO("resourceInfo");

    private final String literals;

    DeSelectType(String literals) {
      this.literals = literals;
    }

    /**
     * use literals as toString.
     * @return the literals of this type.
     */
    @Override
    public String toString() {
      return literals;
    }

    /**
     * Obtain the <code>DeSelectType</code> by the literals given behind
     * <code>deSelects</code> in URL.
     * <br> e.g: deSelects="resourceRequests"
     * @param literals e.g: resourceRequests
     * @return <code>DeSelectType</code> e.g: DeSelectType.RESOURCE_REQUESTS
     */
    public static DeSelectType obtainType(String literals) {
      for (DeSelectType type : values()) {
        if (type.literals.equalsIgnoreCase(literals)) {
          return type;
        }
      }
      return null;
    }
  }
}