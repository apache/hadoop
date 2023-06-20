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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class that parses {@code QueueCapacityVector} from the capacity
 * configuration property set for a queue.
 *
 * A new syntax for capacity property could be implemented, by creating a parser
 * with a regex to match the pattern and a method that creates a
 * {@code QueueCapacityVector} from the matched pattern.
 * Extending the parsers field with a {@code Parser} object in the constructor
 * is needed in this case.
 *
 * A new capacity type for the existing parsers could be added by extending
 * the {@code QueueCapacityVector.QueueCapacityType} with a new type and its
 * associated postfix symbol.
 */
public class QueueCapacityConfigParser {
  private static final String UNIFORM_REGEX = "^([0-9.]+)(.*)";
  private static final String RESOURCE_REGEX = "^\\[([\\w\\.,\\-_%\\ /]+=[\\w\\.,\\-_%\\ /]+)+\\]$";

  private static final Pattern RESOURCE_PATTERN = Pattern.compile(RESOURCE_REGEX);
  private static final Pattern UNIFORM_PATTERN = Pattern.compile(UNIFORM_REGEX);
  public static final String FLOAT_DIGIT_REGEX = "[0-9.]";

  private final List<Parser> parsers = new ArrayList<>();

  public QueueCapacityConfigParser() {
    parsers.add(new Parser(RESOURCE_PATTERN, this::heterogeneousParser));
    parsers.add(new Parser(UNIFORM_PATTERN, this::uniformParser));
  }

  /**
   * Creates a {@code QueueCapacityVector} parsed from the capacity configuration
   * property set for a queue.
   * @param capacityString capacity string to parse
   * @param queuePath queue for which the capacity property is parsed
   * @return a parsed capacity vector
   */
  public QueueCapacityVector parse(String capacityString, String queuePath) {

    if (queuePath.equals(CapacitySchedulerConfiguration.ROOT)) {
      return QueueCapacityVector.of(100f, ResourceUnitCapacityType.PERCENTAGE);
    }

    if (capacityString == null) {
      return new QueueCapacityVector();
    }
    // Trim all spaces from capacity string
    capacityString = capacityString.replaceAll(" ", "");

    for (Parser parser : parsers) {
      Matcher matcher = parser.regex.matcher(capacityString);
      if (matcher.find()) {
        return parser.parser.apply(matcher);
      }
    }

    return new QueueCapacityVector();
  }

  /**
   * A parser method that is usable on uniform capacity values e.g. percentage or
   * weight.
   * @param matcher a regex matcher that contains parsed value and its possible
   *                suffix
   * @return a parsed capacity vector
   */
  private QueueCapacityVector uniformParser(Matcher matcher) {
    ResourceUnitCapacityType capacityType = null;
    String value = matcher.group(1);
    if (matcher.groupCount() == 2) {
      String matchedSuffix = matcher.group(2);
      for (ResourceUnitCapacityType suffix : ResourceUnitCapacityType.values()) {
        // Absolute uniform syntax is not supported
        if (suffix.equals(ResourceUnitCapacityType.ABSOLUTE)) {
          continue;
        }
        // when capacity is given in percentage, we do not need % symbol
        String uniformSuffix = suffix.getPostfix().replaceAll("%", "");
        if (uniformSuffix.equals(matchedSuffix)) {
          capacityType = suffix;
        }
      }
    }

    if (capacityType == null) {
      return new QueueCapacityVector();
    }

    return QueueCapacityVector.of(Float.parseFloat(value), capacityType);
  }

  /**
   * A parser method that is usable on resource capacity values e.g. mixed or
   * absolute resource.
   * @param matcher a regex matcher that contains the matched resource string
   * @return a parsed capacity vector
   */
  private QueueCapacityVector heterogeneousParser(Matcher matcher) {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();

    /*
     * Absolute resource configuration for a queue will be grouped by "[]".
     * Syntax of absolute resource config could be like below
     * "memory=4Gi vcores=2". Ideally this means "4GB of memory and 2 vcores".
     */
    // Get the sub-group.
    String bracketedGroup = matcher.group(0);
    // Get the string inside starting and closing []
    bracketedGroup = bracketedGroup.substring(1, bracketedGroup.length() - 1);
    // Split by comma and equals delimiter eg. the string memory=1024,vcores=6
    // is converted to an array of array as {{memory,1024}, {vcores, 6}}
    for (String kvPair : bracketedGroup.trim().split(",")) {
      String[] splits = kvPair.split("=");

      // Ensure that each sub string is key value pair separated by '='.
      if (splits.length > 1) {
        setCapacityVector(capacityVector, splits[0], splits[1]);
      }
    }

    // Memory always have to be defined
    if (capacityVector.getMemory() == 0L) {
      return new QueueCapacityVector();
    }

    return capacityVector;
  }

  private void setCapacityVector(
      QueueCapacityVector resource, String resourceName, String resourceValue) {
    ResourceUnitCapacityType capacityType = ResourceUnitCapacityType.ABSOLUTE;

    // Extract suffix from a value e.g. for 6w extract w
    String suffix = resourceValue.replaceAll(FLOAT_DIGIT_REGEX, "");
    if (!resourceValue.endsWith(suffix)) {
      return;
    }

    float parsedResourceValue = Float.parseFloat(resourceValue.substring(
        0, resourceValue.length() - suffix.length()));
    float convertedValue = parsedResourceValue;

    if (!suffix.isEmpty() && UnitsConversionUtil.KNOWN_UNITS.contains(suffix)) {
      // Convert all incoming units to MB if units is configured.
      convertedValue = UnitsConversionUtil.convert(suffix, "Mi", (long) parsedResourceValue);
    } else {
      for (ResourceUnitCapacityType capacityTypeSuffix : ResourceUnitCapacityType.values()) {
        if (capacityTypeSuffix.getPostfix().equals(suffix)) {
          capacityType = capacityTypeSuffix;
        }
      }
    }

    resource.setResource(resourceName, convertedValue, capacityType);
  }

  /**
   * Checks whether the given capacity string is in a capacity vector compatible
   * format.
   * @param configuredCapacity capacity string
   * @return true, if capacity string is in capacity vector format,
   * false otherwise
   */
  public boolean isCapacityVectorFormat(String configuredCapacity) {
    if (configuredCapacity == null) {
      return false;
    }

    String formattedCapacityString = configuredCapacity.replaceAll(" ", "");
    return RESOURCE_PATTERN.matcher(formattedCapacityString).find();
  }

  private static class Parser {
    private final Pattern regex;
    private final Function<Matcher, QueueCapacityVector> parser;

    Parser(Pattern regex, Function<Matcher, QueueCapacityVector> parser) {
      this.regex = regex;
      this.parser = parser;
    }
  }

}
