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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AbsoluteResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueVectorResourceType;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class that parses {@code QueueResourcesVector} from the capacity
 * configuration property set for a queue.
 */
public class QueueCapacityConfigParser {
  private static final String UNIFORM_REGEX = "^([0-9.]+)(.*)";
  private static final String RESOURCE_REGEX = "^\\[[\\w\\.,\\-_=%\\ /]+\\]$";

  private static final Pattern RESOURCE_PATTERN = Pattern.compile(RESOURCE_REGEX);
  private static final Pattern UNIFORM_PATTERN = Pattern.compile(UNIFORM_REGEX);

  public static final Set<String> ALLOWED_MEMORY_NAMES = ImmutableSet.of(
      AbsoluteResourceType.MEMORY.name().toLowerCase(),
      ResourceInformation.MEMORY_URI);

  private final List<Parser> parsers = new ArrayList<>();

  public QueueCapacityConfigParser() {
    parsers.add(new Parser(RESOURCE_PATTERN, this::resourceParser));
    parsers.add(new Parser(UNIFORM_PATTERN, this::uniformParser));
  }

  /**
   * Creates a {@code QueueResourceVector} parsed from the capacity configuration
   * property set for a queue.
   * @param conf configuration object
   * @param queuePath queue for which the capacity property is parsed
   * @param label node label
   * @return a parsed resource vector
   */
  public QueueCapacityVector parse(CapacitySchedulerConfiguration conf,
                                   String queuePath, String label) {

    if (queuePath.equals(CapacitySchedulerConfiguration.ROOT)) {
      return createUniformResourceVector(QueueVectorResourceType.PERCENTAGE, 100f);
    }

    String propertyName = CapacitySchedulerConfiguration.getNodeLabelPrefix(
        queuePath, label) + CapacitySchedulerConfiguration.CAPACITY;
    String capacityString = conf.get(propertyName);

    if (capacityString == null) {
      return QueueCapacityVector.empty();
    }

    for (Parser parser : parsers) {
      Matcher matcher = parser.regex.matcher(capacityString);
      if (matcher.find()) {
        return parser.parser.apply(matcher);
      }
    }

    return QueueCapacityVector.empty();
  }

  /**
   * A parser method that is usable on uniform capacity values eg. percentage or
   * weight.
   * @param matcher a regex matcher that contains parsed value and its possible
   *                suffix
   * @return a parsed resource vector
   */
  private QueueCapacityVector uniformParser(Matcher matcher) {
    QueueVectorResourceType vectorResourceType = QueueVectorResourceType.PERCENTAGE;
    String value = matcher.group(1);
    if (matcher.groupCount() == 2) {
      String matchedSuffix = matcher.group(2);
      if (!matchedSuffix.isEmpty()) {
        for (QueueVectorResourceType suffix : QueueVectorResourceType.values()) {
          // when capacity is given in percentage, we do not need % symbol
          String uniformSuffix = suffix.getPostfix().replaceAll("%", "");
          if (uniformSuffix.equals(matchedSuffix)) {
            vectorResourceType = suffix;
          }
        }
      }
    }

    return createUniformResourceVector(vectorResourceType, Float.parseFloat(value));
  }

  private QueueCapacityVector createUniformResourceVector(QueueVectorResourceType vectorResourceType, float parsedValue) {
    Set<String> resourceTypes = ResourceUtils.getResourceTypes().keySet();
    QueueCapacityVector resource = QueueCapacityVector.empty();

    for (String resourceName : resourceTypes) {
      resource.setResource(resourceName, parsedValue, vectorResourceType);
    }
    return resource;
  }

  /**
   * A parser method that is usable on resource capacity values eg. mixed or
   * absolute resource.
   * @param matcher a regex matcher that contains the matched resource string
   * @return a parsed resource vector
   */
  private QueueCapacityVector resourceParser(Matcher matcher) {
    // Define resource here.
    QueueCapacityVector resourceVector = QueueCapacityVector.empty();

    /*
     * Absolute resource configuration for a queue will be grouped by "[]".
     * Syntax of absolute resource config could be like below
     * "memory=4Gi vcores=2". Ideally this means "4GB of memory and 2 vcores".
     */
    // Get the sub-group.
    String bracketedGroup = matcher.group(0);
    if (bracketedGroup.trim().isEmpty()) {
      return resourceVector;
    }
    bracketedGroup = bracketedGroup.substring(1, bracketedGroup.length() - 1);
    // Split by comma and equals delimiter eg. memory=1024, vcores=6 to
    // [[memory, 1024], [vcores, 6]]
    for (String kvPair : bracketedGroup.trim().split(",")) {
      String[] splits = kvPair.split("=");

      // Ensure that each sub string is key value pair separated by '='.
      if (splits.length > 1) {
        setResourceVector(resourceVector, splits[0], splits[1]);
      }
    }

    // Memory has to be configured always.
    if (resourceVector.getMemory() == 0L) {
      return QueueCapacityVector.empty();
    }

    return resourceVector;
  }

  private void setResourceVector(QueueCapacityVector resource, String resourceName, String resourceValue) {
    QueueVectorResourceType resourceType = QueueVectorResourceType.ABSOLUTE;

    // Extract suffix from a value eg. for 6w extract w
    String suffix = resourceValue.replaceAll("[0-9]", "");
    if (!resourceValue.endsWith(suffix)) {
      return;
    }

    String cleanResourceName = resourceName.replaceAll(" ", "");
    float parsedResourceValue = Float.parseFloat(resourceValue.substring(
        0, resourceValue.length() - suffix.length()));
    float convertedValue = parsedResourceValue;

    if (!suffix.isEmpty() && UnitsConversionUtil.KNOWN_UNITS.contains(suffix)) {
      // Convert all incoming units to MB if units is configured.
      convertedValue = UnitsConversionUtil.convert(suffix, "Mi", (long) parsedResourceValue);
    } else {
      for (QueueVectorResourceType resourceTypeSuffix : QueueVectorResourceType.values()) {
        if (resourceTypeSuffix.getPostfix().equals(suffix)) {
          resourceType = resourceTypeSuffix;
        }
      }
    }

    resource.setResource(cleanResourceName, convertedValue, resourceType);
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
