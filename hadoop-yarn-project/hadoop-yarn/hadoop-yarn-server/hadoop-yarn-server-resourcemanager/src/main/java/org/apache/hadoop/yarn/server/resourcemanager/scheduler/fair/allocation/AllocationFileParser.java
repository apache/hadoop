/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfigurationException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.ConfigurableResource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Responsible for parsing allocation.xml config file.
 * All node's text value is stored to textValues if {@link #VALID_TAG_NAMES}
 * contains the tag name.
 * Other meaningful fields are also saved in {@link #parse()}.
 */
public class AllocationFileParser {
  private static final Logger LOG =
      LoggerFactory.getLogger(AllocationFileParser.class);

  private static final String QUEUE_MAX_RESOURCES_DEFAULT =
      "queueMaxResourcesDefault";
  private static final String USER_MAX_APPS_DEFAULT = "userMaxAppsDefault";
  private static final String DEFAULT_FAIR_SHARE_PREEMPTION_TIMEOUT =
      "defaultFairSharePreemptionTimeout";
  private static final String FAIR_SHARE_PREEMPTION_TIMEOUT =
      "fairSharePreemptionTimeout";
  private static final String DEFAULT_MIN_SHARE_PREEMPTION_TIMEOUT =
      "defaultMinSharePreemptionTimeout";
  private static final String QUEUE_MAX_APPS_DEFAULT = "queueMaxAppsDefault";
  private static final String DEFAULT_FAIR_SHARE_PREEMPTION_THRESHOLD =
      "defaultFairSharePreemptionThreshold";
  private static final String QUEUE_MAX_AM_SHARE_DEFAULT =
      "queueMaxAMShareDefault";
  private static final String RESERVATION_PLANNER = "reservation-planner";
  private static final String RESERVATION_AGENT = "reservation-agent";
  private static final String RESERVATION_ADMISSION_POLICY =
      "reservation-policy";
  private static final String QUEUE_PLACEMENT_POLICY = "queuePlacementPolicy";
  private static final String QUEUE = "queue";
  private static final String POOL = "pool";
  private static final String USER = "user";
  private static final String USERNAME = "name";
  private static final String MAX_RUNNING_APPS = "maxRunningApps";
  private static final String DEFAULT_QUEUE_SCHEDULING_POLICY =
      "defaultQueueSchedulingPolicy";
  private static final String DEFAULT_QUEUE_SCHEDULING_MODE =
      "defaultQueueSchedulingMode";

  private static final Set<String> VALID_TAG_NAMES =
      Sets.newHashSet(QUEUE_MAX_RESOURCES_DEFAULT, USER_MAX_APPS_DEFAULT,
          DEFAULT_FAIR_SHARE_PREEMPTION_TIMEOUT, FAIR_SHARE_PREEMPTION_TIMEOUT,
          DEFAULT_MIN_SHARE_PREEMPTION_TIMEOUT, QUEUE_MAX_APPS_DEFAULT,
          DEFAULT_FAIR_SHARE_PREEMPTION_THRESHOLD, QUEUE_MAX_AM_SHARE_DEFAULT,
          RESERVATION_PLANNER, RESERVATION_AGENT, RESERVATION_ADMISSION_POLICY,
          QUEUE_PLACEMENT_POLICY, QUEUE, POOL, USER,
          DEFAULT_QUEUE_SCHEDULING_POLICY, DEFAULT_QUEUE_SCHEDULING_MODE);

  private final NodeList elements;
  private final Map<String, String> textValues = Maps.newHashMap();
  private Element queuePlacementPolicyElement;
  private final List<Element> queueElements = new ArrayList<>();
  private final Map<String, Integer> userMaxApps = new HashMap<>();
  private SchedulingPolicy defaultSchedulingPolicy;

  public AllocationFileParser(NodeList elements) {
    this.elements = elements;
  }

  public void parse() throws AllocationConfigurationException {
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node instanceof Element) {
        Element element = (Element) node;
        final String tagName = element.getTagName();
        if (VALID_TAG_NAMES.contains(tagName)) {
          if (tagName.equals(QUEUE_PLACEMENT_POLICY)) {
            queuePlacementPolicyElement = element;
          } else if (isSchedulingPolicy(element)) {
            defaultSchedulingPolicy = extractSchedulingPolicy(element);
          } else if (isQueue(element)) {
            queueElements.add(element);
          } else if (tagName.equals(USER)) {
            extractUserData(element);
          } else {
            textValues.put(tagName, getTrimmedTextData(element));
          }
        } else {
          LOG.warn("Bad element in allocations file: " + tagName);
        }
      }
    }
  }

  private boolean isSchedulingPolicy(Element element) {
    return DEFAULT_QUEUE_SCHEDULING_POLICY.equals(element.getTagName())
        || DEFAULT_QUEUE_SCHEDULING_MODE.equals(element.getTagName());
  }

  private void extractUserData(Element element) {
    final String userName = element.getAttribute(USERNAME);
    final NodeList fields = element.getChildNodes();
    for (int j = 0; j < fields.getLength(); j++) {
      final Node fieldNode = fields.item(j);
      if (!(fieldNode instanceof Element)) {
        continue;
      }
      final Element field = (Element) fieldNode;
      if (MAX_RUNNING_APPS.equals(field.getTagName())) {
        final String text = getTrimmedTextData(field);
        final int val = Integer.parseInt(text);
        userMaxApps.put(userName, val);
      }
    }
  }

  private SchedulingPolicy extractSchedulingPolicy(Element element)
      throws AllocationConfigurationException {
    String text = getTrimmedTextData(element);
    if (text.equalsIgnoreCase(FifoPolicy.NAME)) {
      throw new AllocationConfigurationException("Bad fair scheduler "
          + "config file: defaultQueueSchedulingPolicy or "
          + "defaultQueueSchedulingMode can't be FIFO.");
    }
    return SchedulingPolicy.parse(text);
  }

  private boolean isQueue(Element element) {
    return element.getTagName().equals(QUEUE)
        || element.getTagName().equals(POOL);
  }

  private String getTrimmedTextData(Element element) {
    return ((Text) element.getFirstChild()).getData().trim();
  }

  public ConfigurableResource getQueueMaxResourcesDefault()
      throws AllocationConfigurationException {
    Optional<String> value = getTextValue(QUEUE_MAX_RESOURCES_DEFAULT);
    if (value.isPresent()) {
      return FairSchedulerConfiguration.parseResourceConfigValue(value.get());
    }
    return new ConfigurableResource(Resources.unbounded());
  }

  public int getUserMaxAppsDefault() {
    Optional<String> value = getTextValue(USER_MAX_APPS_DEFAULT);
    return value.map(Integer::parseInt).orElse(Integer.MAX_VALUE);
  }

  public long getDefaultFairSharePreemptionTimeout() {
    Optional<String> value = getTextValue(FAIR_SHARE_PREEMPTION_TIMEOUT);
    Optional<String> defaultValue =
        getTextValue(DEFAULT_FAIR_SHARE_PREEMPTION_TIMEOUT);

    if (value.isPresent() && !defaultValue.isPresent()) {
      return Long.parseLong(value.get()) * 1000L;
    } else if (defaultValue.isPresent()) {
      return Long.parseLong(defaultValue.get()) * 1000L;
    }
    return Long.MAX_VALUE;
  }

  public long getDefaultMinSharePreemptionTimeout() {
    Optional<String> value = getTextValue(DEFAULT_MIN_SHARE_PREEMPTION_TIMEOUT);
    return value.map(v -> Long.parseLong(v) * 1000L).orElse(Long.MAX_VALUE);
  }

  public int getQueueMaxAppsDefault() {
    Optional<String> value = getTextValue(QUEUE_MAX_APPS_DEFAULT);
    return value.map(Integer::parseInt).orElse(Integer.MAX_VALUE);
  }

  public float getDefaultFairSharePreemptionThreshold() {
    Optional<String> value =
        getTextValue(DEFAULT_FAIR_SHARE_PREEMPTION_THRESHOLD);
    if (value.isPresent()) {
      float floatValue = Float.parseFloat(value.get());
      return Math.max(Math.min(floatValue, 1.0f), 0.0f);
    }
    return 0.5f;
  }

  public float getQueueMaxAMShareDefault() {
    Optional<String> value = getTextValue(QUEUE_MAX_AM_SHARE_DEFAULT);
    if (value.isPresent()) {
      float val = Float.parseFloat(value.get());
      return Math.min(val, 1.0f);
    }
    return 0.5f;
  }

  // Reservation global configuration knobs
  public Optional<String> getReservationPlanner() {
    return getTextValue(RESERVATION_PLANNER);
  }

  public Optional<String> getReservationAgent() {
    return getTextValue(RESERVATION_AGENT);
  }

  public Optional<String> getReservationAdmissionPolicy() {
    return getTextValue(RESERVATION_ADMISSION_POLICY);
  }

  public Optional<Element> getQueuePlacementPolicy() {
    return Optional.ofNullable(queuePlacementPolicyElement);
  }

  private Optional<String> getTextValue(String key) {
    return Optional.ofNullable(textValues.get(key));
  }

  public List<Element> getQueueElements() {
    return queueElements;
  }

  public Map<String, Integer> getUserMaxApps() {
    return userMaxApps;
  }

  public SchedulingPolicy getDefaultSchedulingPolicy() {
    if (defaultSchedulingPolicy != null) {
      return defaultSchedulingPolicy;
    }
    return SchedulingPolicy.DEFAULT_POLICY;
  }
}
