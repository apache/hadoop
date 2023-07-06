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

import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.*;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import java.util.List;
import java.util.Map;

/**
 * Responsible for loading queue configuration properties
 * from a list of {@link Element}s containing queues.
 */
public class AllocationFileQueueParser {
  private static final Logger LOG =
      LoggerFactory.getLogger(AllocationFileQueueParser.class);

  public static final String ROOT = "root";
  public static final AccessControlList EVERYBODY_ACL =
      new AccessControlList("*");
  static final AccessControlList NOBODY_ACL = new AccessControlList(" ");
  private static final String MIN_RESOURCES = "minResources";
  private static final String MAX_RESOURCES = "maxResources";
  private static final String MAX_CHILD_RESOURCES = "maxChildResources";
  private static final String MAX_RUNNING_APPS = "maxRunningApps";
  private static final String MAX_AMSHARE = "maxAMShare";
  public static final String MAX_CONTAINER_ALLOCATION =
      "maxContainerAllocation";
  private static final String WEIGHT = "weight";
  private static final String MIN_SHARE_PREEMPTION_TIMEOUT =
      "minSharePreemptionTimeout";
  private static final String FAIR_SHARE_PREEMPTION_TIMEOUT =
      "fairSharePreemptionTimeout";
  private static final String FAIR_SHARE_PREEMPTION_THRESHOLD =
      "fairSharePreemptionThreshold";
  private static final String SCHEDULING_POLICY = "schedulingPolicy";
  private static final String SCHEDULING_MODE = "schedulingMode";
  private static final String ACL_SUBMIT_APPS = "aclSubmitApps";
  private static final String ACL_ADMINISTER_APPS = "aclAdministerApps";
  private static final String ACL_ADMINISTER_RESERVATIONS =
      "aclAdministerReservations";
  private static final String ACL_LIST_RESERVATIONS = "aclListReservations";
  private static final String ACL_SUBMIT_RESERVATIONS = "aclSubmitReservations";
  private static final String RESERVATION = "reservation";
  private static final String ALLOW_PREEMPTION_FROM = "allowPreemptionFrom";
  private static final String QUEUE = "queue";
  private static final String POOL = "pool";

  private final List<Element> elements;

  public AllocationFileQueueParser(List<Element> elements) {
    this.elements = elements;
  }

  // Load queue elements. A root queue can either be included or omitted. If
  // it's included, all other queues must be inside it.
  public QueueProperties parse() throws AllocationConfigurationException {
    QueueProperties.Builder queuePropertiesBuilder =
        new QueueProperties.Builder();
    for (Element element : elements) {
      String parent = ROOT;
      if (element.getAttribute("name").equalsIgnoreCase(ROOT)) {
        if (elements.size() > 1) {
          throw new AllocationConfigurationException(
              "If configuring root queue,"
                  + " no other queues can be placed alongside it.");
        }
        parent = null;
      }
      loadQueue(parent, element, queuePropertiesBuilder);
    }

    return queuePropertiesBuilder.build();
  }

  /**
   * Loads a queue from a queue element in the configuration file.
   */
  private void loadQueue(String parentName, Element element,
      QueueProperties.Builder builder) throws AllocationConfigurationException {
    String queueName =
        FairSchedulerUtilities.trimQueueName(element.getAttribute("name"));

    if (queueName.contains(".")) {
      throw new AllocationConfigurationException("Bad fair scheduler config "
          + "file: queue name (" + queueName + ") shouldn't contain period.");
    }

    if (queueName.isEmpty()) {
      throw new AllocationConfigurationException("Bad fair scheduler config "
          + "file: queue name shouldn't be empty or "
          + "consist only of whitespace.");
    }

    if (parentName != null) {
      queueName = parentName + "." + queueName;
    }

    NodeList fields = element.getChildNodes();
    boolean isLeaf = true;
    boolean isReservable = false;
    boolean isMaxAMShareSet = false;

    for (int j = 0; j < fields.getLength(); j++) {
      Node fieldNode = fields.item(j);
      if (!(fieldNode instanceof Element)) {
        continue;
      }
      Element field = (Element) fieldNode;
      if (MIN_RESOURCES.equals(field.getTagName())) {
        String text = getTrimmedTextData(field);
        ConfigurableResource val =
            FairSchedulerConfiguration.parseResourceConfigValue(text, 0L);
        builder.minQueueResources(queueName, val.getResource());
      } else if (MAX_RESOURCES.equals(field.getTagName())) {
        String text = getTrimmedTextData(field);
        ConfigurableResource val =
            FairSchedulerConfiguration.parseResourceConfigValue(text);
        builder.maxQueueResources(queueName, val);
      } else if (MAX_CHILD_RESOURCES.equals(field.getTagName())) {
        String text = getTrimmedTextData(field);
        ConfigurableResource val =
            FairSchedulerConfiguration.parseResourceConfigValue(text);
        builder.maxChildQueueResources(queueName, val);
      } else if (MAX_RUNNING_APPS.equals(field.getTagName())) {
        String text = getTrimmedTextData(field);
        int val = Integer.parseInt(text);
        builder.queueMaxApps(queueName, val);
      } else if (MAX_AMSHARE.equals(field.getTagName())) {
        String text = getTrimmedTextData(field);
        float val = Float.parseFloat(text);
        val = Math.min(val, 1.0f);
        builder.queueMaxAMShares(queueName, val);
        isMaxAMShareSet = true;
      } else if (MAX_CONTAINER_ALLOCATION.equals(field.getTagName())) {
        String text = getTrimmedTextData(field);
        ConfigurableResource val =
            FairSchedulerConfiguration.parseResourceConfigValue(text);
        builder.queueMaxContainerAllocation(queueName, val.getResource());
      } else if (WEIGHT.equals(field.getTagName())) {
        String text = getTrimmedTextData(field);
        double val = Double.parseDouble(text);
        builder.queueWeights(queueName, (float) val);
      } else if (MIN_SHARE_PREEMPTION_TIMEOUT.equals(field.getTagName())) {
        String text = getTrimmedTextData(field);
        long val = Long.parseLong(text) * 1000L;
        builder.minSharePreemptionTimeouts(queueName, val);
      } else if (FAIR_SHARE_PREEMPTION_TIMEOUT.equals(field.getTagName())) {
        String text = getTrimmedTextData(field);
        long val = Long.parseLong(text) * 1000L;
        builder.fairSharePreemptionTimeouts(queueName, val);
      } else if (FAIR_SHARE_PREEMPTION_THRESHOLD.equals(field.getTagName())) {
        String text = getTrimmedTextData(field);
        float val = Float.parseFloat(text);
        val = Math.max(Math.min(val, 1.0f), 0.0f);
        builder.fairSharePreemptionThresholds(queueName, val);
      } else if (SCHEDULING_POLICY.equals(field.getTagName())
          || SCHEDULING_MODE.equals(field.getTagName())) {
        String text = getTrimmedTextData(field);
        SchedulingPolicy policy = SchedulingPolicy.parse(text);
        builder.queuePolicies(queueName, policy);
      } else if (ACL_SUBMIT_APPS.equals(field.getTagName())) {
        String text = ((Text) field.getFirstChild()).getData();
        builder.queueAcls(queueName, AccessType.SUBMIT_APP,
            new AccessControlList(text));
      } else if (ACL_ADMINISTER_APPS.equals(field.getTagName())) {
        String text = ((Text) field.getFirstChild()).getData();
        builder.queueAcls(queueName, AccessType.ADMINISTER_QUEUE,
            new AccessControlList(text));
      } else if (ACL_ADMINISTER_RESERVATIONS.equals(field.getTagName())) {
        String text = ((Text) field.getFirstChild()).getData();
        builder.reservationAcls(queueName,
            ReservationACL.ADMINISTER_RESERVATIONS,
            new AccessControlList(text));
      } else if (ACL_LIST_RESERVATIONS.equals(field.getTagName())) {
        String text = ((Text) field.getFirstChild()).getData();
        builder.reservationAcls(queueName, ReservationACL.LIST_RESERVATIONS,
            new AccessControlList(text));
      } else if (ACL_SUBMIT_RESERVATIONS.equals(field.getTagName())) {
        String text = ((Text) field.getFirstChild()).getData();
        builder.reservationAcls(queueName, ReservationACL.SUBMIT_RESERVATIONS,
            new AccessControlList(text));
      } else if (RESERVATION.equals(field.getTagName())) {
        isReservable = true;
        builder.reservableQueues(queueName);
        builder.configuredQueues(FSQueueType.PARENT, queueName);
      } else if (ALLOW_PREEMPTION_FROM.equals(field.getTagName())) {
        String text = getTrimmedTextData(field);
        if (!Boolean.parseBoolean(text)) {
          builder.nonPreemptableQueues(queueName);
        }
      } else if (QUEUE.endsWith(field.getTagName())
          || POOL.equals(field.getTagName())) {
        loadQueue(queueName, field, builder);
        isLeaf = false;
      }
    }
    // if a leaf in the alloc file is marked as type='parent'
    // then store it as a parent queue
    if (isLeaf && !"parent".equals(element.getAttribute("type"))) {
      // reservable queue has been already configured as parent
      if (!isReservable) {
        builder.configuredQueues(FSQueueType.LEAF, queueName);
      }
    } else {
      if (isReservable) {
        throw new AllocationConfigurationException(
            getErrorString(queueName, RESERVATION));
      } else if (isMaxAMShareSet) {
        throw new AllocationConfigurationException(
            getErrorString(queueName, MAX_AMSHARE));
      }
      builder.configuredQueues(FSQueueType.PARENT, queueName);
    }

    // Set default acls if not defined
    // The root queue defaults to all access
    for (QueueACL acl : QueueACL.values()) {
      AccessType accessType = SchedulerUtils.toAccessType(acl);
      if (!builder.isAclDefinedForAccessType(queueName, accessType)) {
        AccessControlList defaultAcl =
            queueName.equals(ROOT) ? EVERYBODY_ACL : NOBODY_ACL;
        builder.queueAcls(queueName, accessType, defaultAcl);
      }
    }

    checkMinAndMaxResource(builder.getMinQueueResources(),
        builder.getMaxQueueResources(), queueName);
  }

  /**
   * Set up the error string based on the supplied parent queueName and element.
   * @param parentQueueName the parent queue name.
   * @param element the element that should not be present for the parent queue.
   * @return the error string.
   */
  private String getErrorString(String parentQueueName, String element) {
    return "The configuration settings"
        + " for " + parentQueueName + " are invalid. A queue element that "
        + "contains child queue elements or that has the type='parent' "
        + "attribute cannot also include a " + element + " element.";
  }

  private String getTrimmedTextData(Element element) {
    return ((Text) element.getFirstChild()).getData().trim();
  }

  private void checkMinAndMaxResource(Map<String, Resource> minResources,
      Map<String, ConfigurableResource> maxResources, String queueName) {

    ConfigurableResource maxConfigurableResource = maxResources.get(queueName);
    Resource minResource = minResources.get(queueName);

    if (maxConfigurableResource != null && minResource != null) {
      Resource maxResource = maxConfigurableResource.getResource();

      // check whether max resource is greater or equals to min resource when
      // max resource are absolute values
      if (maxResource != null && !Resources.fitsIn(minResource, maxResource)) {
        LOG.warn(String.format(
            "Queue %s has max resources %s less than " + "min resources %s",
            queueName, maxResource, minResource));
      }
    }
  }
}
