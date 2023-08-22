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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.Permission;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueStateManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.AppPriorityACLsManager;

import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.getACLsForFlexibleAutoCreatedLeafQueue;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.getACLsForFlexibleAutoCreatedParentQueue;

/**
 *
 * Context of the Queues in Capacity Scheduler.
 *
 */
@Private
@Unstable
public class CapacitySchedulerQueueManager implements SchedulerQueueManager<
    CSQueue, CapacitySchedulerConfiguration>{

  private static final Logger LOG = LoggerFactory.getLogger(
      CapacitySchedulerQueueManager.class);

  static class QueueHook {
    public CSQueue hook(CSQueue queue) {
      return queue;
    }
  }

  private static final QueueHook NOOP = new QueueHook();
  private CapacitySchedulerContext csContext;
  private final YarnAuthorizationProvider authorizer;
  private final CSQueueStore queues = new CSQueueStore();
  private CSQueue root;
  private final RMNodeLabelsManager labelManager;
  private AppPriorityACLsManager appPriorityACLManager;
  private CapacitySchedulerQueueCapacityHandler queueCapacityHandler;

  private QueueStateManager<CSQueue, CapacitySchedulerConfiguration>
      queueStateManager;
  private ConfiguredNodeLabels configuredNodeLabels;

  /**
   * Construct the service.
   * @param conf the configuration
   * @param labelManager the labelManager
   * @param appPriorityACLManager App priority ACL manager
   */
  public CapacitySchedulerQueueManager(Configuration conf,
      RMNodeLabelsManager labelManager,
      AppPriorityACLsManager appPriorityACLManager) {
    this.authorizer = YarnAuthorizationProvider.getInstance(conf);
    this.labelManager = labelManager;
    this.queueStateManager = new QueueStateManager<>();
    this.appPriorityACLManager = appPriorityACLManager;
    this.configuredNodeLabels = new ConfiguredNodeLabels();
    this.queueCapacityHandler = new CapacitySchedulerQueueCapacityHandler(labelManager,
        new CapacitySchedulerConfiguration(conf));
  }

  @Override
  public CSQueue getRootQueue() {
    return this.root;
  }

  @VisibleForTesting
  protected void setRootQueue(CSQueue rootQueue) {
    this.root = rootQueue;
  }

  @Override
  public Map<String, CSQueue> getQueues() {
    return queues.getFullNameQueues();
  }

  @VisibleForTesting
  public Map<String, CSQueue> getShortNameQueues() {
    return queues.getShortNameQueues();
  }

  @Override
  public void removeQueue(String queueName) {
    this.queues.remove(queueName);
  }

  @Override
  public void addQueue(String queueName, CSQueue queue) {
    this.queues.add(queue);
  }

  @Override
  public CSQueue getQueue(String queueName) {
    return queues.get(queueName);
  }

  public CSQueue getQueueByFullName(String name) {
    return queues.getByFullName(name);
  }

  String normalizeQueueName(String name) {
    CSQueue queue = this.queues.get(name);
    if (queue != null) {
      return queue.getQueuePath();
    }
    //We return the original name here instead of null, to make sure we don't
    // introduce a NPE, and let the process fail where it would fail for unknown
    // queues, resulting more informative error messages.
    return name;
  }

  public boolean isAmbiguous(String shortName) {
    return queues.isAmbiguous(shortName);
  }

  /**
   * Set the CapacitySchedulerContext.
   * @param capacitySchedulerContext the CapacitySchedulerContext
   */
  public void setCapacitySchedulerContext(
      CapacitySchedulerContext capacitySchedulerContext) {
    this.csContext = capacitySchedulerContext;
  }

  /**
   * Initialized the queues.
   * @param conf the CapacitySchedulerConfiguration
   * @throws IOException if fails to initialize queues
   */
  public void initializeQueues(CapacitySchedulerConfiguration conf)
    throws IOException {
    configuredNodeLabels = new ConfiguredNodeLabels(conf);
    root = parseQueue(this.csContext.getQueueContext(), conf, null,
        CapacitySchedulerConfiguration.ROOT, queues, queues, NOOP);
    setQueueAcls(authorizer, appPriorityACLManager, queues);
    labelManager.reinitializeQueueLabels(getQueueToLabels());
    this.queueStateManager.initialize(this);
    root.updateClusterResource(csContext.getClusterResource(),
        new ResourceLimits(csContext.getClusterResource()));
    LOG.info("Initialized root queue " + root);
  }

  @Override
  public void reinitializeQueues(CapacitySchedulerConfiguration newConf)
      throws IOException {
    // Parse new queues
    CSQueueStore newQueues = new CSQueueStore();
    configuredNodeLabels = new ConfiguredNodeLabels(newConf);
    CSQueue newRoot = parseQueue(this.csContext.getQueueContext(), newConf, null,
        CapacitySchedulerConfiguration.ROOT, newQueues, queues, NOOP);

    // When failing over, if using configuration store, don't validate queue
    // hierarchy since queues can be removed without being STOPPED.
    if (!csContext.isConfigurationMutable() ||
        csContext.getRMContext().getHAServiceState()
            != HAServiceProtocol.HAServiceState.STANDBY) {
      // Ensure queue hierarchy in the new XML file is proper.
      CapacitySchedulerConfigValidator
              .validateQueueHierarchy(queues, newQueues, newConf);
    }

    // Add new queues and delete OldQeueus only after validation.
    updateQueues(queues, newQueues);

    // Re-configure queues
    root.reinitialize(newRoot, this.csContext.getClusterResource());

    setQueueAcls(authorizer, appPriorityACLManager, queues);

    // Re-calculate headroom for active applications
    Resource clusterResource = this.csContext.getClusterResource();
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));

    labelManager.reinitializeQueueLabels(getQueueToLabels());
    this.queueStateManager.initialize(this);
  }

  /**
   * Parse the queue from the configuration.
   * @param queueContext the CapacitySchedulerQueueContext
   * @param conf the CapacitySchedulerConfiguration
   * @param parent the parent queue
   * @param queueName the queue name
   * @param newQueues all the queues
   * @param oldQueues the old queues
   * @param hook the queue hook
   * @return the CSQueue
   * @throws IOException
   */
  static CSQueue parseQueue(
      CapacitySchedulerQueueContext queueContext, CapacitySchedulerConfiguration conf,
      CSQueue parent, String queueName, CSQueueStore newQueues, CSQueueStore oldQueues,
      QueueHook hook) throws IOException {
    CSQueue queue;
    String fullQueueName = (parent == null) ? queueName :
        (QueuePath.createFromQueues(parent.getQueuePath(), queueName).getFullPath());
    String[] staticChildQueueNames = conf.getQueues(fullQueueName);
    List<String> childQueueNames = staticChildQueueNames != null ?
        Arrays.asList(staticChildQueueNames) : Collections.emptyList();
    CSQueue oldQueue = oldQueues.get(fullQueueName);

    boolean isReservableQueue = conf.isReservable(fullQueueName);
    boolean isAutoCreateEnabled = conf.isAutoCreateChildQueueEnabled(fullQueueName);
    // if a queue is eligible for auto queue creation v2 it must be a ParentQueue
    // (even if it is empty)
    final boolean isDynamicParent = oldQueue instanceof AbstractParentQueue &&
            oldQueue.isDynamicQueue();
    boolean isAutoQueueCreationEnabledParent = isDynamicParent || conf.isAutoQueueCreationV2Enabled(
        fullQueueName) || isAutoCreateEnabled;

    if (childQueueNames.size() == 0 && !isAutoQueueCreationEnabledParent) {
      validateParent(parent, queueName);
      // Check if the queue will be dynamically managed by the Reservation system
      if (isReservableQueue) {
        queue = new PlanQueue(queueContext, queueName, parent, oldQueues.get(fullQueueName));
        ReservationQueue defaultResQueue = ((PlanQueue) queue).initializeDefaultInternalQueue();
        newQueues.add(defaultResQueue);
      } else {
        queue = new LeafQueue(queueContext, queueName, parent, oldQueues.get(fullQueueName));
      }

      queue = hook.hook(queue);
    } else {
      if (isReservableQueue) {
        throw new IllegalStateException("Only Leaf Queues can be reservable for " + fullQueueName);
      }

      AbstractParentQueue parentQueue;
      if (isAutoCreateEnabled) {
        parentQueue = new ManagedParentQueue(queueContext, queueName, parent, oldQueues.get(
            fullQueueName));
      } else {
        parentQueue = new ParentQueue(queueContext, queueName, parent, oldQueues.get(
            fullQueueName));
      }

      queue = hook.hook(parentQueue);
      List<CSQueue> childQueues = new ArrayList<>();
      for (String childQueueName : childQueueNames) {
        CSQueue childQueue = parseQueue(queueContext, conf, queue, childQueueName, newQueues,
            oldQueues, hook);
        childQueues.add(childQueue);
      }

      if (!childQueues.isEmpty()) {
        parentQueue.setChildQueues(childQueues);
      }

    }

    newQueues.add(queue);

    LOG.info("Initialized queue: " + fullQueueName);
    return queue;
  }

  /**
   * Updates to our list of queues: Adds the new queues and deletes the removed
   * ones... be careful, do not overwrite existing queues.
   *
   * @param existingQueues, the existing queues
   * @param newQueues the new queues based on new XML
   */
  private void updateQueues(CSQueueStore existingQueues,
                            CSQueueStore newQueues) {
    CapacitySchedulerConfiguration conf = csContext.getConfiguration();
    for (CSQueue queue : newQueues.getQueues()) {
      if (existingQueues.get(queue.getQueuePath()) == null) {
        existingQueues.add(queue);
      }
    }

    for (CSQueue queue : existingQueues.getQueues()) {
      boolean isDanglingDynamicQueue = isDanglingDynamicQueue(
          newQueues, existingQueues, queue);
      boolean isRemovable = isDanglingDynamicQueue || !isDynamicQueue(queue)
          && newQueues.get(queue.getQueuePath()) == null
          && !(queue instanceof AutoCreatedLeafQueue &&
          conf.isAutoCreateChildQueueEnabled(queue.getParent().getQueuePath()));

      if (isRemovable) {
        existingQueues.remove(queue);
      }
    }

  }

  @VisibleForTesting
  /**
   * Set the acls for the queues.
   * @param authorizer the yarnAuthorizationProvider
   * @param queues the queues
   * @throws IOException if fails to set queue acls
   */
  public static void setQueueAcls(YarnAuthorizationProvider authorizer,
      AppPriorityACLsManager appPriorityACLManager, CSQueueStore queues)
      throws IOException {
    List<Permission> permissions = new ArrayList<>();
    for (CSQueue queue : queues.getQueues()) {
      AbstractCSQueue csQueue = (AbstractCSQueue) queue;
      permissions.add(
          new Permission(csQueue.getPrivilegedEntity(), csQueue.getACLs()));

      if (queue instanceof AbstractLeafQueue) {
        AbstractLeafQueue lQueue = (AbstractLeafQueue) queue;

        // Clear Priority ACLs first since reinitialize also call same.
        appPriorityACLManager.clearPriorityACLs(lQueue.getQueuePath());
        appPriorityACLManager.addPrioirityACLs(lQueue.getPriorityACLs(),
            lQueue.getQueuePath());
      }
    }
    authorizer.setPermission(permissions,
        UserGroupInformation.getCurrentUser());
  }

  /**
   * Check that the String provided in input is the name of an existing,
   * LeafQueue, if successful returns the queue.
   *
   * @param queue the queue name
   * @return the LeafQueue
   * @throws YarnException if the queue does not exist or the queue
   *           is not the type of LeafQueue.
   */
  public AbstractLeafQueue getAndCheckLeafQueue(String queue) throws YarnException {
    CSQueue ret = this.getQueue(queue);
    if (ret == null) {
      throw new YarnException("The specified Queue: " + queue
          + " doesn't exist");
    }
    if (!(ret instanceof AbstractLeafQueue)) {
      throw new YarnException("The specified Queue: " + queue
          + " is not a Leaf Queue.");
    }
    return (AbstractLeafQueue) ret;
  }

  /**
   * Get the default priority of the queue.
   * @param queueName the queue name
   * @return the default priority of the queue
   */
  public Priority getDefaultPriorityForQueue(String queueName) {
    Queue queue = getQueue(queueName);
    if (null == queue || null == queue.getDefaultApplicationPriority()) {
      // Return with default application priority
      return Priority.newInstance(CapacitySchedulerConfiguration
          .DEFAULT_CONFIGURATION_APPLICATION_PRIORITY);
    }
    return Priority.newInstance(queue.getDefaultApplicationPriority()
        .getPriority());
  }

  /**
   * Get a map of queueToLabels.
   * @return the map of queueToLabels
   */
  private Map<String, Set<String>> getQueueToLabels() {
    Map<String, Set<String>> queueToLabels = new HashMap<>();
    for (CSQueue queue :  getQueues().values()) {
      queueToLabels.put(queue.getQueuePath(), queue.getAccessibleNodeLabels());
    }
    return queueToLabels;
  }

  @Private
  public QueueStateManager<CSQueue, CapacitySchedulerConfiguration>
      getQueueStateManager() {
    return this.queueStateManager;
  }

  public CapacitySchedulerQueueCapacityHandler getQueueCapacityHandler() {
    return queueCapacityHandler;
  }

  /**
   * Removes an {@code AutoCreatedLeafQueue} from the manager collection and
   * from its parent children collection.
   *
   * @param queueName queue to be removed
   * @throws SchedulerDynamicEditException if queue is not eligible for deletion
   */
  public void removeLegacyDynamicQueue(String queueName)
      throws SchedulerDynamicEditException {
    LOG.info("Removing queue: " + queueName);
    CSQueue q = this.getQueue(queueName);
    if (q == null || !(AbstractAutoCreatedLeafQueue.class.isAssignableFrom(
        q.getClass()))) {
      throw new SchedulerDynamicEditException(
          "The queue that we are asked " + "to remove (" + queueName
              + ") is not a AutoCreatedLeafQueue or ReservationQueue");
    }
    AbstractAutoCreatedLeafQueue disposableLeafQueue =
        (AbstractAutoCreatedLeafQueue) q;
    // at this point we should have no more apps
    if (disposableLeafQueue.getNumApplications() > 0) {
      throw new SchedulerDynamicEditException(
          "The queue " + queueName + " is not empty " + disposableLeafQueue
              .getApplications().size() + " active apps "
              + disposableLeafQueue.getPendingApplications().size()
              + " pending apps");
    }

    ((AbstractManagedParentQueue) disposableLeafQueue.getParent())
        .removeChildQueue(q);
    removeQueue(queueName);
    LOG.info(
        "Removal of AutoCreatedLeafQueue " + queueName + " has succeeded");
  }

  /**
   * Adds an {@code AutoCreatedLeafQueue} to the manager collection and extends
   * the children collection of its parent.
   *
   * @param queue to be added
   * @throws SchedulerDynamicEditException if queue is not eligible to be added
   * @throws IOException if parent can not accept the queue
   */
  public void addLegacyDynamicQueue(Queue queue)
      throws SchedulerDynamicEditException, IOException {
    if (queue == null) {
      throw new SchedulerDynamicEditException(
          "Queue specified is null. Should be an implementation of "
              + "AbstractAutoCreatedLeafQueue");
    } else if (!(AbstractAutoCreatedLeafQueue.class
        .isAssignableFrom(queue.getClass()))) {
      throw new SchedulerDynamicEditException(
          "Queue is not an implementation of "
              + "AbstractAutoCreatedLeafQueue : " + queue.getClass());
    }

    AbstractAutoCreatedLeafQueue newQueue =
        (AbstractAutoCreatedLeafQueue) queue;

    if (newQueue.getParent() == null || !(AbstractManagedParentQueue.class.
        isAssignableFrom(newQueue.getParent().getClass()))) {
      throw new SchedulerDynamicEditException(
          "ParentQueue for " + newQueue + " is not properly set"
              + " (should be set and be a PlanQueue or ManagedParentQueue)");
    }

    AbstractManagedParentQueue parent =
        (AbstractManagedParentQueue) newQueue.getParent();
    String queuePath = newQueue.getQueuePath();
    parent.addChildQueue(newQueue);
    addQueue(queuePath, newQueue);

    LOG.info("Creation of AutoCreatedLeafQueue " + newQueue + " succeeded");
  }

  /**
   * Auto creates a LeafQueue and its upper hierarchy given a path at runtime.
   *
   * @param queue the application placement information of the queue
   * @return the auto created LeafQueue
   * @throws YarnException if the given path is not eligible to be auto created
   * @throws IOException if the given path can not be added to the parent
   */
  public AbstractLeafQueue createQueue(QueuePath queue)
      throws YarnException, IOException {
    String leafQueueName = queue.getLeafName();
    String parentQueueName = queue.getParent();

    if (!StringUtils.isEmpty(parentQueueName)) {
      CSQueue parentQueue = getQueue(parentQueueName);

      if (parentQueue != null && csContext.getConfiguration()
          .isAutoCreateChildQueueEnabled(parentQueue.getQueuePath())) {
        return createLegacyAutoQueue(queue);
      } else {
        return createAutoQueue(queue);
      }
    }

    throw new SchedulerDynamicEditException(
        "Could not auto-create leaf queue for " + leafQueueName
            + ". Queue mapping does not specify"
            + " which parent queue it needs to be created under.");
  }

  /**
   * Determines the missing parent paths of a potentially auto creatable queue.
   * The missing parents are sorted in a way that the first item is the highest
   * in the hierarchy.
   * Example:
   * root.a, root.a.b, root.a.b.c
   *
   * @param queue to be auto created
   * @return missing parent paths
   * @throws SchedulerDynamicEditException if the given queue is not eligible
   *                                       to be auto created
   */
  public List<String> determineMissingParents(
      QueuePath queue) throws SchedulerDynamicEditException {
    if (!queue.hasParent()) {
      throw new SchedulerDynamicEditException("Can not auto create queue "
          + queue.getFullPath() + " due to missing ParentQueue path.");
    }

    if (isAmbiguous(queue.getParent())) {
      throw new SchedulerDynamicEditException("Could not auto-create queue "
          + queue + " due to ParentQueue " + queue.getParent() +
          " being ambiguous.");
    }

    // Start from the first parent
    int firstStaticParentDistance = 1;

    StringBuilder parentCandidate = new StringBuilder(queue.getParent());
    LinkedList<String> parentsToCreate = new LinkedList<>();

    CSQueue firstExistingParent = getQueue(parentCandidate.toString());
    CSQueue firstExistingStaticParent = firstExistingParent;

    while (isNonStaticParent(firstExistingStaticParent)
        && parentCandidate.length() != 0) {
      ++firstStaticParentDistance;

      if (firstExistingParent == null) {
        parentsToCreate.addFirst(parentCandidate.toString());
      }

      int lastIndex = parentCandidate.lastIndexOf(".");
      parentCandidate.setLength(Math.max(lastIndex, 0));

      if (firstExistingParent == null) {
        firstExistingParent = getQueue(parentCandidate.toString());
      }

      firstExistingStaticParent = getQueue(parentCandidate.toString());
    }

    if (firstExistingParent == null || firstExistingStaticParent == null) {
      throw new SchedulerDynamicEditException("Could not auto-create queue "
          + queue + " parent queue does not exist.");
    }

    int maximumDepthOfStaticParent = csContext.getConfiguration().getMaximumAutoCreatedQueueDepth(
        firstExistingStaticParent.getQueuePath());
    if (firstStaticParentDistance > maximumDepthOfStaticParent) {
      throw new SchedulerDynamicEditException(
          "Could not auto create queue " + queue.getFullPath()
              + ". The distance of the LeafQueue from the first static " +
              "ParentQueue is " + firstStaticParentDistance + ", which is " +
              "above the limit.");
    }

    if (!(firstExistingParent instanceof AbstractParentQueue)) {
      throw new SchedulerDynamicEditException(
          "Could not auto create hierarchy of "
              + queue.getFullPath() + ". Queue " + queue.getParent() +
              " is not a ParentQueue."
      );
    }

    AbstractParentQueue existingParentQueue = (AbstractParentQueue) firstExistingParent;

    if (!existingParentQueue.isEligibleForAutoQueueCreation()) {
      throw new SchedulerDynamicEditException("Auto creation of queue " +
          queue.getFullPath() + " is not enabled under parent "
          + existingParentQueue.getQueuePath());
    }

    return parentsToCreate;
  }

  public List<Permission> getPermissionsForDynamicQueue(
      QueuePath queuePath,
      CapacitySchedulerConfiguration csConf) {
    List<Permission> permissions = new ArrayList<>();

    try {
      PrivilegedEntity privilegedEntity = new PrivilegedEntity(queuePath.getFullPath());

      CSQueue parentQueue = getQueueByFullName(queuePath.getParent());
      if (parentQueue == null) {
        for (String missingParent : determineMissingParents(queuePath)) {
          String parentOfMissingParent = new QueuePath(missingParent).getParent();
          permissions.add(new Permission(new PrivilegedEntity(missingParent),
              getACLsForFlexibleAutoCreatedParentQueue(
                  new AutoCreatedQueueTemplate(csConf,
                      new QueuePath(parentOfMissingParent)))));
        }
      }

      if (parentQueue instanceof AbstractManagedParentQueue) {
        // An AbstractManagedParentQueue must have been found for Legacy AQC
        permissions.add(new Permission(privilegedEntity,
            csConf.getACLsForLegacyAutoCreatedLeafQueue(queuePath.getParent())));
      } else {
        // Every other case must be a Flexible Leaf Queue
        permissions.add(new Permission(privilegedEntity,
            getACLsForFlexibleAutoCreatedLeafQueue(
                new AutoCreatedQueueTemplate(csConf, new QueuePath(queuePath.getParent())))));
      }

    } catch (SchedulerDynamicEditException e) {
      LOG.debug("Could not determine missing parents for queue {} reason {}",
          queuePath.getFullPath(), e.getMessage());
    }

    return permissions;
  }

  /**
   * Get {@code ConfiguredNodeLabels} which contains the configured node labels
   * for all queues.
   * @return configured node labels
   */
  public ConfiguredNodeLabels getConfiguredNodeLabelsForAllQueues() {
    return configuredNodeLabels;
  }

  @VisibleForTesting
  public void reinitConfiguredNodeLabels(CapacitySchedulerConfiguration conf) {
    this.configuredNodeLabels = new ConfiguredNodeLabels(conf);
  }

  private LeafQueue createAutoQueue(QueuePath queue)
      throws SchedulerDynamicEditException {
    List<String> parentsToCreate = determineMissingParents(queue);
    // First existing parent is either the parent of the last missing parent
    // or the parent of the given path
    String existingParentName = queue.getParent();
    if (!parentsToCreate.isEmpty()) {
      existingParentName = parentsToCreate.get(0).substring(
          0, parentsToCreate.get(0).lastIndexOf("."));
    }

    ParentQueue existingParentQueue = (ParentQueue) getQueue(
        existingParentName);

    for (String current : parentsToCreate) {
      existingParentQueue = existingParentQueue.addDynamicParentQueue(current);
      addQueue(existingParentQueue.getQueuePath(), existingParentQueue);
    }

    LeafQueue leafQueue = existingParentQueue.addDynamicLeafQueue(
        queue.getFullPath());
    addQueue(leafQueue.getQueuePath(), leafQueue);

    return leafQueue;
  }

  private AbstractLeafQueue createLegacyAutoQueue(QueuePath queue)
      throws IOException, SchedulerDynamicEditException {
    CSQueue parentQueue = getQueue(queue.getParent());
    // Case 1: Handle ManagedParentQueue
    ManagedParentQueue autoCreateEnabledParentQueue =
        (ManagedParentQueue) parentQueue;
    AutoCreatedLeafQueue autoCreatedLeafQueue =
        new AutoCreatedLeafQueue(
            csContext.getQueueContext(), queue.getLeafName(), autoCreateEnabledParentQueue);

    addLegacyDynamicQueue(autoCreatedLeafQueue);
    return autoCreatedLeafQueue;
  }

  private boolean isNonStaticParent(CSQueue queue) {
    return (!(queue instanceof AbstractCSQueue)
        || ((AbstractCSQueue) queue).isDynamicQueue());
  }

  private boolean isDynamicQueue(CSQueue queue) {
    return (queue instanceof AbstractCSQueue) &&
        ((AbstractCSQueue) queue).isDynamicQueue();
  }

  private boolean isDanglingDynamicQueue(
      CSQueueStore newQueues, CSQueueStore existingQueues,
      CSQueue queue) {
    if (!isDynamicQueue(queue)) {
      return false;
    }
    if (queue.getParent() == null) {
      return true;
    }
    if (newQueues.get(queue.getParent().getQueuePath()) != null) {
      return false;
    }
    CSQueue parent = existingQueues.get(queue.getParent().getQueuePath());
    if (parent == null) {
      return true;
    }
    // A dynamic queue is dangling, if its parent is not parsed in newQueues
    // or if its parent is not a dynamic queue. Dynamic queues are not parsed in
    // newQueues but they are deleted automatically, so it is safe to assume
    // that existingQueues contain valid dynamic queues.
    return !isDynamicQueue(parent);
  }

  private static void validateParent(CSQueue parent, String queueName) {
    if (parent == null) {
      throw new IllegalStateException("Queue configuration missing child queue names for "
          + queueName);
    }
  }
}
