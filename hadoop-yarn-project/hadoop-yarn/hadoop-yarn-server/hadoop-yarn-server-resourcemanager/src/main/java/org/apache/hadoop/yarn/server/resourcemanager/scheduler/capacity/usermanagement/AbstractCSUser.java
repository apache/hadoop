package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.usermanagement;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Provides an interface to get user related information (metadata & bookkeeping counters)
 * To preserve package & class integrity, public methods of this class should not modify any attributes
 * And getters should only return immutable attributes or deep copied instances
 */
public abstract class AbstractCSUser {

  // Primary attribute which tracks users resource usage
  final ResourceUsage userResourceUsage = new ResourceUsage();
  private final String userName;
  private final AtomicInteger pendingApplications = new AtomicInteger(0);
  private final AtomicInteger activeApplications = new AtomicInteger(0);

  // Caches the users resource limit which can be used to display in webapp
  private volatile Resource userResourceLimit = Resource.newInstance(0, 0);

  AbstractCSUser(String name) {
    this.userName = name;
  }

  public Resource getUsedCloned() {
    return Resources.clone(userResourceUsage.getUsed());
  }

  public Resource getUsedCloned(String label) {
    return Resources.clone(userResourceUsage.getUsed(label));
  }

  public Resource getReservedCloned(String label) {
    return Resources.clone(userResourceUsage.getReserved(label));
  }

  public Resource getConsumedAMResourcesCloned() {
    return Resources.clone(userResourceUsage.getAMUsed());
  }

  public Resource getConsumedAMResourcesCloned(String label) {
    return Resources.clone(userResourceUsage.getAMUsed(label));
  }

  public String getUserName() {
    return this.userName;
  }

  public int getPendingApplications() {
    return pendingApplications.get();
  }

  public int getActiveApplications() {
    return activeApplications.get();
  }

  /**
   * Returns total applications for the user - but the count isn't strongly consistent and depends on if apps are moving
   * from pending to active concurrently
   */
  public int getTotalApplications() {
    return getPendingApplications() + getActiveApplications();
  }

  // TODO - remove this API
  public void incAMUsed(String label, Resource res) {
    this.userResourceUsage.incAMUsed(label, res);
  }

  // TODO - remove this API
  public void decAMUsed(String label, Resource res) {
    this.userResourceUsage.decAMUsed(label, res);
  }

  // TODO - remove this API
  public void setAMLimit(String label, Resource res) {
    this.userResourceUsage.setAMLimit(label, res);
  }

  // TODO - remove this API
  public void activateApplication() {
    pendingApplications.decrementAndGet();
    activeApplications.incrementAndGet();
  }

  void submitApplication() {
    pendingApplications.incrementAndGet();
  }

  void finishApplication(boolean wasActive) {
    if (wasActive) {
      activeApplications.decrementAndGet();
    } else {
      pendingApplications.decrementAndGet();
    }
  }

  // Should not be made public method because clients can then modify the users resource usage (which they shouldn't be doing)
  // Clients should use getUsedCloned() if they want users resource object
  Resource getUsed(String label) {
    return userResourceUsage.getUsed(label);
  }

  Resource getUserResourceLimit() {
    return userResourceLimit;
  }

  void setUserResourceLimit(Resource userResourceLimit) {
    this.userResourceLimit = userResourceLimit;
  }
}
