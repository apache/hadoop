package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.GB;

public class CapacitySchedulerQueueCalculationTestBase {

  protected static class QueueAssertionBuilder {
    public static final String EFFECTIVE_MIN_RES_INFO = "Effective Minimum Resource";
    public static final Function<QueueResourceQuotas, Resource> EFFECTIVE_MIN_RES =
        QueueResourceQuotas::getEffectiveMinResource;

    public static final String CAPACITY_INFO = "Capacity";
    public static final Function<QueueCapacities, Float> CAPACITY =
        QueueCapacities::getCapacity;

    public static final String ABS_CAPACITY_INFO = "Absolute Capacity";
    public static final Function<QueueCapacities, Float> ABS_CAPACITY =
        QueueCapacities::getAbsoluteCapacity;

    private static final String ASSERTION_ERROR_MESSAGE =
        "'%s' of queue '%s' does not match %f";
    private static final String RESOURCE_ASSERTION_ERROR_MESSAGE =
        "'%s' of queue '%s' does not match %s";
    private final CapacityScheduler cs;

    QueueAssertionBuilder(CapacityScheduler cs) {
      this.cs = cs;
    }

    public class QueueAssertion {

      public class ValueAssertion {
        private float expectedValue = 0;
        private Resource expectedResource = null;
        private String assertionType;
        private Supplier<Float> valueSupplier;
        private Supplier<Resource> resourceSupplier;

        ValueAssertion(float expectedValue) {
          this.expectedValue = expectedValue;
        }

        ValueAssertion(Resource expectedResource) {
          this.expectedResource = expectedResource;
        }

        public QueueAssertion assertEffectiveMinResource() {
          return withResourceSupplier(EFFECTIVE_MIN_RES, EFFECTIVE_MIN_RES_INFO);
        }

        public QueueAssertion assertCapacity() {
          return withCapacitySupplier(CAPACITY, CAPACITY_INFO);
        }

        public QueueAssertion assertAbsoluteCapacity() {
          return withCapacitySupplier(ABS_CAPACITY, ABS_CAPACITY_INFO);
        }

        public QueueAssertion withResourceSupplier(
            Function<QueueResourceQuotas, Resource> assertion, String messageInfo) {
          CSQueue queue = cs.getQueue(queuePath);
          if (queue == null) {
            Assert.fail("Queue " + queuePath + " is not found");
          }

          assertionType = messageInfo;
          resourceSupplier = () -> assertion.apply(queue.getQueueResourceQuotas());
          QueueAssertion.this.assertions.add(this);

          return QueueAssertion.this;
        }

        public QueueAssertion withCapacitySupplier(
            Function<QueueCapacities, Float> assertion, String messageInfo) {
          CSQueue queue = cs.getQueue(queuePath);
          if (queue == null) {
            Assert.fail("Queue " + queuePath + " is not found");
          }
          assertionType = messageInfo;
          valueSupplier = () -> assertion.apply(queue.getQueueCapacities());
          QueueAssertion.this.assertions.add(this);

          return QueueAssertion.this;
        }
      }

      private final String queuePath;
      private final List<ValueAssertion> assertions = new ArrayList<>();

      QueueAssertion(String queuePath) {
        this.queuePath = queuePath;
      }

      public ValueAssertion toExpect(float expected) {
        return new ValueAssertion(expected);
      }

      public ValueAssertion toExpect(Resource expected) {
        return new ValueAssertion(expected);
      }


      public QueueAssertion withQueue(String queuePath) {
        return QueueAssertionBuilder.this.withQueue(queuePath);
      }

      public QueueAssertionBuilder build() {
        return QueueAssertionBuilder.this.build();
      }
    }

    private final Map<String, QueueAssertion> assertions = new LinkedHashMap<>();

    public QueueAssertionBuilder build() {
      return this;
    }

    public QueueAssertion withQueue(String queuePath) {
      assertions.putIfAbsent(queuePath, new QueueAssertion(queuePath));
      return assertions.get(queuePath);
    }

    public void finishAssertion() {
      for (Map.Entry<String, QueueAssertion> assertionEntry : assertions.entrySet()) {
        for (QueueAssertion.ValueAssertion assertion : assertionEntry.getValue().assertions) {
          if (assertion.resourceSupplier != null) {
            String errorMessage = String.format(RESOURCE_ASSERTION_ERROR_MESSAGE,
                assertion.assertionType, assertionEntry.getKey(),
                assertion.expectedResource.toString());
            Assert.assertEquals(errorMessage,
                assertion.expectedResource,
                assertion.resourceSupplier.get());
          } else {
            String errorMessage = String.format(ASSERTION_ERROR_MESSAGE,
                assertion.assertionType, assertionEntry.getKey(),
                assertion.expectedValue);
            Assert.assertEquals(errorMessage,
                assertion.expectedValue,
                assertion.valueSupplier.get(),
                1e-6);
          }
        }
      }
    }

    public Set<String> getQueues() {
      return assertions.keySet();
    }
  }

  protected MockRM mockRM;
  protected CapacityScheduler cs;
  protected CapacitySchedulerConfiguration csConf;
  protected NullRMNodeLabelsManager mgr;

  @Before
  public void setUp() throws Exception {
    csConf = new CapacitySchedulerConfiguration();
    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    csConf.setQueues("root", new String[]{"a", "b"});
    csConf.setNonLabeledQueueWeight("root", 1f);
    csConf.setNonLabeledQueueWeight("root.a", 6f);
    csConf.setNonLabeledQueueWeight("root.b", 4f);
    csConf.setQueues("root.a", new String[]{"a1", "a2"});
    csConf.setNonLabeledQueueWeight("root.a.a1", 1f);
    csConf.setQueues("root.a.a1", new String[]{"a11", "a12"});
    csConf.setNonLabeledQueueWeight("root.a.a1.a11", 1f);
    csConf.setNonLabeledQueueWeight("root.a.a1.a12", 1f);

    mgr = new NullRMNodeLabelsManager();
    mgr.init(csConf);
    mockRM = new MockRM(csConf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    cs = (CapacityScheduler) mockRM.getResourceScheduler();
    cs.updatePlacementRules();
    // Policy for new auto created queue's auto deletion when expired
    mockRM.start();
    cs.start();
    mockRM.registerNode("h1:1234", 10 * GB); // label = x
  }

  protected QueueHierarchyUpdateContext update(
      QueueAssertionBuilder assertions, Resource resource) throws IOException {
    cs.reinitialize(csConf, mockRM.getRMContext());

    CapacitySchedulerQueueCapacityHandler queueController =
        new CapacitySchedulerQueueCapacityHandler(mgr);
    mgr.setResourceForLabel(CommonNodeLabelsManager.NO_LABEL, resource);
    for (String queueToAssert : assertions.getQueues()) {
      CSQueue queue = cs.getQueue(queueToAssert);
      queueController.setup(queue);
    }

    QueueHierarchyUpdateContext updateContext =
        queueController.update(resource, cs.getQueue("root"));

    assertions.finishAssertion();

    return updateContext;
  }

  protected QueueAssertionBuilder createAssertionBuilder() {
    return new QueueAssertionBuilder(cs);
  }
}
