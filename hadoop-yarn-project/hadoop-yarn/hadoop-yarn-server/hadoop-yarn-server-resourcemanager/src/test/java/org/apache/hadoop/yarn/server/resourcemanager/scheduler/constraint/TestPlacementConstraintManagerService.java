/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.NODE;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.RACK;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetCardinality;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetNotIn;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTag;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.nodeAttribute;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.And;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link PlacementConstraintManagerService}.
 */
public class TestPlacementConstraintManagerService {

  private PlacementConstraintManagerService pcm;

  protected PlacementConstraintManagerService createPCM() {
    return new MemoryPlacementConstraintManager();
  }

  private ApplicationId appId1, appId2;
  private PlacementConstraint c1, c2, c3, c4;
  private Set<String> sourceTag1, sourceTag2, sourceTag3, sourceTag4;
  private Map<Set<String>, PlacementConstraint> constraintMap1, constraintMap2;

  @Before
  public void before() {
    this.pcm = createPCM();

    // Build appIDs, constraints, source tags, and constraint map.
    long ts = System.currentTimeMillis();
    appId1 = BuilderUtils.newApplicationId(ts, 123);
    appId2 = BuilderUtils.newApplicationId(ts, 234);

    c1 = PlacementConstraints.build(targetIn(NODE, allocationTag("hbase-m")));
    c2 = PlacementConstraints.build(targetIn(RACK, allocationTag("hbase-rs")));
    c3 = PlacementConstraints
        .build(targetNotIn(NODE, nodeAttribute("java", "1.8")));
    c4 = PlacementConstraints
        .build(targetCardinality(RACK, 2, 10, allocationTag("zk")));

    sourceTag1 = new HashSet<>(Arrays.asList("spark"));
    sourceTag2 = new HashSet<>(Arrays.asList("zk"));
    sourceTag3 = new HashSet<>(Arrays.asList("storm"));
    sourceTag4 = new HashSet<>(Arrays.asList("hbase-m", "hbase-sec"));

    constraintMap1 = Stream
        .of(new SimpleEntry<>(sourceTag1, c1),
            new SimpleEntry<>(sourceTag2, c2))
        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

    constraintMap2 = Stream.of(new SimpleEntry<>(sourceTag3, c4))
        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
  }

  @Test
  public void testRegisterUnregisterApps() {
    Assert.assertEquals(0, pcm.getNumRegisteredApplications());

    // Register two applications.
    pcm.registerApplication(appId1, constraintMap1);
    Assert.assertEquals(1, pcm.getNumRegisteredApplications());
    Map<Set<String>, PlacementConstraint> constrMap =
        pcm.getConstraints(appId1);
    Assert.assertNotNull(constrMap);
    Assert.assertEquals(2, constrMap.size());
    Assert.assertNotNull(constrMap.get(sourceTag1));
    Assert.assertNotNull(constrMap.get(sourceTag2));

    pcm.registerApplication(appId2, constraintMap2);
    Assert.assertEquals(2, pcm.getNumRegisteredApplications());
    constrMap = pcm.getConstraints(appId2);
    Assert.assertNotNull(constrMap);
    Assert.assertEquals(1, constrMap.size());
    Assert.assertNotNull(constrMap.get(sourceTag3));
    Assert.assertNull(constrMap.get(sourceTag2));

    // Try to register the same app again.
    pcm.registerApplication(appId2, constraintMap1);
    Assert.assertEquals(2, pcm.getNumRegisteredApplications());

    // Unregister appId1.
    pcm.unregisterApplication(appId1);
    Assert.assertEquals(1, pcm.getNumRegisteredApplications());
    Assert.assertNull(pcm.getConstraints(appId1));
    Assert.assertNotNull(pcm.getConstraints(appId2));
  }

  @Test
  public void testAddConstraint() {
    // Cannot add constraint to unregistered app.
    Assert.assertEquals(0, pcm.getNumRegisteredApplications());
    pcm.addConstraint(appId1, sourceTag1, c1, false);
    Assert.assertEquals(0, pcm.getNumRegisteredApplications());

    // Register application.
    pcm.registerApplication(appId1, new HashMap<>());
    Assert.assertEquals(1, pcm.getNumRegisteredApplications());
    Assert.assertEquals(0, pcm.getConstraints(appId1).size());

    // Add two constraints.
    pcm.addConstraint(appId1, sourceTag1, c1, false);
    pcm.addConstraint(appId1, sourceTag2, c3, false);
    Assert.assertEquals(2, pcm.getConstraints(appId1).size());

    // Constraint for sourceTag1 should not be replaced.
    pcm.addConstraint(appId1, sourceTag1, c2, false);
    Assert.assertEquals(2, pcm.getConstraints(appId1).size());
    Assert.assertEquals(c1, pcm.getConstraint(appId1, sourceTag1));
    Assert.assertNotEquals(c2, pcm.getConstraint(appId1, sourceTag1));

    // Now c2 should replace c1 for sourceTag1.
    pcm.addConstraint(appId1, sourceTag1, c2, true);
    Assert.assertEquals(2, pcm.getConstraints(appId1).size());
    Assert.assertEquals(c2, pcm.getConstraint(appId1, sourceTag1));
  }

  @Test
  public void testGlobalConstraints() {
    Assert.assertEquals(0, pcm.getNumGlobalConstraints());
    pcm.addGlobalConstraint(sourceTag1, c1, false);
    Assert.assertEquals(1, pcm.getNumGlobalConstraints());
    Assert.assertNotNull(pcm.getGlobalConstraint(sourceTag1));

    // Constraint for sourceTag1 should not be replaced.
    pcm.addGlobalConstraint(sourceTag1, c2, false);
    Assert.assertEquals(1, pcm.getNumGlobalConstraints());
    Assert.assertEquals(c1, pcm.getGlobalConstraint(sourceTag1));
    Assert.assertNotEquals(c2, pcm.getGlobalConstraint(sourceTag1));

    // Now c2 should replace c1 for sourceTag1.
    pcm.addGlobalConstraint(sourceTag1, c2, true);
    Assert.assertEquals(1, pcm.getNumGlobalConstraints());
    Assert.assertEquals(c2, pcm.getGlobalConstraint(sourceTag1));

    pcm.removeGlobalConstraint(sourceTag1);
    Assert.assertEquals(0, pcm.getNumGlobalConstraints());
  }

  @Test
  public void testValidateConstraint() {
    // At the moment we only disallow multiple source tags to be associated with
    // a constraint. TODO: More tests to be added for YARN-6621.
    Assert.assertTrue(pcm.validateConstraint(sourceTag1, c1));
    Assert.assertFalse(pcm.validateConstraint(sourceTag4, c1));
  }

  @Test
  public void testGetRequestConstraint() {
    // Request Constraint(RC), App Constraint(AC), Global Constraint(GC)
    PlacementConstraint constraint;
    And mergedConstraint;
    SchedulingRequest request;

    // RC = c1
    // AC = null
    // GC = null
    constraint = pcm.getMultilevelConstraint(appId1, null, c1);
    Assert.assertTrue(constraint.getConstraintExpr() instanceof And);
    mergedConstraint = (And) constraint.getConstraintExpr();
    Assert.assertEquals(1, mergedConstraint.getChildren().size());
    Assert.assertEquals(c1, mergedConstraint.getChildren().get(0).build());

    // RC = null
    // AC = tag1->c1, tag2->c2
    // GC = null
    pcm.registerApplication(appId1, constraintMap1);
    // if the source tag in the request is not mapped to any existing
    // registered constraint, we should get an empty AND constraint.
    constraint = pcm.getMultilevelConstraint(appId1,
        Sets.newHashSet("not_exist_tag"), null);
    Assert.assertTrue(constraint.getConstraintExpr() instanceof And);
    mergedConstraint = (And) constraint.getConstraintExpr();
    // AND()
    Assert.assertEquals(0, mergedConstraint.getChildren().size());
    // if a mapping is found for a given source tag
    constraint = pcm.getMultilevelConstraint(appId1, sourceTag1, null);
    Assert.assertTrue(constraint.getConstraintExpr() instanceof And);
    mergedConstraint = (And) constraint.getConstraintExpr();
    // AND(c1)
    Assert.assertEquals(1, mergedConstraint.getChildren().size());
    Assert.assertEquals(c1, mergedConstraint.getChildren().get(0).build());
    pcm.unregisterApplication(appId1);

    // RC = null
    // AC = null
    // GC = tag1->c1
    pcm.addGlobalConstraint(sourceTag1, c1, true);
    constraint = pcm.getMultilevelConstraint(appId1,
        Sets.newHashSet(sourceTag1), null);
    Assert.assertTrue(constraint.getConstraintExpr() instanceof And);
    mergedConstraint = (And) constraint.getConstraintExpr();
    // AND(c1)
    Assert.assertEquals(1, mergedConstraint.getChildren().size());
    Assert.assertEquals(c1, mergedConstraint.getChildren().get(0).build());
    pcm.removeGlobalConstraint(sourceTag1);

    // RC = c2
    // AC = tag1->c1, tag2->c2
    // GC = tag1->c3
    pcm.addGlobalConstraint(sourceTag1, c3, true);
    pcm.registerApplication(appId1, constraintMap1);
    // both RC, AC and GC should be respected
    constraint = pcm.getMultilevelConstraint(appId1, sourceTag1, c2);
    Assert.assertTrue(constraint.getConstraintExpr() instanceof And);
    mergedConstraint = (And) constraint.getConstraintExpr();
    // AND(c1, c2, c3)
    Assert.assertEquals(3, mergedConstraint.getChildren().size());
    pcm.removeGlobalConstraint(sourceTag1);
    pcm.unregisterApplication(appId1);

    // RC = c1
    // AC = tag1->c1, tag2->c2
    // GC = tag1->c2
    pcm.addGlobalConstraint(sourceTag1, c2, true);
    pcm.registerApplication(appId1, constraintMap1);
    constraint = pcm.getMultilevelConstraint(appId1,
        Sets.newHashSet(sourceTag1), c1);
    Assert.assertTrue(constraint.getConstraintExpr() instanceof And);
    mergedConstraint = (And) constraint.getConstraintExpr();
    // AND(c1, c2)
    Assert.assertEquals(2, mergedConstraint.getChildren().size());
    pcm.removeGlobalConstraint(sourceTag1);
    pcm.unregisterApplication(appId1);
  }
}
