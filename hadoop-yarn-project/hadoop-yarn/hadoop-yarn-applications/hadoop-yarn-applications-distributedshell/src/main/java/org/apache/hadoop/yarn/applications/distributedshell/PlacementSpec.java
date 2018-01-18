/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.applications.distributedshell;

import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Class encapsulating a SourceTag, number of container and a Placement
 * Constraint.
 */
public class PlacementSpec {

  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementSpec.class);
  private static final String SPEC_DELIM = ":";
  private static final String KV_SPLIT_DELIM = "=";
  private static final String SPEC_VAL_DELIM = ",";
  private static final String IN = "in";
  private static final String NOT_IN = "notin";
  private static final String CARDINALITY = "cardinality";

  public final String sourceTag;
  public final int numContainers;
  public final PlacementConstraint constraint;

  public PlacementSpec(String sourceTag, int numContainers,
      PlacementConstraint constraint) {
    this.sourceTag = sourceTag;
    this.numContainers = numContainers;
    this.constraint = constraint;
  }

  // Placement specification should be of the form:
  // PlacementSpec => ""|KeyVal;PlacementSpec
  // KeyVal => SourceTag=Constraint
  // SourceTag => String
  // Constraint => NumContainers|
  //               NumContainers,"in",Scope,TargetTag|
  //               NumContainers,"notin",Scope,TargetTag|
  //               NumContainers,"cardinality",Scope,TargetTag,MinCard,MaxCard
  // NumContainers => int (number of containers)
  // Scope => "NODE"|"RACK"
  // TargetTag => String (Target Tag)
  // MinCard => int (min cardinality - needed if ConstraintType == cardinality)
  // MaxCard => int (max cardinality - needed if ConstraintType == cardinality)

  /**
   * Parser to convert a string representation of a placement spec to mapping
   * from source tag to Placement Constraint.
   *
   * @param specs Placement spec.
   * @return Mapping from source tag to placement constraint.
   */
  public static Map<String, PlacementSpec> parse(String specs) {
    LOG.info("Parsing Placement Specs: [{}]", specs);
    Scanner s = new Scanner(specs).useDelimiter(SPEC_DELIM);
    Map<String, PlacementSpec> pSpecs = new HashMap<>();
    while (s.hasNext()) {
      String sp = s.next();
      LOG.info("Parsing Spec: [{}]", sp);
      String[] specSplit = sp.split(KV_SPLIT_DELIM);
      String sourceTag = specSplit[0];
      Scanner ps = new Scanner(specSplit[1]).useDelimiter(SPEC_VAL_DELIM);
      int numContainers = ps.nextInt();
      if (!ps.hasNext()) {
        pSpecs.put(sourceTag,
            new PlacementSpec(sourceTag, numContainers, null));
        LOG.info("Creating Spec without constraint {}: num[{}]",
            sourceTag, numContainers);
        continue;
      }
      String cType = ps.next().toLowerCase();
      String scope = ps.next().toLowerCase();

      String targetTag = ps.next();
      scope = scope.equals("rack") ? PlacementConstraints.RACK :
          PlacementConstraints.NODE;

      PlacementConstraint pc;
      if (cType.equals(IN)) {
        pc = PlacementConstraints.build(
            PlacementConstraints.targetIn(scope,
                PlacementConstraints.PlacementTargets.allocationTag(
                    targetTag)));
        LOG.info("Creating IN Constraint for source tag [{}], num[{}]: " +
                "scope[{}], target[{}]",
            sourceTag, numContainers, scope, targetTag);
      } else if (cType.equals(NOT_IN)) {
        pc = PlacementConstraints.build(
            PlacementConstraints.targetNotIn(scope,
                PlacementConstraints.PlacementTargets.allocationTag(
                    targetTag)));
        LOG.info("Creating NOT_IN Constraint for source tag [{}], num[{}]: " +
                "scope[{}], target[{}]",
            sourceTag, numContainers, scope, targetTag);
      } else if (cType.equals(CARDINALITY)) {
        int minCard = ps.nextInt();
        int maxCard = ps.nextInt();
        pc = PlacementConstraints.build(
            PlacementConstraints.targetCardinality(scope, minCard, maxCard,
                PlacementConstraints.PlacementTargets.allocationTag(
                    targetTag)));
        LOG.info("Creating CARDINALITY Constraint source tag [{}], num[{}]: " +
                "scope[{}], min[{}], max[{}], target[{}]",
            sourceTag, numContainers, scope, minCard, maxCard, targetTag);
      } else {
        throw new RuntimeException(
            "Could not parse constraintType [" + cType + "]" +
                " in [" + specSplit[1] + "]");
      }
      pSpecs.put(sourceTag, new PlacementSpec(sourceTag, numContainers, pc));
    }
    return pSpecs;
  }
}
