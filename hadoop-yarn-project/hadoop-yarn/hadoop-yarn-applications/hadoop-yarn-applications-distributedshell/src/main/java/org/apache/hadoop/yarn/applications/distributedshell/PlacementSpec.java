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
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParseException;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.SourceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Class encapsulating a SourceTag, number of container and a Placement
 * Constraint.
 */
public class PlacementSpec {

  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementSpec.class);

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
  public static Map<String, PlacementSpec> parse(String specs)
      throws IllegalArgumentException {
    LOG.info("Parsing Placement Specs: [{}]", specs);
    Map<String, PlacementSpec> pSpecs = new HashMap<>();
    Map<SourceTags, PlacementConstraint> parsed;
    try {
      parsed = PlacementConstraintParser.parsePlacementSpec(specs);
      for (Map.Entry<SourceTags, PlacementConstraint> entry :
          parsed.entrySet()) {
        LOG.info("Parsed source tag: {}, number of allocations: {}",
            entry.getKey().getTag(), entry.getKey().getNumOfAllocations());
        LOG.info("Parsed constraint: {}", entry.getValue()
            .getConstraintExpr().getClass().getSimpleName());
        pSpecs.put(entry.getKey().getTag(), new PlacementSpec(
            entry.getKey().getTag(),
            entry.getKey().getNumOfAllocations(),
            entry.getValue()));
      }
      return pSpecs;
    } catch (PlacementConstraintParseException e) {
      throw new IllegalArgumentException(
          "Invalid placement spec: " + specs, e);
    }
  }
}
