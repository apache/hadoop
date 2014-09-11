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

package org.apache.hadoop.util;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** Utility to assist with generation of progress reports.  Applications build
 * a hierarchy of {@link Progress} instances, each modelling a phase of
 * execution.  The root is constructed with {@link #Progress()}.  Nodes for
 * sub-phases are created by calling {@link #addPhase()}.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class Progress {
  private static final Log LOG = LogFactory.getLog(Progress.class);
  private String status = "";
  private float progress;
  private int currentPhase;
  private ArrayList<Progress> phases = new ArrayList<Progress>();
  private Progress parent;

  // Each phase can have different progress weightage. For example, in
  // Map Task, map phase accounts for 66.7% and sort phase for 33.3%.
  // User needs to give weightages as parameters to all phases(when adding
  // phases) in a Progress object, if he wants to give weightage to any of the
  // phases. So when nodes are added without specifying weightage, it means 
  // fixed weightage for all phases.
  private boolean fixedWeightageForAllPhases = false;
  private float progressPerPhase = 0.0f;
  private ArrayList<Float> progressWeightagesForPhases = new ArrayList<Float>();
  
  /** Creates a new root node. */
  public Progress() {}

  /** Adds a named node to the tree. */
  public Progress addPhase(String status) {
    Progress phase = addPhase();
    phase.setStatus(status);
    return phase;
  }

  /** Adds a node to the tree. Gives equal weightage to all phases */
  public synchronized Progress addPhase() {
    Progress phase = addNewPhase();
    // set equal weightage for all phases
    progressPerPhase = 1.0f / phases.size();
    fixedWeightageForAllPhases = true;
    return phase;
  }
  
  /** Adds a new phase. Caller needs to set progress weightage */
  private synchronized Progress addNewPhase() {
    Progress phase = new Progress();
    phases.add(phase);
    phase.setParent(this);
    return phase;
  }

  /** Adds a named node with a specified progress weightage to the tree. */
  public Progress addPhase(String status, float weightage) {
    Progress phase = addPhase(weightage);
    phase.setStatus(status);

    return phase;
  }

  /** Adds a node with a specified progress weightage to the tree. */
  public synchronized Progress addPhase(float weightage) {
    Progress phase = new Progress();
    progressWeightagesForPhases.add(weightage);
    phases.add(phase);
    phase.setParent(this);

    // Ensure that the sum of weightages does not cross 1.0
    float sum = 0;
    for (int i = 0; i < phases.size(); i++) {
      sum += progressWeightagesForPhases.get(i);
    }
    if (sum > 1.0) {
      LOG.warn("Sum of weightages can not be more than 1.0; But sum = " + sum);
    }

    return phase;
  }

  /** Adds n nodes to the tree. Gives equal weightage to all phases */
  public synchronized void addPhases(int n) {
    for (int i = 0; i < n; i++) {
      addNewPhase();
    }
    // set equal weightage for all phases
    progressPerPhase = 1.0f / phases.size();
    fixedWeightageForAllPhases = true;
  }

  /**
   * returns progress weightage of the given phase
   * @param phaseNum the phase number of the phase(child node) for which we need
   *                 progress weightage
   * @return returns the progress weightage of the specified phase
   */
  float getProgressWeightage(int phaseNum) {
    if (fixedWeightageForAllPhases) {
      return progressPerPhase; // all phases are of equal weightage
    }
    return progressWeightagesForPhases.get(phaseNum);
  }

  synchronized Progress getParent() { return parent; }
  synchronized void setParent(Progress parent) { this.parent = parent; }
  
  /** Called during execution to move to the next phase at this level in the
   * tree. */
  public synchronized void startNextPhase() {
    currentPhase++;
  }

  /** Returns the current sub-node executing. */
  public synchronized Progress phase() {
    return phases.get(currentPhase);
  }

  /** Completes this node, moving the parent node to its next child. */
  public void complete() {
    // we have to traverse up to our parent, so be careful about locking.
    Progress myParent;
    synchronized(this) {
      progress = 1.0f;
      myParent = parent;
    }
    if (myParent != null) {
      // this will synchronize on the parent, so we make sure we release
      // our lock before getting the parent's, since we're traversing 
      // against the normal traversal direction used by get() or toString().
      // We don't need transactional semantics, so we're OK doing this. 
      myParent.startNextPhase();
    }
  }

  /** Called during execution on a leaf node to set its progress. */
  public synchronized void set(float progress) {
    if (Float.isNaN(progress)) {
      progress = 0;
      LOG.debug("Illegal progress value found, progress is Float.NaN. " +
        "Progress will be changed to 0");
    }
    else if (progress == Float.NEGATIVE_INFINITY) {
      progress = 0;
      LOG.debug("Illegal progress value found, progress is " +
        "Float.NEGATIVE_INFINITY. Progress will be changed to 0");
    }
    else if (progress < 0) {
      progress = 0;
      LOG.debug("Illegal progress value found, progress is less than 0." +
        " Progress will be changed to 0");
    }
    else if (progress > 1) {
      progress = 1;
      LOG.debug("Illegal progress value found, progress is larger than 1." +
        " Progress will be changed to 1");
    }
    else if (progress == Float.POSITIVE_INFINITY) {
      progress = 1;
      LOG.debug("Illegal progress value found, progress is " +
        "Float.POSITIVE_INFINITY. Progress will be changed to 1");
    }
    this.progress = progress;
  }

  /** Returns the overall progress of the root. */
  // this method probably does not need to be synchronized as getInternal() is
  // synchronized and the node's parent never changes. Still, it doesn't hurt. 
  public synchronized float get() {
    Progress node = this;
    while (node.getParent() != null) {                 // find the root
      node = parent;
    }
    return node.getInternal();
  }

  /**
   * Returns progress in this node. get() would give overall progress of the
   * root node(not just given current node).
   */
  public synchronized float getProgress() {
    return getInternal();
  }
  
  /** Computes progress in this node. */
  private synchronized float getInternal() {
    int phaseCount = phases.size();
    if (phaseCount != 0) {
      float subProgress = 0.0f;
      float progressFromCurrentPhase = 0.0f;
      if (currentPhase < phaseCount) {
        subProgress = phase().getInternal();
        progressFromCurrentPhase =
          getProgressWeightage(currentPhase) * subProgress;
      }
      
      float progressFromCompletedPhases = 0.0f;
      if (fixedWeightageForAllPhases) { // same progress weightage for each phase
        progressFromCompletedPhases = progressPerPhase * currentPhase;
      }
      else {
        for (int i = 0; i < currentPhase; i++) {
          // progress weightages of phases could be different. Add them
          progressFromCompletedPhases += getProgressWeightage(i);
        }
      }
      return  progressFromCompletedPhases + progressFromCurrentPhase;
    } else {
      return progress;
    }
  }

  public synchronized void setStatus(String status) {
    this.status = status;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    toString(result);
    return result.toString();
  }

  private synchronized void toString(StringBuilder buffer) {
    buffer.append(status);
    if (phases.size() != 0 && currentPhase < phases.size()) {
      buffer.append(" > ");
      phase().toString(buffer);
    }
  }

}
