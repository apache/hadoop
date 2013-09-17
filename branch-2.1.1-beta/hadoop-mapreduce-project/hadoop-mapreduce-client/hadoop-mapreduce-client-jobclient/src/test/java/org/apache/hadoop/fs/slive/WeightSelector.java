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

package org.apache.hadoop.fs.slive;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.slive.Constants.Distribution;
import org.apache.hadoop.fs.slive.Constants.OperationType;
import org.apache.hadoop.fs.slive.Weights.UniformWeight;
import org.apache.hadoop.fs.slive.ObserveableOp.Observer;

/**
 * This class is the main handler that selects operations to run using the
 * currently held selection object. It configures and weights each operation and
 * then hands the operations + weights off to the selector object to determine
 * which one should be ran. If no operations are left to be ran then it will
 * return null.
 */
class WeightSelector {

  // what a weight calculation means
  interface Weightable {
    Double weight(int elapsed, int duration);
  }

  private static final Log LOG = LogFactory.getLog(WeightSelector.class);

  private static class OperationInfo {
    Integer amountLeft;
    Operation operation;
    Distribution distribution;
  }

  private Map<OperationType, OperationInfo> operations;
  private Map<Distribution, Weightable> weights;
  private RouletteSelector selector;
  private OperationFactory factory;

  WeightSelector(ConfigExtractor cfg, Random rnd) {
    selector = new RouletteSelector(rnd);
    factory = new OperationFactory(cfg, rnd);
    configureOperations(cfg);
    configureWeights(cfg);
  }

  protected RouletteSelector getSelector() {
    return selector;
  }

  private void configureWeights(ConfigExtractor e) {
    weights = new HashMap<Distribution, Weightable>();
    weights.put(Distribution.UNIFORM, new UniformWeight());
    // weights.put(Distribution.BEG, new BeginWeight());
    // weights.put(Distribution.END, new EndWeight());
    // weights.put(Distribution.MID, new MidWeight());
  }

  /**
   * Determines how many initial operations a given operation data should have
   * 
   * @param totalAm
   *          the total amount of operations allowed
   * 
   * @param opData
   *          the given operation information (with a valid percentage >= 0)
   * 
   * @return the number of items to allow to run
   * 
   * @throws IllegalArgumentException
   *           if negative operations are determined
   */
  static int determineHowMany(int totalAm, OperationData opData,
      OperationType type) {
    if (totalAm <= 0) {
      return 0;
    }
    int amLeft = (int) Math.floor(opData.getPercent() * totalAm);
    if (amLeft < 0) {
      throw new IllegalArgumentException("Invalid amount " + amLeft
          + " determined for operation type " + type.name());
    }
    return amLeft;
  }

  /**
   * Sets up the operation using the given configuration by setting up the
   * number of operations to perform (and how many are left) and setting up the
   * operation objects to be used throughout selection.
   * 
   * @param cfg
   *          ConfigExtractor.
   */
  private void configureOperations(ConfigExtractor cfg) {
    operations = new TreeMap<OperationType, OperationInfo>();
    Map<OperationType, OperationData> opinfo = cfg.getOperations();
    int totalAm = cfg.getOpCount();
    int opsLeft = totalAm;
    NumberFormat formatter = Formatter.getPercentFormatter();
    for (final OperationType type : opinfo.keySet()) {
      OperationData opData = opinfo.get(type);
      OperationInfo info = new OperationInfo();
      info.distribution = opData.getDistribution();
      int amLeft = determineHowMany(totalAm, opData, type);
      opsLeft -= amLeft;
      LOG
          .info(type.name() + " has " + amLeft + " initial operations out of "
              + totalAm + " for its ratio "
              + formatter.format(opData.getPercent()));
      info.amountLeft = amLeft;
      Operation op = factory.getOperation(type);
      // wrap operation in finalizer so that amount left gets decrements when
      // its done
      if (op != null) {
        Observer fn = new Observer() {
          public void notifyFinished(Operation op) {
            OperationInfo opInfo = operations.get(type);
            if (opInfo != null) {
              --opInfo.amountLeft;
            }
          }

          public void notifyStarting(Operation op) {
          }
        };
        info.operation = new ObserveableOp(op, fn);
        operations.put(type, info);
      }
    }
    if (opsLeft > 0) {
      LOG
          .info(opsLeft
              + " left over operations found (due to inability to support partial operations)");
    }
  }

  /**
   * Selects an operation from the known operation set or returns null if none
   * are available by applying the weighting algorithms and then handing off the
   * weight operations to the selection object.
   * 
   * @param elapsed
   *          the currently elapsed time (milliseconds) of the running program
   * @param duration
   *          the maximum amount of milliseconds of the running program
   * 
   * @return operation or null if none left
   */
  Operation select(int elapsed, int duration) {
    List<OperationWeight> validOps = new ArrayList<OperationWeight>(operations
        .size());
    for (OperationType type : operations.keySet()) {
      OperationInfo opinfo = operations.get(type);
      if (opinfo == null || opinfo.amountLeft <= 0) {
        continue;
      }
      Weightable weighter = weights.get(opinfo.distribution);
      if (weighter != null) {
        OperationWeight weightOp = new OperationWeight(opinfo.operation,
            weighter.weight(elapsed, duration));
        validOps.add(weightOp);
      } else {
        throw new RuntimeException("Unable to get weight for distribution "
            + opinfo.distribution);
      }
    }
    if (validOps.isEmpty()) {
      return null;
    }
    return getSelector().select(validOps);
  }
}
