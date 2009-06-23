/*
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
package org.apache.hadoop.fi;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * This class is responsible for the decision of when a fault 
 * has to be triggered within a class of Hadoop
 * 
 *  Default probability of injection is set to 0%. To change it
 *  one can set the sys. prop. -Dfi.*=<new probability level>
 *  Another way to do so is to set this level through FI config file,
 *  located under src/test/fi-site.conf
 *  
 *  To change the level one has to specify the following sys,prop.:
 *  -Dfi.<name of fault location>=<probability level> in the runtime
 *  Probability level is specified by a float between 0.0 and 1.0
 *  
 *  <name of fault location> might be represented by a short classname
 *  or otherwise. This decision is left up to the discretion of aspects
 *  developer, but has to be consistent through the code 
 */
public class ProbabilityModel {
  private static Random generator = new Random();
  private static final Log LOG = LogFactory.getLog(ProbabilityModel.class);

  static final String FPROB_NAME = "fi.";
  private static final String ALL_PROBABILITIES = FPROB_NAME + "*";
  private static final float DEFAULT_PROB = 0.00f; // Default probability rate is 0%

  private static Configuration conf = FiConfig.getConfig();

  static {
    // Set new default probability if specified through a system.property
    // If neither is specified set default probability to DEFAULT_PROB 
    conf.set(ALL_PROBABILITIES, 
        System.getProperty(ALL_PROBABILITIES, 
            conf.get(ALL_PROBABILITIES, Float.toString(DEFAULT_PROB))));

    LOG.info(ALL_PROBABILITIES + "=" + conf.get(ALL_PROBABILITIES));
  }

  // Simplistic method to check if we have reached the point of injection
  public static boolean injectCriteria(String klassName) {
    boolean trigger = false;
    // TODO fix this: make it more sophisticated!!!
    if (generator.nextFloat() < getProbability(klassName)) {
      trigger = true;
    }
    return trigger;
  }

  // This primitive checks for arbitrary set of desired probability and
  // uses default setting if it wasn't
  // The probability expected to be set as an float between 0 and 100
  protected static float getProbability(final String klass) {
    String newProbName = FPROB_NAME + klass;

    conf.setIfUnset(newProbName, System.getProperty(newProbName, conf.get(ALL_PROBABILITIES)));
    float ret = conf.getFloat(newProbName, conf.getFloat(ALL_PROBABILITIES, DEFAULT_PROB));
    LOG.debug("Request for " + newProbName + " returns=" + ret);
    // Make sure that probability level is valid.
    if (ret < 0.00 || ret > 1.00) 
      ret = conf.getFloat(ALL_PROBABILITIES, DEFAULT_PROB);
    
    return ret;
  }
}
