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
package org.apache.hadoop.yarn.state;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;

@Private
public class VisualizeStateMachine {

  /**
   * @param classes list of classes which have static field
   *                stateMachineFactory of type StateMachineFactory
   * @return graph represent this StateMachine
   */
  public static Graph getGraphFromClasses(String graphName, List<String> classes)
      throws Exception {
    Graph ret = null;
    if (classes.size() != 1) {
      ret = new Graph(graphName);
    }
    for (String className : classes) {
      Class clz = Class.forName(className);
      Field factoryField = clz.getDeclaredField("stateMachineFactory");
      factoryField.setAccessible(true);
      StateMachineFactory factory = (StateMachineFactory) factoryField.get(null);
      if (classes.size() == 1) {
        return factory.generateStateGraph(graphName);
      }
      String gname = clz.getSimpleName();
      if (gname.endsWith("Impl")) {
        gname = gname.substring(0, gname.length()-4);
      }
      ret.addSubGraph(factory.generateStateGraph(gname));
    }
    return ret;
  }

  public static void main(String [] args) throws Exception {
    if (args.length < 3) {
      System.err.printf("Usage: %s <GraphName> <class[,class[,...]]> <OutputFile>\n",
          VisualizeStateMachine.class.getName());
      System.exit(1);
    }
    String [] classes = args[1].split(",");
    ArrayList<String> validClasses = new ArrayList<String>();
    for (String c : classes) {
      String vc = c.trim();
      if (vc.length()>0) {
        validClasses.add(vc);
      }
    }
    Graph g = getGraphFromClasses(args[0], validClasses);
    g.save(args[2]);
  }
}
