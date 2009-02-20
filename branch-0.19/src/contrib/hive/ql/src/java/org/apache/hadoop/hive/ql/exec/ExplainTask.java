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

package org.apache.hadoop.hive.ql.exec;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.plan.explain;
import org.apache.hadoop.hive.ql.plan.explainWork;
import org.apache.hadoop.util.StringUtils;


/**
 * ExplainTask implementation
 * 
 **/
public class ExplainTask extends Task<explainWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  public int execute() {
    
    try {
    	OutputStream outS = FileSystem.get(conf).create(work.getResFile());
    	PrintStream out = new PrintStream(outS);
    	
      // Print out the parse AST
      outputAST(work.getAstStringTree(), out, 0);
      out.println();
      
      outputDependencies(out, work.getRootTasks(), 0);
      out.println();
      
      // Go over all the tasks and dump out the plans
      outputStagePlans(out, work.getRootTasks(), 0);
      out.close();
      
      return (0);
    }
    catch (Exception e) {
      console.printError("Failed with exception " +   e.getMessage(), "\n" + StringUtils.stringifyException(e));
      return (1);
    }
  }

  private String indentString(int indent) {
    StringBuilder sb = new StringBuilder();
    for(int i=0; i<indent; ++i) {
      sb.append(" ");
    }
    
    return sb.toString();
  }

  private void outputMap(Map<?, ?> mp, String header,
                         PrintStream out, boolean extended, int indent) 
    throws Exception {

    boolean first_el = true;
    for(Entry<?,?> ent: mp.entrySet()) {
      if (first_el) {
        out.println(header);
      }
      first_el = false;
              
      // Print the key
      out.print(indentString(indent));
      out.printf("%s ", ent.getKey().toString());
      // Print the value
      if (isPrintable(ent.getValue())) {
        out.print(ent.getValue());
        out.println();
      }
      else if (ent.getValue() instanceof Serializable) {
        out.println();
        outputPlan((Serializable)ent.getValue(), out, extended, indent+2);
      }
    }
  }

  private void outputList(List<?> l, String header,
                          PrintStream out, boolean extended, int indent) 
    throws Exception {
  
    boolean first_el = true;
    boolean nl = false;
    for(Object o: l) {
      if (first_el) {
        out.print(header);
      }
              
      if (isPrintable(o)) {
        if (!first_el) {
          out.print(", ");
        } else {
          out.print(" ");
        }
                
        out.print(o);
        nl = true;
      }
      else if (o instanceof Serializable) {
        if (first_el) {
          out.println();
        }
        outputPlan((Serializable)o, out, extended, indent+2);
      }
              
      first_el = false;
    }
            
    if (nl) {
      out.println();
    }
  }

  private boolean isPrintable(Object val) {
    if (val instanceof String ||
        val instanceof Integer ||
        val instanceof Byte ||
        val instanceof Float ||
        val instanceof Double) {
      return true;
    }

    if (val.getClass().isPrimitive()) {
      return true;
    }
    
    return false;
  }

  private void outputPlan(Serializable work, PrintStream out, boolean extended, int indent) 
    throws Exception {
    // Check if work has an explain annotation
    Annotation note = work.getClass().getAnnotation(explain.class);
    
    if (note instanceof explain) {
      explain xpl_note = (explain)note;
      if (extended || xpl_note.normalExplain()) {
        out.print(indentString(indent));
        out.println(xpl_note.displayName());
      }
    }

    // If this is an operator then we need to call the plan generation on the conf and then
    // the children
    if (work instanceof Operator) {
      Operator<? extends Serializable> operator = (Operator<? extends Serializable>) work;
      if (operator.getConf() != null) {
        outputPlan(operator.getConf(), out, extended, indent);
      }
      if (operator.getChildOperators() != null) {
        for(Operator<? extends Serializable> op: operator.getChildOperators()) {
          outputPlan(op, out, extended, indent+2);
        }
      }
      return;
    }
    
    // We look at all methods that generate values for explain
    Method[] methods = work.getClass().getMethods();
    Arrays.sort(methods, new MethodComparator());

    for(Method m: methods) {
      int prop_indents = indent+2;
      note = m.getAnnotation(explain.class);

      if (note instanceof explain) {
        explain xpl_note = (explain)note;

        if (extended || xpl_note.normalExplain()) {
          
          Object val = m.invoke(work);

          if (val == null) {
            continue;
          }
          
          String header = null;
          if (!xpl_note.displayName().equals("")){
            header = indentString(prop_indents) + xpl_note.displayName() +":";
          } else {
            prop_indents = indent;
            header = indentString(prop_indents);
          }

          if (isPrintable(val)) {
            
            out.printf("%s ", header);
            out.println(val);
            continue;
          }
          // Try this as a map
          try {
            // Go through the map and print out the stuff
            Map<?,?> mp = (Map<?,?>)val;
            outputMap(mp, header, out, extended, prop_indents+2);
            continue;
          }
          catch (ClassCastException ce) {
            // Ignore - all this means is that this is not a map
          }

          // Try this as a list
          try {
            List<?> l = (List<?>)val;
            outputList(l, header, out, extended, prop_indents+2);
            
            continue;
          }
          catch (ClassCastException ce) {
            // Ignore
          }
          

          // Finally check if it is serializable
          try {
            Serializable s = (Serializable)val;
            out.println(header);
            outputPlan(s, out, extended, prop_indents+2);
            
            continue;
          }
          catch (ClassCastException ce) {
            // Ignore
          }
        }
      }
    }
  }
  
  private void outputPlan(Task<? extends Serializable> task, PrintStream out, 
                          boolean extended, HashSet<Task<? extends Serializable>> displayedSet,
                          int indent) 
    throws Exception {
  
    if (displayedSet.contains(task)) {
      return;
    }
    displayedSet.add(task);
    
    out.print(indentString(indent));
    out.printf("Stage: %s\n", task.getId());
    // Start by getting the work part of the task and call the output plan for the work
    outputPlan(task.getWork(), out, extended, indent+2);
    out.println();
    if (task.getChildTasks() != null) {
      for(Task<? extends Serializable> child: task.getChildTasks()) {
        outputPlan(child, out, extended, displayedSet, indent);
      }
    }
  }

  private void outputDependencies(Task<? extends Serializable> task, PrintStream out, int indent) 
    throws Exception {
    
    out.print(indentString(indent));
    out.printf("%s", task.getId());
    if (task.getParentTasks() == null || task.getParentTasks().isEmpty()) {
      out.print(" is a root stage");
    }
    else {
      out.print(" depends on stages: ");
      boolean first = true;
      for(Task<? extends Serializable> parent: task.getParentTasks()) {
        if (!first) {
          out.print(", ");
        }
        first = false;
        out.print(parent.getId());
      }
    }
    out.println();
    
    if (task.getChildTasks() != null) {
      for(Task<? extends Serializable> child: task.getChildTasks()) {
        outputDependencies(child, out, indent);
      }
    }
  }

  public void outputAST(String treeString, PrintStream out, int indent) {
    out.print(indentString(indent));
    out.println("ABSTRACT SYNTAX TREE:");
    out.print(indentString(indent+2));
    out.println(treeString);    
  }

  public void outputDependencies(PrintStream out, 
                                 List<Task<? extends Serializable>> rootTasks,
                                 int indent) 
    throws Exception {
    out.print(indentString(indent));
    out.println("STAGE DEPENDENCIES:");
    for(Task<? extends Serializable> rootTask: rootTasks) {
      outputDependencies(rootTask, out, indent+2);
    }
  }

  public void outputStagePlans(PrintStream out, 
                               List<Task<? extends Serializable>> rootTasks,
                               int indent) 
    throws Exception {
    out.print(indentString(indent));
    out.println("STAGE PLANS:");
    for(Task<? extends Serializable> rootTask: rootTasks) {
      outputPlan(rootTask, out, work.getExtended(),
                 new HashSet<Task<? extends Serializable>>(), indent+2);
    }
  }

  public static class MethodComparator implements Comparator {
    public int compare(Object o1, Object o2) {
      Method m1 = (Method)o1;
      Method m2 = (Method)o2;
      return m1.getName().compareTo(m2.getName());
    }
  }

}
