/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.examples;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class ExampleDriver {
  
  /**
   * A description of an example program based on its class and a 
   * human-readable description.
   * @author Owen O'Malley
   * @date feb 2006
   */
  static private class ProgramDescription {
    
    static final Class[] paramTypes = new Class[] {String[].class};
    
    /**
     * Create a description of an example program.
     * @param mainClass the class with the main for the example program
     * @param description a string to display to the user in help messages
     * @throws SecurityException if we can't use reflection
     * @throws NoSuchMethodException if the class doesn't have a main method
     */
    public ProgramDescription(Class mainClass, 
                              String description)
    throws SecurityException, NoSuchMethodException {
      this.main = mainClass.getMethod("main", paramTypes);
      this.description = description;
    }
    
    /**
     * Invoke the example application with the given arguments
     * @param args the arguments for the application
     * @throws Throwable The exception thrown by the invoked method
     */
    public void invoke(String[] args)
    throws Throwable {
      try {
        main.invoke(null, new Object[]{args});
      } catch (InvocationTargetException except) {
        throw except.getCause();
      }
    }
    
    public String getDescription() {
      return description;
    }
    
    private Method main;
    private String description;
  }
  
  private static void printUsage(Map programs) {
    System.out.println("Valid program names are:");
    for(Iterator itr=programs.entrySet().iterator(); itr.hasNext();) {
      Map.Entry item = (Entry) itr.next();
      System.out.println("  " + (String) item.getKey() + ": " +
          ((ProgramDescription) item.getValue()).getDescription());
    }   
  }
  
  /**
   * This is a driver for the example programs.
   * It looks at the first command line argument and tries to find an
   * example program with that name.
   * If it is found, it calls the main method in that class with the rest 
   * of the command line arguments.
   * @param args The argument from the user. args[0] is the command to run.
   * @throws NoSuchMethodException 
   * @throws SecurityException 
   * @throws IllegalAccessException 
   * @throws IllegalArgumentException 
   * @throws Throwable Anything thrown by the example program's main
   */
  public static void main(String[] args) 
  throws Throwable 
  {
    Map programs = new TreeMap();
    
    // Add new programs to this list
    programs.put("wordcount", new ProgramDescription(WordCount.class,
    "A map/reduce program that counts the words in the input files."));
    programs.put("grep", new ProgramDescription(Grep.class,
    "A map/reduce program that counts the matches of a regex in the input."));
    
    // Make sure they gave us a program name.
    if (args.length == 0) {
      System.out.println("An example program must be given as the" + 
          " first argument.");
      printUsage(programs);
      return;
    }
    
    // And that it is good.
    ProgramDescription pgm = (ProgramDescription) programs.get(args[0]);
    if (pgm == null) {
      System.out.println("Unknown program '" + args[0] + "' chosen.");
      printUsage(programs);
      return;
    }
    
    // Remove the leading argument and call main
    String[] new_args = new String[args.length - 1];
    for(int i=1; i < args.length; ++i) {
      new_args[i-1] = args[i];
    }
    pgm.invoke(new_args);
  }
  
}
