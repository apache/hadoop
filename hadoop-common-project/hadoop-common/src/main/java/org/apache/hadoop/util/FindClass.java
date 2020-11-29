/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import java.security.CodeSource;

/**
 * This entry point exists for diagnosing classloader problems:
 * is a class or resource present -and if so, where?
 *
 * <p>
 * Actions
 * <br>
 * <ul>
 *   <li><pre>load</pre>: load a class but do not attempt to create it </li>
 *   <li><pre>create</pre>: load and create a class, print its string value</li>
 *   <li><pre>printresource</pre>: load a resource then print it to stdout</li>
 *   <li><pre>resource</pre>: load a resource then print the URL of that
 *   resource</li>
 * </ul>
 *
 * It returns an error code if a class/resource cannot be loaded/found
 * -and optionally a class may be requested as being loaded.
 * The latter action will call the class's constructor -it must support an
 * empty constructor); any side effects from the
 * constructor or static initializers will take place.
 *
 * All error messages are printed to {@link System#out}; errors
 * to {@link System#err}.
 * 
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public final class FindClass extends Configured implements Tool {

  /**
   * create command: {@value}
   */
  public static final String A_CREATE = "create";

  /**
   * Load command: {@value}
   */
  public static final String A_LOAD = "load";

  /**
   * Command to locate a resource: {@value}
   */
  public static final String A_RESOURCE = "locate";

  /**
   * Command to locate and print a resource: {@value}
   */
  public static final String A_PRINTRESOURCE = "print";

  /**
   * Exit code when the operation succeeded: {@value}
   */
  public static final int SUCCESS = 0;

  /**
   * generic error {@value}
   */
  protected static final int E_GENERIC = 1;

  /**
   * usage error -bad arguments or similar {@value}
   */
  protected static final int E_USAGE = 2;

  /**
   * class or resource not found {@value}
   */
  protected static final int E_NOT_FOUND = 3;

  /**
   * class load failed {@value}
   */
  protected static final int E_LOAD_FAILED = 4;

  /**
   * class creation failed {@value}
   */
  protected static final int E_CREATE_FAILED = 5;

  /**
   * Output stream. Defaults to {@link System#out}
   */
  private static PrintStream stdout = System.out;
  
  /**
   * Error stream. Defaults to {@link System#err}
   */
  private static PrintStream stderr = System.err;

  /**
   * Empty constructor; passes a new Configuration
   * object instance to its superclass's constructor
   */
  public FindClass() {
    super(new Configuration());
  }

  /**
   * Create a class with a specified configuration
   * @param conf configuration
   */
  public FindClass(Configuration conf) {
    super(conf);
  }

  /**
   * Change the output streams to be something other than the 
   * System.out and System.err streams
   * @param out new stdout stream
   * @param err new stderr stream
   */
  @VisibleForTesting
  public static void setOutputStreams(PrintStream out, PrintStream err) {
    stdout = out;
    stderr = err;
  }

  /**
   * Get a class fromt the configuration
   * @param name the class name
   * @return the class
   * @throws ClassNotFoundException if the class was not found
   * @throws Error on other classloading problems
   */
  private Class getClass(String name) throws ClassNotFoundException {
    return getConf().getClassByName(name);
  }

  /**
   * Get the resource
   * @param name resource name
   * @return URL or null for not found
   */
  private URL getResource(String name) {
    return getConf().getResource(name);
  }

  /**
   * Load a resource
   * @param name resource name
   * @return the status code
   */
  private int loadResource(String name) {
    URL url = getResource(name);
    if (url == null) {
      err("Resource not found: %s", name);
      return E_NOT_FOUND;
    }
    out("%s: %s", name, url);
    return SUCCESS;
  }

  /**
   * Dump a resource to out
   * @param name resource name
   * @return the status code
   */
  @SuppressWarnings("NestedAssignment")
  private int dumpResource(String name) {
    URL url = getResource(name);
    if (url == null) {
      err("Resource not found:" + name);
      return E_NOT_FOUND;
    }
    try {
      //open the resource
      InputStream instream = url.openStream();
      //read it in and print
      int data;
      while (-1 != (data = instream.read())) {
        stdout.print((char) data);
      }
      //end of file
      stdout.print('\n');
      return SUCCESS;
    } catch (IOException e) {
      printStack(e, "Failed to read resource %s at URL %s", name, url);
      return E_LOAD_FAILED;
    }
  }

  /**
   * print something to stderr
   * @param s string to print
   */
  private static void err(String s, Object... args) {
    stderr.format(s, args);
    stderr.print('\n');
  }

  /**
   * print something to stdout
   * @param s string to print
   */
  private static void out(String s, Object... args) {
    stdout.format(s, args);
    stdout.print('\n');
  }

  /**
   * print a stack trace with text
   * @param e the exception to print
   * @param text text to print
   */
  private static void printStack(Throwable e, String text, Object... args) {
    err(text, args);
    e.printStackTrace(stderr);
  }

  /**
   * Loads the class of the given name
   * @param name classname
   * @return outcome code
   */
  private int loadClass(String name) {
    try {
      Class clazz = getClass(name);
      loadedClass(name, clazz);
      return SUCCESS;
    } catch (ClassNotFoundException e) {
      printStack(e, "Class not found " + name);
      return E_NOT_FOUND;
    } catch (Exception e) {
      printStack(e, "Exception while loading class " + name);
      return E_LOAD_FAILED;
    } catch (Error e) {
      printStack(e, "Error while loading class " + name);
      return E_LOAD_FAILED;
    }
  }

  /**
   * Log that a class has been loaded, and where from.
   * @param name classname
   * @param clazz class
   */
  private void loadedClass(String name, Class clazz) {
    out("Loaded %s as %s", name, clazz);
    CodeSource source = clazz.getProtectionDomain().getCodeSource();
    URL url = source.getLocation();
    out("%s: %s", name, url);
  }

  /**
   * Create an instance of a class
   * @param name classname
   * @return the outcome
   */
  private int createClassInstance(String name) {
    try {
      Class clazz = getClass(name);
      loadedClass(name, clazz);
      Object instance = clazz.newInstance();
      try {
        //stringify
        out("Created instance " + instance.toString());
      } catch (Exception e) {
        //catch those classes whose toString() method is brittle, but don't fail the probe
        printStack(e,
                   "Created class instance but the toString() operator failed");
      }
      return SUCCESS;
    } catch (ClassNotFoundException e) {
      printStack(e, "Class not found " + name);
      return E_NOT_FOUND;
    } catch (Exception e) {
      printStack(e, "Exception while creating class " + name);
      return E_CREATE_FAILED;
    } catch (Error e) {
      printStack(e, "Exception while creating class " + name);
      return E_CREATE_FAILED;
    }
  }

  /**
   * Run the class/resource find or load operation
   * @param args command specific arguments.
   * @return the outcome
   * @throws Exception if something went very wrong
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      return usage(args);
    }
    String action = args[0];
    String name = args[1];
    int result;
    if (A_LOAD.equals(action)) {
      result = loadClass(name);
    } else if (A_CREATE.equals(action)) {
      //first load to separate load errors from create
      result = loadClass(name);
      if (result == SUCCESS) {
        //class loads, so instantiate it
        result = createClassInstance(name);
      }
    } else if (A_RESOURCE.equals(action)) {
      result = loadResource(name);
    } else if (A_PRINTRESOURCE.equals(action)) {
      result = dumpResource(name);
    } else {
      result = usage(args);
    }
    return result;
  }

  /**
   * Print a usage message
   * @param args the command line arguments
   * @return an exit code
   */
  private int usage(String[] args) {
    err(
      "Usage : [load | create] <classname>");
    err(
      "        [locate | print] <resourcename>]");
    err("The return codes are:");
    explainResult(SUCCESS,
                  "The operation was successful");
    explainResult(E_GENERIC,
                  "Something went wrong");
    explainResult(E_USAGE,
                  "This usage message was printed");
    explainResult(E_NOT_FOUND,
                  "The class or resource was not found");
    explainResult(E_LOAD_FAILED,
                  "The class was found but could not be loaded");
    explainResult(E_CREATE_FAILED,
                  "The class was loaded, but an instance of it could not be created");
    return E_USAGE;
  }

  /**
   * Explain an error code as part of the usage
   * @param errorcode error code returned
   * @param text error text
   */
  private void explainResult(int errorcode, String text) {
    err(" %2d -- %s ", errorcode , text);
  }

  /**
   * Main entry point. 
   * Runs the class via the {@link ToolRunner}, then
   * exits with an appropriate exit code. 
   * @param args argument list
   */
  public static void main(String[] args) {
    try {
      int result = ToolRunner.run(new FindClass(), args);
      System.exit(result);
    } catch (Exception e) {
      printStack(e, "Running FindClass");
      System.exit(E_GENERIC);
    }
  }
}
