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
package org.apache.hadoop.fs.shell;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parse the args of a command and check the format of args.
 */
public class CommandFormat {
  final String name;
  final int minPar, maxPar;
  final Map<String, Boolean> options = new HashMap<String, Boolean>();
  boolean ignoreUnknownOpts = false;
  
  /** constructor */
  public CommandFormat(String n, int min, int max, String ... possibleOpt) {
    name = n;
    minPar = min;
    maxPar = max;
    for (String opt : possibleOpt) {
      if (opt == null) {
        ignoreUnknownOpts = true;
      } else {
        options.put(opt, Boolean.FALSE);
      }
    }
  }

  /** Parse parameters starting from the given position
   * Consider using the variant that directly takes a List
   * 
   * @param args an array of input arguments
   * @param pos the position at which starts to parse
   * @return a list of parameters
   */
  public List<String> parse(String[] args, int pos) {
    List<String> parameters = new ArrayList<String>(Arrays.asList(args));
    parameters.subList(0, pos).clear();
    parse(parameters);
    return parameters;
  }

  /** Parse parameters from the given list of args.  The list is
   *  destructively modified to remove the options.
   * 
   * @param args as a list of input arguments
   */
  public void parse(List<String> args) {
    int pos = 0;
    while (pos < args.size()) {
      String arg = args.get(pos);
      if (arg.startsWith("-") && arg.length() > 1) {
        String opt = arg.substring(1);
        if (options.containsKey(opt)) {
          args.remove(pos);
          options.put(opt, Boolean.TRUE);
          continue;
        }
        if (!ignoreUnknownOpts) throw new UnknownOptionException(arg);
      }
      pos++;
    }
    int psize = args.size();
    if (psize < minPar) {
      throw new NotEnoughArgumentsException(minPar, psize);
    }
    if (psize > maxPar) {
      throw new TooManyArgumentsException(maxPar, psize);
    }
  }
  
  /** Return if the option is set or not
   * 
   * @param option String representation of an option
   * @return true is the option is set; false otherwise
   */
  public boolean getOpt(String option) {
    return options.containsKey(option) ? options.get(option) : false;
  }
  
  /** Returns all the options that are set
   * 
   * @return Set<String> of the enabled options
   */
  public Set<String> getOpts() {
    Set<String> optSet = new HashSet<String>();
    for (Map.Entry<String, Boolean> entry : options.entrySet()) {
      if (entry.getValue()) {
        optSet.add(entry.getKey());
      }
    }
    return optSet;
  }
  
  /** Used when the arguments exceed their bounds 
   */
  public static abstract class IllegalNumberOfArgumentsException
  extends IllegalArgumentException {
    private static final long serialVersionUID = 0L;
    protected int expected;
    protected int actual;

    protected IllegalNumberOfArgumentsException(int want, int got) {
      expected = want;
      actual = got;
    }

    public String getMessage() {
      return "expected " + expected + " but got " + actual;
    }
  }

  /** Used when too many arguments are supplied to a command
   */
  public static class TooManyArgumentsException
  extends IllegalNumberOfArgumentsException {
    private static final long serialVersionUID = 0L;

    public TooManyArgumentsException(int expected, int actual) {
      super(expected, actual);
    }

    public String getMessage() {
      return "Too many arguments: " + super.getMessage();
    }
  }
  
  /** Used when too few arguments are supplied to a command
   */
  public static class NotEnoughArgumentsException
  extends IllegalNumberOfArgumentsException {
    private static final long serialVersionUID = 0L;

    public NotEnoughArgumentsException(int expected, int actual) {
      super(expected, actual);
    }

    public String getMessage() {
      return "Not enough arguments: " + super.getMessage();
    }
  }
  
  /** Used when an unsupported option is supplied to a command
   */
  public static class UnknownOptionException extends IllegalArgumentException {
    private static final long serialVersionUID = 0L;
    protected String option = null;
    
    public UnknownOptionException(String unknownOption) {
      super("Illegal option " + unknownOption);
      option = unknownOption;
    }
    
    public String getOption() {
      return option;
    }
  }
}
