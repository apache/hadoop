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
package org.apache.hadoop.ha;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * This class parses the configured list of fencing methods, and
 * is responsible for trying each one in turn while logging informative
 * output.<p>
 * 
 * The fencing methods are configured as a carriage-return separated list.
 * Each line in the list is of the form:<p>
 * <code>com.example.foo.MyMethod(arg string)</code>
 * or
 * <code>com.example.foo.MyMethod</code>
 * The class provided must implement the {@link FenceMethod} interface.
 * The fencing methods that ship with Hadoop may also be referred to
 * by shortened names:<p>
 * <ul>
 * <li><code>shell(/path/to/some/script.sh args...)</code></li>
 * <li><code>sshfence(...)</code> (see {@link SshFenceByTcpPort})
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NodeFencer {
  private static final String CLASS_RE = "([a-zA-Z0-9\\.\\$]+)";
  private static final Pattern CLASS_WITH_ARGUMENT =
    Pattern.compile(CLASS_RE + "\\((.+?)\\)");
  private static final Pattern CLASS_WITHOUT_ARGUMENT =
    Pattern.compile(CLASS_RE);
  private static final Pattern HASH_COMMENT_RE =
    Pattern.compile("#.*$");

  private static final Log LOG = LogFactory.getLog(NodeFencer.class);

  /**
   * Standard fencing methods included with Hadoop.
   */
  private static final Map<String, Class<? extends FenceMethod>> STANDARD_METHODS =
    ImmutableMap.<String, Class<? extends FenceMethod>>of(
        "shell", ShellCommandFencer.class,
        "sshfence", SshFenceByTcpPort.class);
  
  private final List<FenceMethodWithArg> methods;
  
  NodeFencer(Configuration conf, String spec)
      throws BadFencingConfigurationException {
    this.methods = parseMethods(conf, spec);
  }
  
  public static NodeFencer create(Configuration conf, String confKey)
      throws BadFencingConfigurationException {
    String confStr = conf.get(confKey);
    if (confStr == null) {
      return null;
    }
    return new NodeFencer(conf, confStr);
  }

  public boolean fence(HAServiceTarget fromSvc) {
    LOG.info("====== Beginning Service Fencing Process... ======");
    int i = 0;
    for (FenceMethodWithArg method : methods) {
      LOG.info("Trying method " + (++i) + "/" + methods.size() +": " + method);
      
      try {
        if (method.method.tryFence(fromSvc, method.arg)) {
          LOG.info("====== Fencing successful by method " + method + " ======");
          return true;
        }
      } catch (BadFencingConfigurationException e) {
        LOG.error("Fencing method " + method + " misconfigured", e);
        continue;
      } catch (Throwable t) {
        LOG.error("Fencing method " + method + " failed with an unexpected error.", t);
        continue;
      }
      LOG.warn("Fencing method " + method + " was unsuccessful.");
    }
    
    LOG.error("Unable to fence service by any configured method.");
    return false;
  }

  private static List<FenceMethodWithArg> parseMethods(Configuration conf,
      String spec)
      throws BadFencingConfigurationException {
    String[] lines = spec.split("\\s*\n\\s*");
    
    List<FenceMethodWithArg> methods = Lists.newArrayList();
    for (String line : lines) {
      line = HASH_COMMENT_RE.matcher(line).replaceAll("");
      line = line.trim();
      if (!line.isEmpty()) {
        methods.add(parseMethod(conf, line));
      }
    }
    
    return methods;
  }

  private static FenceMethodWithArg parseMethod(Configuration conf, String line)
      throws BadFencingConfigurationException {
    Matcher m;
    if ((m = CLASS_WITH_ARGUMENT.matcher(line)).matches()) {
      String className = m.group(1);
      String arg = m.group(2);
      return createFenceMethod(conf, className, arg);
    } else if ((m = CLASS_WITHOUT_ARGUMENT.matcher(line)).matches()) {
      String className = m.group(1);
      return createFenceMethod(conf, className, null);
    } else {
      throw new BadFencingConfigurationException(
          "Unable to parse line: '" + line + "'");
    }
  }

  private static FenceMethodWithArg createFenceMethod(
      Configuration conf, String clazzName, String arg)
      throws BadFencingConfigurationException {

    Class<?> clazz;
    try {
      // See if it's a short name for one of the built-in methods
      clazz = STANDARD_METHODS.get(clazzName);
      if (clazz == null) {
        // Try to instantiate the user's custom method
        clazz = Class.forName(clazzName);
      }
    } catch (Exception e) {
      throw new BadFencingConfigurationException(
          "Could not find configured fencing method " + clazzName,
          e);
    }
    
    // Check that it implements the right interface
    if (!FenceMethod.class.isAssignableFrom(clazz)) {
      throw new BadFencingConfigurationException("Class " + clazzName +
          " does not implement FenceMethod");
    }
    
    FenceMethod method = (FenceMethod)ReflectionUtils.newInstance(
        clazz, conf);
    method.checkArgs(arg);
    return new FenceMethodWithArg(method, arg);
  }
  
  private static class FenceMethodWithArg {
    private final FenceMethod method;
    private final String arg;
    
    private FenceMethodWithArg(FenceMethod method, String arg) {
      this.method = method;
      this.arg = arg;
    }
    
    @Override
    public String toString() {
      return method.getClass().getCanonicalName() + "(" + arg + ")";
    }
  }
}
