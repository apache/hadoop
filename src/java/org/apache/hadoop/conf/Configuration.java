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

package org.apache.hadoop.conf;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.URL;
import java.io.*;

import javax.xml.parsers.*;

import org.w3c.dom.*;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.*;

import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Provides access to configuration parameters.  Configurations are specified
 * by resources.  A resource contains a set of name/value pairs.
 *
 * <p>Each resource is named by either a String or by a Path.  If named by a
 * String, then the classpath is examined for a file with that name.  If a
 * File, then the local filesystem is examined directly, without referring to
 * the CLASSPATH.
 *
 * <p>Configuration resources are of two types: default and
 * final.  Default values are loaded first and final values are loaded last, and
 * thus override default values.
 *
 * <p>Hadoop's default resource is the String "hadoop-default.xml" and its
 * final resource is the String "hadoop-site.xml".  Other tools built on Hadoop
 * may specify additional resources.
 * 
 * <p>The values returned by most <tt>get*</tt> methods are based on String representations. 
 * This String is processed for <b>variable expansion</b>. The available variables are the 
 * <em>System properties</em> and the <em>other properties</em> defined in this Configuration.
 * <p>The only <tt>get*</tt> method that is not processed for variable expansion is
 * {@link #getObject(String)} (as it cannot assume that the returned values are String). 
 * You can use <tt>getObject</tt> to obtain the raw value of a String property without 
 * variable expansion: if <tt>(String)conf.getObject("my.jdk")</tt> is <tt>"JDK ${java.version}"</tt>
 * then conf.get("my.jdk")</tt> is <tt>"JDK 1.5.0"</tt> 
 * <p> Example XML config using variables:<br><tt>
 * &lt;name>basedir&lt;/name>&lt;value>/user/${user.name}&lt;/value><br> 
 * &lt;name>tempdir&lt;/name>&lt;value>${basedir}/tmp&lt;/value><br>
 * </tt>When conf.get("tempdir") is called:<br>
 * <tt>${basedir}</tt> is resolved to another property in this Configuration.
 * Then <tt>${user.name}</tt> is resolved to a System property.
 */
public class Configuration {
  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.conf.Configuration");

  private boolean   quietmode = false;
  private ArrayList defaultResources = new ArrayList();
  private ArrayList finalResources = new ArrayList();

  private Properties properties;
  private Properties overlay;
  private ClassLoader classLoader;
  {
    classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = Configuration.class.getClassLoader();
    }
  }
  
  /** A new configuration. */
  public Configuration() {
    if (LOG.isDebugEnabled()) {
      LOG.debug(StringUtils.stringifyException(new IOException("config()")));
    }
    defaultResources.add("hadoop-default.xml");
    finalResources.add("hadoop-site.xml");
  }

  /** A new configuration with the same settings cloned from another. */
  public Configuration(Configuration other) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(StringUtils.stringifyException
                (new IOException("config(config)")));
    }
    this.defaultResources = (ArrayList)other.defaultResources.clone();
    this.finalResources = (ArrayList)other.finalResources.clone();
    if (other.properties != null)
      this.properties = (Properties)other.properties.clone();
    if(other.overlay!=null)
      this.overlay = (Properties)other.overlay.clone();
  }

  /** Add a default resource. */
  public void addDefaultResource(String name) {
    addResource(defaultResources, name);
  }

  /** Add a default resource. */
  public void addDefaultResource(URL url) {
    addResource(defaultResources, url);
  }

  /** Add a default resource. */
  public void addDefaultResource(Path file) {
    addResource(defaultResources, file);
  }

  /** Add a final resource. */
  public void addFinalResource(String name) {
    addResource(finalResources, name);
  }

  /** Add a final resource. */
  public void addFinalResource(URL url) {
    addResource(finalResources, url);
  }

  /** Add a final resource. */
  public void addFinalResource(Path file) {
    addResource(finalResources, file);
  }

  private synchronized void addResource(ArrayList resources, Object resource) {
    resources.add(resource);                      // add to resources
    properties = null;                            // trigger reload
  }
  
  /**
   * Returns the value of the <code>name</code> property, or null if no such
   * property exists.
   */
  public Object getObject(String name) { return getProps().get(name);}

  /** Sets the value of the <code>name</code> property. */
  public void setObject(String name, Object value) {
    getProps().put(name, value);
  }

  /** Returns the value of the <code>name</code> property.  If no such property
   * exists, then <code>defaultValue</code> is returned.
   */
  public Object get(String name, Object defaultValue) {
    Object res = getObject(name);
    if (res != null) return res;
    else return defaultValue;
  }
  
  private static Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");
  private static int MAX_SUBST = 20;

  private String substituteVars(String expr) {
    if(expr == null) {
      return null;
    }
    Matcher match = varPat.matcher("");
    String eval = expr;
    for(int s=0; s<MAX_SUBST; s++) {
      match.reset(eval);
      if(! match.find()) {
        return eval;
      }
      String var = match.group();
      var = var.substring(2, var.length()-1); // remove ${ .. }
      String val = System.getProperty(var);
      if(val == null) {
        val = (String)this.getObject(var);
      }
      if(val == null) {
        return eval; // return literal ${var}: var is unbound
      }
      // substitute
      eval = eval.substring(0, match.start())+val+eval.substring(match.end());
    }
    throw new IllegalStateException("Variable substitution depth too large: " 
                                    + MAX_SUBST + " " + expr);
  }
  
  /** Returns the value of the <code>name</code> property, or null if no
   * such property exists. */
  public String get(String name) {
    return substituteVars(getProps().getProperty(name));
  }

  /** Sets the value of the <code>name</code> property. */
  public void set(String name, Object value) {
    getOverlay().setProperty(name, value.toString());
    getProps().setProperty(name, value.toString());
  }
  
  private synchronized Properties getOverlay() {
    if(overlay==null){
      overlay=new Properties();
    }
    return overlay;
  }

  /** Returns the value of the <code>name</code> property.  If no such property
   * exists, then <code>defaultValue</code> is returned.
   */
  public String get(String name, String defaultValue) {
     return substituteVars(getProps().getProperty(name, defaultValue));
  }
    
  /** Returns the value of the <code>name</code> property as an integer.  If no
   * such property is specified, or if the specified value is not a valid
   * integer, then <code>defaultValue</code> is returned.
   */
  public int getInt(String name, int defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Integer.parseInt(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /** Sets the value of the <code>name</code> property to an integer. */
  public void setInt(String name, int value) {
    set(name, Integer.toString(value));
  }


  /** Returns the value of the <code>name</code> property as a long.  If no
   * such property is specified, or if the specified value is not a valid
   * long, then <code>defaultValue</code> is returned.
   */
  public long getLong(String name, long defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Long.parseLong(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /** Sets the value of the <code>name</code> property to a long. */
  public void setLong(String name, long value) {
    set(name, Long.toString(value));
  }

  /** Returns the value of the <code>name</code> property as a float.  If no
   * such property is specified, or if the specified value is not a valid
   * float, then <code>defaultValue</code> is returned.
   */
  public float getFloat(String name, float defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Float.parseFloat(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /** Returns the value of the <code>name</code> property as an boolean.  If no
   * such property is specified, or if the specified value is not a valid
   * boolean, then <code>defaultValue</code> is returned.  Valid boolean values
   * are "true" and "false".
   */
  public boolean getBoolean(String name, boolean defaultValue) {
    String valueString = get(name);
    if ("true".equals(valueString))
      return true;
    else if ("false".equals(valueString))
      return false;
    else return defaultValue;
  }

  /** Sets the value of the <code>name</code> property to an integer. */
  public void setBoolean(String name, boolean value) {
    set(name, Boolean.toString(value));
  }

  /** Returns the value of the <code>name</code> property as an array of
   * strings.  If no such property is specified, then <code>null</code>
   * is returned.  Values are comma delimited.
   */
  public String[] getStrings(String name) {
    String valueString = get(name);
    return StringUtils.getStrings(valueString);
  }

  /**
   * Load a class by name.
   * @param name the class name
   * @return the class object
   * @throws ClassNotFoundException if the class is not found
   */
  public Class getClassByName(String name) throws ClassNotFoundException {
    return Class.forName(name, true, classLoader);
  }
  
  /** Returns the value of the <code>name</code> property as a Class.  If no
   * such property is specified, then <code>defaultValue</code> is returned.
   */
  public Class getClass(String name, Class defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return getClassByName(valueString);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /** Returns the value of the <code>name</code> property as a Class.  If no
   * such property is specified, then <code>defaultValue</code> is returned.
   * An error is thrown if the returned class does not implement the named
   * interface. 
   */
  public Class getClass(String propertyName, Class defaultValue,Class xface) {
    try {
      Class theClass = getClass(propertyName, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass))
        throw new RuntimeException(theClass+" not "+xface.getName());
      return theClass;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Sets the value of the <code>name</code> property to the name of a class.
   * First checks that the class implements the named interface. 
   */
  public void setClass(String propertyName, Class theClass, Class xface) {
    if (!xface.isAssignableFrom(theClass))
      throw new RuntimeException(theClass+" not "+xface.getName());
    set(propertyName, theClass.getName());
  }

  /** Returns a local file under a directory named in <i>dirsProp</i> with
   * the given <i>path</i>.  If <i>dirsProp</i> contains multiple directories,
   * then one is chosen based on <i>path</i>'s hash code.  If the selected
   * directory does not exist, an attempt is made to create it.
   */
  public Path getLocalPath(String dirsProp, String path)
    throws IOException {
    String[] dirs = getStrings(dirsProp);
    int hashCode = path.hashCode();
    FileSystem fs = FileSystem.getNamed("local", this);
    for (int i = 0; i < dirs.length; i++) {  // try each local dir
      int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
      Path file = new Path(dirs[index], path);
      Path dir = file.getParent();
      if (fs.mkdirs(dir) || fs.exists(dir)) {
        return file;
      }
    }
    LOG.warn("Could not make " + path + 
                " in local directories from " + dirsProp);
    for(int i=0; i < dirs.length; i++) {
      int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
      LOG.warn(dirsProp + "[" + index + "]=" + dirs[index]);
    }
    throw new IOException("No valid local directories in property: "+dirsProp);
  }

  /** Returns a local file name under a directory named in <i>dirsProp</i> with
   * the given <i>path</i>.  If <i>dirsProp</i> contains multiple directories,
   * then one is chosen based on <i>path</i>'s hash code.  If the selected
   * directory does not exist, an attempt is made to create it.
   */
  public File getFile(String dirsProp, String path)
    throws IOException {
    String[] dirs = getStrings(dirsProp);
    int hashCode = path.hashCode();
    for (int i = 0; i < dirs.length; i++) {  // try each local dir
      int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
      File file = new File(dirs[index], path);
      File dir = file.getParentFile();
      if (dir.exists() || dir.mkdirs()) {
        return file;
      }
    }
    throw new IOException("No valid local directories in property: "+dirsProp);
  }



  /** Returns the URL for the named resource. */
  public URL getResource(String name) {
    return classLoader.getResource(name);
  }
  /** Returns an input stream attached to the configuration resource with the
   * given <code>name</code>.
   */
  public InputStream getConfResourceAsInputStream(String name) {
    try {
      URL url= getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return url.openStream();
    } catch (Exception e) {
      return null;
    }
  }

  /** Returns a reader attached to the configuration resource with the
   * given <code>name</code>.
   */
  public Reader getConfResourceAsReader(String name) {
    try {
      URL url= getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return new InputStreamReader(url.openStream());
    } catch (Exception e) {
      return null;
    }
  }

  private synchronized Properties getProps() {
    if (properties == null) {
      Properties newProps = new Properties();
      loadResources(newProps, defaultResources, false, quietmode);
      loadResources(newProps, finalResources, true, true);
      properties = newProps;
      if(overlay!=null)
        properties.putAll(overlay);
    }
    return properties;
  }

  /** @return Iterator&lt; Map.Entry&lt;String,String> >  */
  public Iterator entries() {
    return getProps().entrySet().iterator();
  }

  private void loadResources(Properties props,
                             ArrayList resources,
                             boolean reverse, boolean quiet) {
    ListIterator i = resources.listIterator(reverse ? resources.size() : 0);
    while (reverse ? i.hasPrevious() : i.hasNext()) {
      loadResource(props, reverse ? i.previous() : i.next(), quiet);
    }
  }

  private void loadResource(Properties properties, Object name, boolean quiet) {
    try {
      DocumentBuilder builder =
        DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = null;


      if (name instanceof URL) {                  // an URL resource
        URL url = (URL)name;
        if (url != null) {
          if (!quiet) {
            LOG.info("parsing " + url);
          }
          doc = builder.parse(url.toString());
        }
      } else if (name instanceof String) {        // a CLASSPATH resource
        URL url = getResource((String)name);
        if (url != null) {
          if (!quiet) {
            LOG.info("parsing " + url);
          }
          doc = builder.parse(url.toString());
        }
      } else if (name instanceof Path) {          // a file resource
        Path file = (Path)name;
        FileSystem fs = FileSystem.getNamed("local", this);
        if (fs.exists(file)) {
          if (!quiet) {
            LOG.info("parsing " + file);
          }
          InputStream in = new BufferedInputStream(fs.openRaw(file));
          try {
            doc = builder.parse(in);
          } finally {
            in.close();
          }
        }
      }

      if (doc == null) {
        if (quiet)
          return;
        throw new RuntimeException(name + " not found");
      }

      Element root = doc.getDocumentElement();
      if (!"configuration".equals(root.getTagName()))
        LOG.fatal("bad conf file: top-level element not <configuration>");
      NodeList props = root.getChildNodes();
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element))
          continue;
        Element prop = (Element)propNode;
        if (!"property".equals(prop.getTagName()))
          LOG.warn("bad conf file: element not <property>");
        NodeList fields = prop.getChildNodes();
        String attr = null;
        String value = null;
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element)fieldNode;
          if ("name".equals(field.getTagName()))
            attr = ((Text)field.getFirstChild()).getData();
          if ("value".equals(field.getTagName()) && field.hasChildNodes())
            value = ((Text)field.getFirstChild()).getData();
        }
        if (attr != null && value != null)
          properties.setProperty(attr, value);
      }
        
    } catch (Exception e) {
      LOG.fatal("error parsing conf file: " + e);
      throw new RuntimeException(e);
    }
    
  }

  /** Writes non-default properties in this configuration.*/
  public void write(OutputStream out) throws IOException {
    Properties properties = getProps();
    try {
      Document doc =
        DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
      Element conf = doc.createElement("configuration");
      doc.appendChild(conf);
      conf.appendChild(doc.createTextNode("\n"));
      for (Enumeration e = properties.keys(); e.hasMoreElements();) {
        String name = (String)e.nextElement();
        Object object = properties.get(name);
        String value = null;
        if(object instanceof String) {
          value = (String) object;
        }else {
          continue;
        }
        Element propNode = doc.createElement("property");
        conf.appendChild(propNode);
      
        Element nameNode = doc.createElement("name");
        nameNode.appendChild(doc.createTextNode(name));
        propNode.appendChild(nameNode);
      
        Element valueNode = doc.createElement("value");
        valueNode.appendChild(doc.createTextNode(value));
        propNode.appendChild(valueNode);

        conf.appendChild(doc.createTextNode("\n"));
      }
    
      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(out);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();
      transformer.transform(source, result);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the class loader for this job.
   * @return the correct class loader
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }
  
  /**
   * Set the class loader that will be used to load the various objects.
   * @param classLoader the new class loader
   */
  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }
  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("Configuration: ");
    sb.append("defaults: ");
    toString(defaultResources, sb);
    sb.append("final: ");
    toString(finalResources, sb);
    return sb.toString();
  }

  private void toString(ArrayList resources, StringBuffer sb) {
    ListIterator i = resources.listIterator();
    while (i.hasNext()) {
      if (i.nextIndex() != 0) {
        sb.append(" , ");
      }
      sb.append(i.next());
    }
  }

  /** Make this class quiet. Error and informational
   *  messages might not be logged.
   */
  public void setQuietMode(boolean value) {
    quietmode = value;
  }

  /** For debugging.  List non-default properties to the terminal and exit. */
  public static void main(String[] args) throws Exception {
    new Configuration().write(System.out);
  }

}
