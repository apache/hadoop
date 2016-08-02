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

package org.apache.slider.common.tools;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.core.exceptions.BadConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Methods to aid in config, both in the Configuration class and
 * with other parts of setting up Slider-initated processes.
 * 
 * Some of the methods take an argument of a map iterable for their sources; this allows
 * the same method
 */
public class ConfigHelper {
  private static final Logger log = LoggerFactory.getLogger(ConfigHelper.class);

  private static AtomicBoolean sliderResourceInjected =
      new AtomicBoolean(false);
  private static AtomicBoolean sliderResourceInjectionAttempted =
      new AtomicBoolean(false);
  
  /**
   * Dump the (sorted) configuration
   * @param conf config
   * @return the sorted keyset
   */
  public static Set<String> dumpConf(Configuration conf) {
    Set<String> keys = sortedConfigKeys(conf);
    for (String key : keys) {
      log.info("{}={}", key, conf.get(key));
    }
    return keys;
  }

  /**
   * Take a configuration and return a sorted set
   * @param conf config
   * @return the sorted keyset

   */
  public static Set<String> sortedConfigKeys(Iterable<Map.Entry<String, String>> conf) {
    TreeSet<String> sorted = new TreeSet<String>();
    for (Map.Entry<String, String> entry : conf) {
      sorted.add(entry.getKey());
    }
    return sorted;
  }

  /**
   * Set an entire map full of values
   *
   * @param config config to patch
   * @param map map of data
   * @param origin origin data
   */
  public static void addConfigMap(Configuration config,
                                  Map<String, String> map,
                                  String origin) throws BadConfigException {
    addConfigMap(config, map.entrySet(), origin);
  }
  
  /**
   * Set an entire map full of values
   *
   * @param config config to patch
   * @param map map of data
   * @param origin origin data
   */
  public static void addConfigMap(Configuration config,
                                  Iterable<Map.Entry<String, String>> map,
                                  String origin) throws BadConfigException {
    for (Map.Entry<String, String> mapEntry : map) {
      String key = mapEntry.getKey();
      String value = mapEntry.getValue();
      if (value == null) {
        throw new BadConfigException("Null value for property " + key);
      }
      config.set(key, value, origin);
    }
  }


  /**
   * Save a config file in a destination directory on a given filesystem
   * @param systemConf system conf used for creating filesystems
   * @param confToSave config to save
   * @param confdir the directory path where the file is to go
   * @param filename the filename
   * @return the destination path where the file was saved
   * @throws IOException IO problems
   */
  public static Path saveConfig(Configuration systemConf,
                                Configuration confToSave,
                                Path confdir,
                                String filename) throws IOException {
    FileSystem fs = FileSystem.get(confdir.toUri(), systemConf);
    Path destPath = new Path(confdir, filename);
    saveConfig(fs, destPath, confToSave);
    return destPath;
  }

  /**
   * Save a config
   * @param fs filesystem
   * @param destPath dest to save
   * @param confToSave  config to save
   * @throws IOException IO problems
   */
  public static void saveConfig(FileSystem fs,
                                Path destPath,
                                Configuration confToSave) throws
                                                              IOException {
    FSDataOutputStream fos = fs.create(destPath);
    try {
      confToSave.writeXml(fos);
    } finally {
      IOUtils.closeStream(fos);
    }
  }

  /**
   * Convert to an XML string
   * @param conf configuration
   * @return conf
   * @throws IOException
   */
  public static String toXml(Configuration conf) throws IOException {
    StringWriter writer = new StringWriter();
    conf.writeXml(writer);
    return writer.toString();
  }
  
  /**
   * This will load and parse a configuration to an XML document
   * @param fs filesystem
   * @param path path
   * @return an XML document
   * @throws IOException IO failure
   */
  public Document parseConfiguration(FileSystem fs,
                                     Path path) throws
                                                IOException {


    byte[] data = loadBytes(fs, path);
    //this is here to track down a parse issue
    //related to configurations
    String s = new String(data, 0, data.length);
    log.debug("XML resource {} is \"{}\"", path, s);
/* JDK7
    try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
      Document document = parseConfigXML(in);
      return document;
    } catch (ParserConfigurationException | SAXException e) {
      throw new IOException(e);
    }
*/
    ByteArrayInputStream in= null;
    try {
      in = new ByteArrayInputStream(data);
      Document document = parseConfigXML(in);
      return document;
    } catch (ParserConfigurationException e) {
      throw new IOException(e);
    } catch (SAXException e) {
      throw new IOException(e);
    } finally {
      IOUtils.closeStream(in);
    }
  }

  public static byte[] loadBytes(FileSystem fs, Path path) throws IOException {
    int len = (int) fs.getLength(path);
    byte[] data = new byte[len];
    /* JDK7
    try(FSDataInputStream in = fs.open(path)) {
      in.readFully(0, data);
    }
*/
    FSDataInputStream in = null;
    in = fs.open(path);
    try {
      in.readFully(0, data);
    } finally {
      IOUtils.closeStream(in);
    }
    return data;
  }

  /**
   * Load a configuration from ANY FS path. The normal Configuration
   * loader only works with file:// URIs
   * @param fs filesystem
   * @param path path
   * @return a loaded resource
   * @throws IOException
   */
  public static Configuration loadConfiguration(FileSystem fs,
                                                Path path) throws IOException {
    byte[] data = loadBytes(fs, path);

    ByteArrayInputStream in2;

    in2 = new ByteArrayInputStream(data);
    Configuration conf1 = new Configuration(false);
    conf1.addResource(in2);
    //now clone it while dropping all its sources
    Configuration conf2   = new Configuration(false);
    String src = path.toString();
    for (Map.Entry<String, String> entry : conf1) {
      String key = entry.getKey();
      String value = entry.getValue();
      conf2.set(key, value, src);
    }
    return conf2;
  }


  /**
   * Generate a config file in a destination directory on the local filesystem
   * @param confdir the directory path where the file is to go
   * @param filename the filename
   * @return the destination path
   */
  public static File saveConfig(Configuration generatingConf,
                                    File confdir,
                                    String filename) throws IOException {


    File destPath = new File(confdir, filename);
    OutputStream fos = new FileOutputStream(destPath);
    try {
      generatingConf.writeXml(fos);
    } finally {
      IOUtils.closeStream(fos);
    }
    return destPath;
  }

  /**
   * Parse an XML Hadoop configuration into an XML document. x-include
   * is supported, but as the location isn't passed in, relative
   * URIs are out.
   * @param in instream
   * @return a document
   * @throws ParserConfigurationException parser feature problems
   * @throws IOException IO problems
   * @throws SAXException XML is invalid
   */
  public static Document parseConfigXML(InputStream in) throws
                                               ParserConfigurationException,
                                               IOException,
                                               SAXException {
    DocumentBuilderFactory docBuilderFactory
      = DocumentBuilderFactory.newInstance();
    //ignore all comments inside the xml file
    docBuilderFactory.setIgnoringComments(true);

    //allow includes in the xml file
    docBuilderFactory.setNamespaceAware(true);
    docBuilderFactory.setXIncludeAware(true);
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    return builder.parse(in);
  }

  /**
   * Load a Hadoop configuration from a local file.
   * @param file file to load
   * @return a configuration which hasn't actually had the load triggered
   * yet.
   * @throws FileNotFoundException file is not there
   * @throws IOException any other IO problem
   */
  public static Configuration loadConfFromFile(File file) throws
                                                          IOException {
    return loadConfFromFile(file, false);
  }

  /**
   *
   * Load a Hadoop configuration from a local file.
   * @param file file to load
   * @param loadDefaults flag to indicate if the defaults should be loaded yet
   * @return a configuration which hasn't actually had the load triggered
   * yet.
   * @throws FileNotFoundException file is not there
   * @throws IOException any other IO problem
   */
  public static Configuration loadConfFromFile(File file,
      boolean loadDefaults) throws IOException {
    if (!file.exists()) {
      throw new FileNotFoundException("File not found :"
                                          + file.getAbsoluteFile());
    }
    Configuration conf = new Configuration(loadDefaults);
    try {
      conf.addResource(file.toURI().toURL());
    } catch (MalformedURLException e) {
      // should never happen...
      throw new IOException(
        "File " + file.toURI() + " doesn't have a valid URL");
    }
    return conf;
  }

  /**
   * Add a configuration from a file to an existing configuration
   * @param conf existing configuration
   * @param file file to load
   * @param overwrite flag to indicate new values should overwrite the predecessor
   * @return the merged configuration
   * @throws IOException
   */
  public static Configuration addConfigurationFile(Configuration conf,
      File file, boolean overwrite)
      throws IOException {
    Configuration c2 = loadConfFromFile(file, false);
    mergeConfigurations(conf, c2, file.getAbsolutePath(), overwrite);
    return conf;
  }

  /**
   * Add the system env variables with the given prefix (by convention, env.)
   * @param conf existing configuration
   * @param prefix prefix
   */
  public static void addEnvironmentVariables(Configuration conf, String prefix) {
    Map<String, String> env = System.getenv();
    for (Map.Entry<String, String> entry : env.entrySet()) {
      conf.set(prefix + entry.getKey(),entry.getValue(), "env");
    }
  }
  
  /**
   * looks for the config under $confdir/$templateFilename; if not there
   * loads it from /conf/templateFile.
   * The property {@link SliderKeys#KEY_TEMPLATE_ORIGIN} is set to the
   * origin to help debug what's happening
   * @param systemConf system conf
   * @param confdir conf dir in FS
   * @param templateFilename filename in the confdir
   * @param fallbackResource resource to fall back on
   * @return loaded conf
   * @throws IOException IO problems
   */
  public static Configuration loadTemplateConfiguration(Configuration systemConf,
                                                        Path confdir,
                                                        String templateFilename,
                                                        String fallbackResource) throws
                                                                         IOException {
    FileSystem fs = FileSystem.get(confdir.toUri(), systemConf);

    Path templatePath = new Path(confdir, templateFilename);
    return loadTemplateConfiguration(fs, templatePath, fallbackResource);
  }

  /**
   * looks for the config under $confdir/$templateFilename; if not there
   * loads it from /conf/templateFile.
   * The property {@link SliderKeys#KEY_TEMPLATE_ORIGIN} is set to the
   * origin to help debug what's happening.
   * @param fs Filesystem
   * @param templatePath HDFS path for template
   * @param fallbackResource resource to fall back on, or "" for no fallback
   * @return loaded conf
   * @throws IOException IO problems
   * @throws FileNotFoundException if the path doesn't have a file and there
   * was no fallback.
   */
  public static Configuration loadTemplateConfiguration(FileSystem fs,
                                                        Path templatePath,
                                                        String fallbackResource)
      throws IOException {
    Configuration conf;
    String origin;
    if (fs.exists(templatePath)) {
      log.debug("Loading template configuration {}", templatePath);
      conf = loadConfiguration(fs, templatePath);
      origin = templatePath.toString();
    } else {
      if (fallbackResource.isEmpty()) {
        throw new FileNotFoundException("No config file found at " + templatePath);
      }
      log.debug("Template {} not found" +
                " -reverting to classpath resource {}", templatePath, fallbackResource);
      conf = new Configuration(false);
      conf.addResource(fallbackResource);
      origin = "Resource " + fallbackResource;
    }
    //force a get
    conf.get(SliderXmlConfKeys.KEY_TEMPLATE_ORIGIN);
    //now set the origin
    conf.set(SliderXmlConfKeys.KEY_TEMPLATE_ORIGIN, origin);
    return conf;
  }


  /**
   * For testing: dump a configuration
   * @param conf configuration
   * @return listing in key=value style
   */
  public static String dumpConfigToString(Configuration conf) {
    Set<String> sorted = sortedConfigKeys(conf);

    StringBuilder builder = new StringBuilder();
    for (String key : sorted) {

      builder.append(key)
             .append("=")
             .append(conf.get(key))
             .append("\n");
    }
    return builder.toString();
  }

  /**
   * Merge in one configuration above another
   * @param base base config
   * @param merge one to merge. This MUST be a non-default-load config to avoid
   * merge origin confusion
   * @param origin description of the origin for the put operation
   * @param overwrite flag to indicate new values should overwrite the predecessor
   * @return the base with the merged values
   */
  public static Configuration mergeConfigurations(Configuration base,
      Iterable<Map.Entry<String, String>> merge,
      String origin,
      boolean overwrite) {
    for (Map.Entry<String, String> entry : merge) {
      String key = entry.getKey();
      if (overwrite || base.get(key) == null) {
        base.set(key, entry.getValue(), origin);
      }
    }
    return base;
  }

  /**
   * Register a resource as a default resource.
   * Do not attempt to use this unless you understand that the
   * order in which default resources are loaded affects the outcome,
   * and that subclasses of Configuration often register new default
   * resources
   * @param resource the resource name
   * @return the URL or null
   */
  public static URL registerDefaultResource(String resource) {
    URL resURL = getResourceUrl(resource);
    if (resURL != null) {
      Configuration.addDefaultResource(resource);
    }
    return resURL;
  }

  /**
   * Load a configuration from a resource on this classpath.
   * If the resource is not found, an empty configuration is returned
   * @param resource the resource name
   * @return the loaded configuration.
   */
  public static Configuration loadFromResource(String resource) {
    Configuration conf = new Configuration(false);
    URL resURL = getResourceUrl(resource);
    if (resURL != null) {
      log.debug("loaded resources from {}", resURL);
      conf.addResource(resource);
    } else{
      log.debug("failed to find {} on the classpath", resource);
    }
    return conf;
    
  }

  /**
   * Get the URL to a resource, null if not on the CP
   * @param resource resource to look for
   * @return the URL or null
   */
  public static URL getResourceUrl(String resource) {
    return ConfigHelper.class.getClassLoader()
                                  .getResource(resource);
  }

  /**
   * Load a resource that must be on the classpath
   * @param resource the resource name
   * @return the loaded configuration
   * @throws FileNotFoundException if the resource is missing
   */
  public static Configuration loadMandatoryResource(String resource)
      throws FileNotFoundException {
    Configuration conf = new Configuration(false);
    URL resURL = getResourceUrl(resource);
    if (resURL != null) {
      log.debug("loaded resources from {}", resURL);
      conf.addResource(resource);
    } else {
      throw new FileNotFoundException(resource);
    }
    return conf;
  }

  /**
   * Propagate a property from a source to a dest config, with a best-effort
   * attempt at propagating the origin.
   * If the 
   * @param dest destination
   * @param src source
   * @param key key to try to copy
   * @return true if the key was found and propagated
   */
  public static boolean propagate(Configuration dest,
                                  Configuration src,
                                  String key) {
    String val = src.get(key);
    if (val != null) {
      String[] origin = src.getPropertySources(key);
      if (origin != null && origin.length > 0) {
        dest.set(key, val, origin[0]);
      } else {
        dest.set(key, val);
        return true;
      }
    }
    return false;
  }


  /**
   * Take a configuration, return a hash map
   * @param conf conf
   * @return hash map
   */
  public static Map<String, String> buildMapFromConfiguration(Configuration conf) {
    Map<String, String> map = new HashMap<String, String>();
    return SliderUtils.mergeEntries(map, conf);
  }

  /**
   * This goes through the keyset of one configuration and retrieves each value
   * from a value source -a different or the same configuration. This triggers
   * the property resolution process of the value, resolving any variables against
   * in-config or inherited configurations
   * @param keysource source of keys
   * @param valuesource the source of values
   * @return a new configuration where <code>foreach key in keysource, get(key)==valuesource.get(key)</code>
   */
  public static Configuration resolveConfiguration(
      Iterable<Map.Entry<String, String>> keysource,
      Configuration valuesource) {
    Configuration result = new Configuration(false);
    for (Map.Entry<String, String> entry : keysource) {
      String key = entry.getKey();
      String value = valuesource.get(key);
      Preconditions.checkState(value != null,
          "no reference for \"%s\" in values", key);
      result.set(key, value);
    }
    return result;
  }

  /**
   * Register anything we consider deprecated
   */
  public static void registerDeprecatedConfigItems() {
    Configuration.addDeprecation(
        SliderXmlConfKeys.REGISTRY_ZK_QUORUM,
        RegistryConstants.KEY_REGISTRY_ZK_QUORUM);
    Configuration.addDeprecation(
        SliderXmlConfKeys.REGISTRY_PATH,
        RegistryConstants.KEY_REGISTRY_ZK_ROOT);
    
  }

  /**
   * Load a configuration with the {@link SliderKeys#SLIDER_XML} resource
   * included
   * @return a configuration instance
   */
  public static Configuration loadSliderConfiguration() {
    Configuration conf = new Configuration();
    conf.addResource(SliderKeys.SLIDER_XML);
    return conf;
  }

  /**
   * Inject the {@link SliderKeys#SLIDER_XML} resource
   * into the configuration resources <i>of all configurations</i>.
   * <p>
   *   This operation is idempotent.
   * <p>
   * If the resource is not on the classpath, downgrades, rather than
   * fails.
   * @return true if the resource was found and loaded.
   */
  public static synchronized boolean injectSliderXMLResource() {
    if (sliderResourceInjectionAttempted.getAndSet(true)) {
      return sliderResourceInjected.get();
    }
    URL resourceUrl = getResourceUrl(SliderKeys.SLIDER_XML);
    if (resourceUrl != null) {
      Configuration.addDefaultResource(SliderKeys.SLIDER_XML);
      sliderResourceInjected.set(true);
    }
    return sliderResourceInjected.get();
  }
  
}
