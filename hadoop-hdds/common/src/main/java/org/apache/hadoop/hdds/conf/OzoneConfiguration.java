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

package org.apache.hadoop.hdds.conf;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

/**
 * Configuration for ozone.
 */
@InterfaceAudience.Private
public class OzoneConfiguration extends Configuration {
  static {
    activate();
  }

  public OzoneConfiguration() {
    OzoneConfiguration.activate();
  }

  public OzoneConfiguration(Configuration conf) {
    super(conf);
    //load the configuration from the classloader of the original conf.
    setClassLoader(conf.getClassLoader());
  }

  public List<Property> readPropertyFromXml(URL url) throws JAXBException {
    JAXBContext context = JAXBContext.newInstance(XMLConfiguration.class);
    Unmarshaller um = context.createUnmarshaller();

    XMLConfiguration config = (XMLConfiguration) um.unmarshal(url);
    return config.getProperties();
  }

  /**
   * Class to marshall/un-marshall configuration from xml files.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "configuration")
  public static class XMLConfiguration {

    @XmlElement(name = "property", type = Property.class)
    private List<Property> properties = new ArrayList<>();

    public XMLConfiguration() {
    }

    public XMLConfiguration(List<Property> properties) {
      this.properties = properties;
    }

    public List<Property> getProperties() {
      return properties;
    }

    public void setProperties(List<Property> properties) {
      this.properties = properties;
    }
  }

  /**
   * Class to marshall/un-marshall configuration properties from xml files.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "property")
  public static class Property implements Comparable<Property> {

    private String name;
    private String value;
    private String tag;
    private String description;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    public String getTag() {
      return tag;
    }

    public void setTag(String tag) {
      this.tag = tag;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    @Override
    public int compareTo(Property o) {
      if (this == o) {
        return 0;
      }
      return this.getName().compareTo(o.getName());
    }

    @Override
    public String toString() {
      return this.getName() + " " + this.getValue() + " " + this.getTag();
    }

    @Override
    public int hashCode(){
      return this.getName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return (obj instanceof Property) && (((Property) obj).getName())
          .equals(this.getName());
    }
  }

  public static void activate() {
    // adds the default resources
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("ozone-default.xml");
    Configuration.addDefaultResource("ozone-site.xml");
  }

  /**
   * The super class method getAllPropertiesByTag
   * does not override values of properties
   * if there is no tag present in the configs of
   * newly added resources.
   * @param tag
   * @return Properties that belong to the tag
   */
  @Override
  public Properties getAllPropertiesByTag(String tag) {
    // Call getProps first to load the newly added resources
    // before calling super.getAllPropertiesByTag
    Properties updatedProps = getProps();
    Properties propertiesByTag = super.getAllPropertiesByTag(tag);
    Properties props = new Properties();
    Enumeration properties = propertiesByTag.propertyNames();
    while (properties.hasMoreElements()) {
      Object propertyName =  properties.nextElement();
      // get the current value of the property
      Object value = updatedProps.getProperty(propertyName.toString());
      if (value != null) {
        props.put(propertyName, value);
      }
    }
    return props;
  }
}
