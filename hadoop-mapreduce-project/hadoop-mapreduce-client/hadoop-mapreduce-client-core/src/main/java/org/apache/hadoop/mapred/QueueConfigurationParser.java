/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.security.authorize.AccessControlList;
import static org.apache.hadoop.mapred.QueueManager.toFullPropertyName;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.DOMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;

/**
 * Class for parsing mapred-queues.xml.
 *    The format consists nesting of
 *    queues within queues - a feature called hierarchical queues.
 *    The parser expects that queues are
 *    defined within the 'queues' tag which is the top level element for
 *    XML document.
 * 
 * Creates the complete queue hieararchy
 */
class QueueConfigurationParser {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueueConfigurationParser.class);
  
  private boolean aclsEnabled = false;

  //Default root.
  protected Queue root = null;

  //xml tags for mapred-queues.xml
  static final String NAME_SEPARATOR = ":";
  static final String QUEUE_TAG = "queue";
  static final String ACL_SUBMIT_JOB_TAG = "acl-submit-job";
  static final String ACL_ADMINISTER_JOB_TAG = "acl-administer-jobs";

  // The value read from queues config file for this tag is not used at all.
  // To enable queue acls and job acls, mapreduce.cluster.acls.enabled is
  // to be set in mapred-site.xml
  @Deprecated
  static final String ACLS_ENABLED_TAG = "aclsEnabled";

  static final String PROPERTIES_TAG = "properties";
  static final String STATE_TAG = "state";
  static final String QUEUE_NAME_TAG = "name";
  static final String QUEUES_TAG = "queues";
  static final String PROPERTY_TAG = "property";
  static final String KEY_TAG = "key";
  static final String VALUE_TAG = "value";

  /**
   * Default constructor for DeperacatedQueueConfigurationParser
   */
  QueueConfigurationParser() {
    
  }

  QueueConfigurationParser(String confFile, boolean areAclsEnabled) {
    aclsEnabled = areAclsEnabled;
    File file = new File(confFile).getAbsoluteFile();
    if (!file.exists()) {
      throw new RuntimeException("Configuration file not found at " +
                                 confFile);
    }
    InputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(file));
      loadFrom(in);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      IOUtils.closeStream(in);
    }
  }

  QueueConfigurationParser(InputStream xmlInput, boolean areAclsEnabled) {
    aclsEnabled = areAclsEnabled;
    loadFrom(xmlInput);
  }

  private void loadFrom(InputStream xmlInput) {
    try {
      this.root = loadResource(xmlInput);
    } catch (ParserConfigurationException e) {
      throw new RuntimeException(e);
    } catch (SAXException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  void setAclsEnabled(boolean aclsEnabled) {
    this.aclsEnabled = aclsEnabled;
  }

  boolean isAclsEnabled() {
    return aclsEnabled;
  }

  Queue getRoot() {
    return root;
  }

  void setRoot(Queue root) {
    this.root = root;
  }

  /**
   * Method to load the resource file.
   * generates the root.
   * 
   * @param resourceInput InputStream that provides the XML to parse
   * @return
   * @throws ParserConfigurationException
   * @throws SAXException
   * @throws IOException
   */
  protected Queue loadResource(InputStream resourceInput)
    throws ParserConfigurationException, SAXException, IOException {
    DocumentBuilderFactory docBuilderFactory
      = DocumentBuilderFactory.newInstance();
    //ignore all comments inside the xml file
    docBuilderFactory.setIgnoringComments(true);

    //allow includes in the xml file
    docBuilderFactory.setNamespaceAware(true);
    try {
      docBuilderFactory.setXIncludeAware(true);
    } catch (UnsupportedOperationException e) {
      LOG.info(
        "Failed to set setXIncludeAware(true) for parser "
          + docBuilderFactory
          + NAME_SEPARATOR + e);
    }
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = null;
    Element queuesNode = null;

    doc = builder.parse(resourceInput);
    queuesNode = doc.getDocumentElement();
    return this.parseResource(queuesNode);
  }

  private Queue parseResource(Element queuesNode) {
    Queue rootNode = null;
    try {
      if (!QUEUES_TAG.equals(queuesNode.getTagName())) {
        LOG.info("Bad conf file: top-level element not <queues>");
        throw new RuntimeException("No queues defined ");
      }
      NamedNodeMap nmp = queuesNode.getAttributes();
      Node acls = nmp.getNamedItem(ACLS_ENABLED_TAG);

      if (acls != null) {
        LOG.warn("Configuring " + ACLS_ENABLED_TAG + " flag in " +
            QueueManager.QUEUE_CONF_FILE_NAME + " is not valid. " +
            "This tag is ignored. Configure " +
            MRConfig.MR_ACLS_ENABLED + " in mapred-site.xml. See the " +
            " documentation of " + MRConfig.MR_ACLS_ENABLED +
            ", which is used for enabling job level authorization and " +
            " queue level authorization.");
      }

      NodeList props = queuesNode.getChildNodes();
      if (props == null || props.getLength() <= 0) {
        LOG.info(" Bad configuration no queues defined ");
        throw new RuntimeException(" No queues defined ");
      }

      //We have root level nodes.
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element)) {
          continue;
        }

        if (!propNode.getNodeName().equals(QUEUE_TAG)) {
          LOG.info("At root level only \" queue \" tags are allowed ");
          throw
            new RuntimeException("Malformed xml document no queue defined ");
        }

        Element prop = (Element) propNode;
        //Add children to root.
        Queue q = createHierarchy("", prop);
        if(rootNode == null) {
          rootNode = new Queue();
          rootNode.setName("");
        }
        rootNode.addChild(q);
      }
      return rootNode;
    } catch (DOMException e) {
      LOG.info("Error parsing conf file: " + e);
      throw new RuntimeException(e);
    }
  }

  /**
   * @param parent Name of the parent queue
   * @param queueNode
   * @return
   */
  private Queue createHierarchy(String parent, Element queueNode) {

    if (queueNode == null) {
      return null;
    }
    //Name of the current queue.
    //Complete qualified queue name.
    String name = "";
    Queue newQueue = new Queue();
    Map<String, AccessControlList> acls =
      new HashMap<String, AccessControlList>();

    NodeList fields = queueNode.getChildNodes();
    validate(queueNode);
    List<Element> subQueues = new ArrayList<Element>();

    String submitKey = "";
    String adminKey = "";
    
    for (int j = 0; j < fields.getLength(); j++) {
      Node fieldNode = fields.item(j);
      if (!(fieldNode instanceof Element)) {
        continue;
      }
      Element field = (Element) fieldNode;
      if (QUEUE_NAME_TAG.equals(field.getTagName())) {
        String nameValue = field.getTextContent();
        if (field.getTextContent() == null ||
          field.getTextContent().trim().equals("") ||
          field.getTextContent().contains(NAME_SEPARATOR)) {
          throw new RuntimeException("Improper queue name : " + nameValue);
        }

        if (!parent.equals("")) {
          name += parent + NAME_SEPARATOR;
        }
        //generate the complete qualified name
        //parent.child
        name += nameValue;
        newQueue.setName(name);
        submitKey = toFullPropertyName(name,
            QueueACL.SUBMIT_JOB.getAclName());
        adminKey = toFullPropertyName(name,
            QueueACL.ADMINISTER_JOBS.getAclName());
      }

      if (QUEUE_TAG.equals(field.getTagName()) && field.hasChildNodes()) {
        subQueues.add(field);
      }
      if(isAclsEnabled()) {
        if (ACL_SUBMIT_JOB_TAG.equals(field.getTagName())) {
          acls.put(submitKey, new AccessControlList(field.getTextContent()));
        }

        if (ACL_ADMINISTER_JOB_TAG.equals(field.getTagName())) {
          acls.put(adminKey, new AccessControlList(field.getTextContent()));
        }
      }

      if (PROPERTIES_TAG.equals(field.getTagName())) {
        Properties properties = populateProperties(field);
        newQueue.setProperties(properties);
      }

      if (STATE_TAG.equals(field.getTagName())) {
        String state = field.getTextContent();
        newQueue.setState(QueueState.getState(state));
      }
    }
    
    if (!acls.containsKey(submitKey)) {
      acls.put(submitKey, new AccessControlList(" "));
    }
    
    if (!acls.containsKey(adminKey)) {
      acls.put(adminKey, new AccessControlList(" "));
    }
    
    //Set acls
    newQueue.setAcls(acls);
    //At this point we have the queue ready at current height level.
    //so we have parent name available.

    for(Element field:subQueues) {
      newQueue.addChild(createHierarchy(newQueue.getName(), field));
    }
    return newQueue;
  }

  /**
   * Populate the properties for Queue
   *
   * @param field
   * @return
   */
  private Properties populateProperties(Element field) {
    Properties props = new Properties();

    NodeList propfields = field.getChildNodes();

    for (int i = 0; i < propfields.getLength(); i++) {
      Node prop = propfields.item(i);

      //If this node is not of type element
      //skip this.
      if (!(prop instanceof Element)) {
        continue;
      }

      if (PROPERTY_TAG.equals(prop.getNodeName())) {
        if (prop.hasAttributes()) {
          NamedNodeMap nmp = prop.getAttributes();
          if (nmp.getNamedItem(KEY_TAG) != null && nmp.getNamedItem(
            VALUE_TAG) != null) {
            props.setProperty(
              nmp.getNamedItem(KEY_TAG).getTextContent(), nmp.getNamedItem(
                VALUE_TAG).getTextContent());
          }
        }
      }
    }
    return props;
  }

  /**
   *
   * Checks if there is NAME_TAG for queues.
   *
   * Checks if (queue has children)
   *  then it shouldnot have acls-* or state
   *   else
   *  throws an Exception.
   * @param node
   */
  private void validate(Node node) {

    NodeList fields = node.getChildNodes();

    //Check if <queue> & (<acls-*> || <state>) are not siblings
    //if yes throw an IOException.
    Set<String> siblings = new HashSet<String>();
    for (int i = 0; i < fields.getLength(); i++) {
      if (!(fields.item(i) instanceof Element)) {
        continue;
      }
      siblings.add((fields.item(i)).getNodeName());
    }

    if(! siblings.contains(QUEUE_NAME_TAG)) {
      throw new RuntimeException(
        " Malformed xml formation queue name not specified ");
    }

    if (siblings.contains(QUEUE_TAG) && (
      siblings.contains(ACL_ADMINISTER_JOB_TAG) ||
        siblings.contains(ACL_SUBMIT_JOB_TAG) ||
        siblings.contains(STATE_TAG)
    )) {
      throw new RuntimeException(
        " Malformed xml formation queue tag and acls " +
          "tags or state tags are siblings ");
    }
  }


  private static String getSimpleQueueName(String fullQName) {
    int index = fullQName.lastIndexOf(NAME_SEPARATOR);
    if (index < 0) {
      return fullQName;
    }
    return fullQName.substring(index + 1, fullQName.length());
  }

  /**
   * Construct an {@link Element} for a single queue, constructing the inner
   * queue &lt;name/&gt;, &lt;properties/&gt;, &lt;state/&gt; and the inner
   * &lt;queue&gt; elements recursively.
   * 
   * @param document
   * @param jqi
   * @return
   */
  static Element getQueueElement(Document document, JobQueueInfo jqi) {

    // Queue
    Element q = document.createElement(QUEUE_TAG);

    // Queue-name
    Element qName = document.createElement(QUEUE_NAME_TAG);
    qName.setTextContent(getSimpleQueueName(jqi.getQueueName()));
    q.appendChild(qName);

    // Queue-properties
    Properties props = jqi.getProperties();
    Element propsElement = document.createElement(PROPERTIES_TAG);
    if (props != null) {
      Set<String> propList = props.stringPropertyNames();
      for (String prop : propList) {
        Element propertyElement = document.createElement(PROPERTY_TAG);
        propertyElement.setAttribute(KEY_TAG, prop);
        propertyElement.setAttribute(VALUE_TAG, (String) props.get(prop));
        propsElement.appendChild(propertyElement);
      }
    }
    q.appendChild(propsElement);

    // Queue-state
    String queueState = jqi.getState().getStateName();
    if (queueState != null
        && !queueState.equals(QueueState.UNDEFINED.getStateName())) {
      Element qStateElement = document.createElement(STATE_TAG);
      qStateElement.setTextContent(queueState);
      q.appendChild(qStateElement);
    }

    // Queue-children
    List<JobQueueInfo> children = jqi.getChildren();
    if (children != null) {
      for (JobQueueInfo child : children) {
        q.appendChild(getQueueElement(document, child));
      }
    }

    return q;
  }

}
