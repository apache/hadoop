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

package org.apache.hadoop.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.Map.Entry;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * This class validates configuration XML files in ${HADOOP_CONF_DIR} or
 * specified ones.
 */
@InterfaceAudience.Private
public final class ConfTest {

  private static final String USAGE =
      "Usage: hadoop conftest [-conffile <path>|-h|--help]\n"
      + "  Options:\n"
      + "  \n"
      + "  -conffile <path>\n"
      + "    If not specified, the files in ${HADOOP_CONF_DIR}\n"
      + "    whose name end with .xml will be verified.\n"
      + "    If specified, that path will be verified.\n"
      + "    You can specify either a file or directory, and\n"
      + "    if a directory specified, the files in that directory\n"
      + "    whose name end with .xml will be verified.\n"
      + "    You can specify this option multiple times.\n"
      + "  -h, --help       Print this help";

  private static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";

  protected ConfTest() {
    super();
  }

  private static List<NodeInfo> parseConf(InputStream in)
      throws XMLStreamException {
    QName configuration = new QName("configuration");
    QName property = new QName("property");

    List<NodeInfo> nodes = new ArrayList<NodeInfo>();
    Stack<NodeInfo> parsed = new Stack<>();

    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLEventReader reader = factory.createXMLEventReader(in);

    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      if (event.isStartElement()) {
        StartElement currentElement = event.asStartElement();
        NodeInfo currentNode = new NodeInfo(currentElement);
        if (parsed.isEmpty()) {
          if (!currentElement.getName().equals(configuration)) {
            return null;
          }
        } else {
          NodeInfo parentNode = parsed.peek();
          QName parentName = parentNode.getStartElement().getName();
          if (parentName.equals(configuration)
              && currentNode.getStartElement().getName().equals(property)) {
            @SuppressWarnings("unchecked")
            Iterator<Attribute> it = currentElement.getAttributes();
            while (it.hasNext()) {
              currentNode.addAttribute(it.next());
            }
          } else if (parentName.equals(property)) {
            parentNode.addElement(currentElement);
          }
        }
        parsed.push(currentNode);
      } else if (event.isEndElement()) {
        NodeInfo node = parsed.pop();
        if (parsed.size() == 1) {
          nodes.add(node);
        }
      } else if (event.isCharacters()) {
        if (2 < parsed.size()) {
          NodeInfo parentNode = parsed.pop();
          StartElement parentElement = parentNode.getStartElement();
          NodeInfo grandparentNode = parsed.peek();
          if (grandparentNode.getElement(parentElement) == null) {
            grandparentNode.setElement(parentElement, event.asCharacters());
          }
          parsed.push(parentNode);
        }
      }
    }

    return nodes;
  }

  public static List<String> checkConf(InputStream in) {
    List<NodeInfo> nodes = null;
    List<String> errors = new ArrayList<String>();

    try {
      nodes = parseConf(in);
      if (nodes == null) {
        errors.add("bad conf file: top-level element not <configuration>");
      }
    } catch (XMLStreamException e) {
      errors.add("bad conf file: " + e.getMessage());
    }

    if (!errors.isEmpty()) {
      return errors;
    }

    Map<String, List<Integer>> duplicatedProperties =
        new HashMap<String, List<Integer>>();

    for (NodeInfo node : nodes) {
      StartElement element = node.getStartElement();
      int line = element.getLocation().getLineNumber();

      if (!element.getName().equals(new QName("property"))) {
        errors.add(String.format("Line %d: element not <property>", line));
        continue;
      }

      List<XMLEvent> events = node.getXMLEventsForQName(new QName("name"));
      if (events == null) {
        errors.add(String.format("Line %d: <property> has no <name>", line));
      } else {
        String v = null;
        for (XMLEvent event : events) {
          if (event.isAttribute()) {
            v = ((Attribute) event).getValue();
          } else {
            Characters c = node.getElement(event.asStartElement());
            if (c != null) {
              v = c.getData();
            }
          }
          if (v == null || v.isEmpty()) {
            errors.add(String.format("Line %d: <property> has an empty <name>",
                line));
          }
        }
        if (v != null && !v.isEmpty()) {
          List<Integer> lines = duplicatedProperties.get(v);
          if (lines == null) {
            lines = new ArrayList<Integer>();
            duplicatedProperties.put(v, lines);
          }
          lines.add(node.getStartElement().getLocation().getLineNumber());
        }
      }

      events = node.getXMLEventsForQName(new QName("value"));
      if (events == null) {
        errors.add(String.format("Line %d: <property> has no <value>", line));
      }

      for (QName qName : node.getDuplicatedQNames()) {
        if (!qName.equals(new QName("source"))) {
          errors.add(String.format("Line %d: <property> has duplicated <%s>s",
              line, qName));
        }
      }
    }

    for (Entry<String, List<Integer>> e : duplicatedProperties.entrySet()) {
      List<Integer> lines = e.getValue();
      if (1 < lines.size()) {
        errors.add(String.format("Line %s: duplicated <property>s for %s",
            StringUtils.join(", ", lines), e.getKey()));
      }
    }

    return errors;
  }

  private static File[] listFiles(File dir) {
    return dir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.isFile() && file.getName().endsWith(".xml");
      }
    });
  }

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException {
    GenericOptionsParser genericParser = new GenericOptionsParser(args);
    String[] remainingArgs = genericParser.getRemainingArgs();

    Option conf = OptionBuilder.hasArg().create("conffile");
    Option help = OptionBuilder.withLongOpt("help").create('h');
    Options opts = new Options().addOption(conf).addOption(help);
    CommandLineParser specificParser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = specificParser.parse(opts, remainingArgs);
    } catch (MissingArgumentException e) {
      terminate(1, "No argument specified for -conffile option");
    } catch (ParseException e) {
      terminate(1, USAGE);
    }
    if (cmd == null) {
      terminate(1, "Failed to parse options");
    }

    if (cmd.hasOption('h')) {
      terminate(0, USAGE);
    }

    List<File> files = new ArrayList<File>();
    if (cmd.hasOption("conffile")) {
      String[] values = cmd.getOptionValues("conffile");
      for (String value : values) {
        File confFile = new File(value);
        if (confFile.isFile()) {
          files.add(confFile);
        } else if (confFile.isDirectory()) {
          files.addAll(Arrays.asList(listFiles(confFile)));
        } else {
          terminate(1, confFile.getAbsolutePath()
              + " is neither a file nor directory");
        }
      }
    } else {
      String confDirName = System.getenv(HADOOP_CONF_DIR);
      if (confDirName == null) {
        terminate(1, HADOOP_CONF_DIR + " is not defined");
      }
      File confDir = new File(confDirName);
      if (!confDir.isDirectory()) {
        terminate(1, HADOOP_CONF_DIR + " is not a directory");
      }
      files = Arrays.asList(listFiles(confDir));
    }
    if (files.isEmpty()) {
      terminate(1, "No input file to validate");
    }

    boolean ok = true;
    for (File file : files) {
      String path = file.getAbsolutePath();
      List<String> errors = checkConf(new FileInputStream(file));
      if (errors.isEmpty()) {
        System.out.println(path + ": valid");
      } else {
        ok = false;
        System.err.println(path + ":");
        for (String error : errors) {
          System.err.println("\t" + error);
        }
      }
    }
    if (ok) {
      System.out.println("OK");
    } else {
      terminate(1, "Invalid file exists");
    }
  }

  private static void terminate(int status, String msg) {
    System.err.println(msg);
    System.exit(status);
  }
}

class NodeInfo {

  private StartElement startElement;
  private List<Attribute> attributes = new ArrayList<Attribute>();
  private Map<StartElement, Characters> elements =
      new HashMap<>();
  private Map<QName, List<XMLEvent>> qNameXMLEventsMap =
      new HashMap<>();

  public NodeInfo(StartElement startElement) {
    this.startElement = startElement;
  }

  private void addQNameXMLEvent(QName qName, XMLEvent event) {
    List<XMLEvent> events = qNameXMLEventsMap.get(qName);
    if (events == null) {
      events = new ArrayList<XMLEvent>();
      qNameXMLEventsMap.put(qName, events);
    }
    events.add(event);
  }

  public StartElement getStartElement() {
    return startElement;
  }

  public void addAttribute(Attribute attribute) {
    attributes.add(attribute);
    addQNameXMLEvent(attribute.getName(), attribute);
  }

  public Characters getElement(StartElement element) {
    return elements.get(element);
  }

  public void addElement(StartElement element) {
    setElement(element, null);
    addQNameXMLEvent(element.getName(), element);
  }

  public void setElement(StartElement element, Characters text) {
    elements.put(element, text);
  }

  public List<QName> getDuplicatedQNames() {
    List<QName> duplicates = new ArrayList<QName>();
    for (Map.Entry<QName, List<XMLEvent>> e : qNameXMLEventsMap.entrySet()) {
      if (1 < e.getValue().size()) {
        duplicates.add(e.getKey());
      }
    }
    return duplicates;
  }

  public List<XMLEvent> getXMLEventsForQName(QName qName) {
    return qNameXMLEventsMap.get(qName);
  }
}
