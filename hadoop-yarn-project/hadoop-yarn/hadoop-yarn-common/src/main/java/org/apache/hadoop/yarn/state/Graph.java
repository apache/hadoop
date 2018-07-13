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
package org.apache.hadoop.yarn.state;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;

@Private
public class Graph {
  public class Edge {
    Node from;
    Node to;
    String label;

    public Edge(Node from, Node to, String info) {
      this.from = from;
      this.to = to;
      this.label = info;
    }

    public boolean sameAs(Edge rhs) {
      if (this.from == rhs.from &&
          this.to == rhs.to) {
        return true;
      }
      return false;
    }

    public Edge combine(Edge rhs) {
      String newlabel = this.label + "," + rhs.label;
      return new Edge(this.from, this.to, newlabel);
    }
  }

  public class Node {
    Graph parent;
    String id;
    List<Edge> ins;
    List<Edge> outs;

    public Node(String id) {
      this.id = id;
      this.parent = Graph.this;
      this.ins = new ArrayList<Graph.Edge>();
      this.outs = new ArrayList<Graph.Edge>();
    }

    public Graph getParent() {
      return parent;
    }

    public Node addEdge(Node to, String info) {
      Edge e = new Edge(this, to, info);
      outs.add(e);
      to.ins.add(e);
      return this;
    }

    public String getUniqueId() {
      return Graph.this.name + "." + id;
    }
  }

  private String name;
  private Graph parent;
  private Set<Graph.Node> nodes = new HashSet<Graph.Node>();
  private Set<Graph> subgraphs = new HashSet<Graph>();

  public Graph(String name, Graph parent) {
    this.name = name;
    this.parent = parent;
  }

  public Graph(String name) {
    this(name, null);
  }

  public Graph() {
    this("graph", null);
  }

  public String getName() {
    return name;
  }

  public Graph getParent() {
    return parent;
  }

  private Node newNode(String id) {
    Node ret = new Node(id);
    nodes.add(ret);
    return ret;
  }

  public Node getNode(String id) {
    for (Node node : nodes) {
      if (node.id.equals(id)) {
        return node;
      }
    }
    return newNode(id);
  }

  public Graph newSubGraph(String name) {
    Graph ret = new Graph(name, this);
    subgraphs.add(ret);
    return ret;
  }

  public void addSubGraph(Graph graph) {
    subgraphs.add(graph);
    graph.parent = this;
  }

  private static String wrapSafeString(String label) {
    if (label.indexOf(',') >= 0) {
      if (label.length()>14) {
        label = label.replaceAll(",", ",\n");
      }
    }
    label = "\"" + StringEscapeUtils.escapeJava(label) + "\"";
    return label;
  }

  public String generateGraphViz(String indent) {
    StringBuilder sb = new StringBuilder();
    if (this.parent == null) {
      sb.append("digraph " + name + " {\n");
      sb.append(String.format("graph [ label=%s, fontsize=24, fontname=Helvetica];%n",
          wrapSafeString(name)));
      sb.append("node [fontsize=12, fontname=Helvetica];\n");
      sb.append("edge [fontsize=9, fontcolor=blue, fontname=Arial];\n");
    } else {
      sb.append("subgraph cluster_" + name + " {\nlabel=\"" + name + "\"\n");
    }
    for (Graph g : subgraphs) {
      String ginfo = g.generateGraphViz(indent+"  ");
      sb.append(ginfo);
      sb.append("\n");
    }
    for (Node n : nodes) {
      sb.append(String.format(
          "%s%s [ label = %s ];%n",
          indent,
          wrapSafeString(n.getUniqueId()),
          n.id));
      List<Edge> combinedOuts = combineEdges(n.outs);
      for (Edge e : combinedOuts) {
        sb.append(String.format(
            "%s%s -> %s [ label = %s ];%n",
            indent,
            wrapSafeString(e.from.getUniqueId()),
            wrapSafeString(e.to.getUniqueId()),
            wrapSafeString(e.label)));
      }
    }
    sb.append("}\n");
    return sb.toString();
  }

  public String generateGraphViz() {
    return generateGraphViz("");
  }

  public void save(String filepath) throws IOException {
    try (OutputStreamWriter fout = new OutputStreamWriter(
        new FileOutputStream(filepath), Charset.forName("UTF-8"))) {
      fout.write(generateGraphViz());
    }
  }

  public static List<Edge> combineEdges(List<Edge> edges) {
    List<Edge> ret = new ArrayList<Edge>();
    for (Edge edge : edges) {
      boolean found = false;
      for (int i = 0; i < ret.size(); i++) {
        Edge current = ret.get(i);
        if (edge.sameAs(current)) {
          ret.set(i, current.combine(edge));
          found = true;
          break;
        }
      }
      if (!found) {
        ret.add(edge);
      }
    }
    return ret;
  }
}
