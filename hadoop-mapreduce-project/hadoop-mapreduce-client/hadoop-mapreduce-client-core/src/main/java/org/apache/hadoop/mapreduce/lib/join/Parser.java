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

package org.apache.hadoop.mapreduce.lib.join;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Very simple shift-reduce parser for join expressions.
 *
 * This should be sufficient for the user extension permitted now, but ought to
 * be replaced with a parser generator if more complex grammars are supported.
 * In particular, this &quot;shift-reduce&quot; parser has no states. Each set
 * of formals requires a different internal node type, which is responsible for
 * interpreting the list of tokens it receives. This is sufficient for the
 * current grammar, but it has several annoying properties that might inhibit
 * extension. In particular, parenthesis are always function calls; an
 * algebraic or filter grammar would not only require a node type, but must
 * also work around the internals of this parser.
 *
 * For most other cases, adding classes to the hierarchy- particularly by
 * extending JoinRecordReader and MultiFilterRecordReader- is fairly
 * straightforward. One need only override the relevant method(s) (usually only
 * {@link CompositeRecordReader#combine}) and include a property to map its
 * value to an identifier in the parser.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Parser {
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public enum TType { CIF, IDENT, COMMA, LPAREN, RPAREN, QUOT, NUM, }

  /**
   * Tagged-union type for tokens from the join expression.
   * @see Parser.TType
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class Token {

    private TType type;

    Token(TType type) {
      this.type = type;
    }

    public TType getType() { return type; }
    
    public Node getNode() throws IOException {
      throw new IOException("Expected nodetype");
    }
    
    public double getNum() throws IOException {
      throw new IOException("Expected numtype");
    }
    
    public String getStr() throws IOException {
      throw new IOException("Expected strtype");
    }
  }

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class NumToken extends Token {
    private double num;
    public NumToken(double num) {
      super(TType.NUM);
      this.num = num;
    }
    public double getNum() { return num; }
  }

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class NodeToken extends Token {
    private Node node;
    NodeToken(Node node) {
      super(TType.CIF);
      this.node = node;
    }
    public Node getNode() {
      return node;
    }
  }

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class StrToken extends Token {
    private String str;
    public StrToken(TType type, String str) {
      super(type);
      this.str = str;
    }
    public String getStr() {
      return str;
    }
  }

  /**
   * Simple lexer wrapping a StreamTokenizer.
   * This encapsulates the creation of tagged-union Tokens and initializes the
   * SteamTokenizer.
   */
  private static class Lexer {

    private StreamTokenizer tok;

    Lexer(String s) {
      tok = new StreamTokenizer(new CharArrayReader(s.toCharArray()));
      tok.quoteChar('"');
      tok.parseNumbers();
      tok.ordinaryChar(',');
      tok.ordinaryChar('(');
      tok.ordinaryChar(')');
      tok.wordChars('$','$');
      tok.wordChars('_','_');
    }

    Token next() throws IOException {
      int type = tok.nextToken();
      switch (type) {
        case StreamTokenizer.TT_EOF:
        case StreamTokenizer.TT_EOL:
          return null;
        case StreamTokenizer.TT_NUMBER:
          return new NumToken(tok.nval);
        case StreamTokenizer.TT_WORD:
          return new StrToken(TType.IDENT, tok.sval);
        case '"':
          return new StrToken(TType.QUOT, tok.sval);
        default:
          switch (type) {
            case ',':
              return new Token(TType.COMMA);
            case '(':
              return new Token(TType.LPAREN);
            case ')':
              return new Token(TType.RPAREN);
            default:
              throw new IOException("Unexpected: " + type);
          }
      }
    }
  }

@SuppressWarnings("unchecked")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract static class Node extends ComposableInputFormat {
    /**
     * Return the node type registered for the particular identifier.
     * By default, this is a CNode for any composite node and a WNode
     * for &quot;wrapped&quot; nodes. User nodes will likely be composite
     * nodes.
     * @see #addIdentifier(java.lang.String, java.lang.Class[], java.lang.Class, java.lang.Class)
     * @see CompositeInputFormat#setFormat(org.apache.hadoop.mapred.JobConf)
     */
    static Node forIdent(String ident) throws IOException {
      try {
        if (!nodeCstrMap.containsKey(ident)) {
          throw new IOException("No nodetype for " + ident);
        }
        return nodeCstrMap.get(ident).newInstance(ident);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      } catch (InstantiationException e) {
        throw new IOException(e);
      } catch (InvocationTargetException e) {
        throw new IOException(e);
      }
    }

    private static final Class<?>[] ncstrSig = { String.class };
    private static final
        Map<String,Constructor<? extends Node>> nodeCstrMap =
        new HashMap<String,Constructor<? extends Node>>();
    protected static final Map<String,Constructor<? extends 
        ComposableRecordReader>> rrCstrMap =
        new HashMap<String,Constructor<? extends ComposableRecordReader>>();

    /**
     * For a given identifier, add a mapping to the nodetype for the parse
     * tree and to the ComposableRecordReader to be created, including the
     * formals required to invoke the constructor.
     * The nodetype and constructor signature should be filled in from the
     * child node.
     */
    protected static void addIdentifier(String ident, Class<?>[] mcstrSig,
                              Class<? extends Node> nodetype,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Constructor<? extends Node> ncstr =
        nodetype.getDeclaredConstructor(ncstrSig);
      ncstr.setAccessible(true);
      nodeCstrMap.put(ident, ncstr);
      Constructor<? extends ComposableRecordReader> mcstr =
        cl.getDeclaredConstructor(mcstrSig);
      mcstr.setAccessible(true);
      rrCstrMap.put(ident, mcstr);
    }

    // inst
    protected int id = -1;
    protected String ident;
    protected Class<? extends WritableComparator> cmpcl;

    protected Node(String ident) {
      this.ident = ident;
    }

    protected void setID(int id) {
      this.id = id;
    }

    protected void setKeyComparator(
        Class<? extends WritableComparator> cmpcl) {
      this.cmpcl = cmpcl;
    }
    abstract void parse(List<Token> args, Configuration conf) 
        throws IOException;
  }

  /**
   * Nodetype in the parse tree for &quot;wrapped&quot; InputFormats.
   */
  static class WNode extends Node {
    private static final Class<?>[] cstrSig =
      { Integer.TYPE, RecordReader.class, Class.class };

    @SuppressWarnings("unchecked")
	static void addIdentifier(String ident,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Node.addIdentifier(ident, cstrSig, WNode.class, cl);
    }

    private String indir;
    private InputFormat<?, ?> inf;

    public WNode(String ident) {
      super(ident);
    }

    /**
     * Let the first actual define the InputFormat and the second define
     * the <tt>mapred.input.dir</tt> property.
     */
    @Override
    public void parse(List<Token> ll, Configuration conf) throws IOException {
      StringBuilder sb = new StringBuilder();
      Iterator<Token> i = ll.iterator();
      while (i.hasNext()) {
        Token t = i.next();
        if (TType.COMMA.equals(t.getType())) {
          try {
          	inf = (InputFormat<?, ?>)ReflectionUtils.newInstance(
          			conf.getClassByName(sb.toString()), conf);
          } catch (ClassNotFoundException e) {
            throw new IOException(e);
          } catch (IllegalArgumentException e) {
            throw new IOException(e);
          }
          break;
        }
        sb.append(t.getStr());
      }
      if (!i.hasNext()) {
        throw new IOException("Parse error");
      }
      Token t = i.next();
      if (!TType.QUOT.equals(t.getType())) {
        throw new IOException("Expected quoted string");
      }
      indir = t.getStr();
      // no check for ll.isEmpty() to permit extension
    }

    private Configuration getConf(Configuration jconf) throws IOException {
      Job job = Job.getInstance(jconf);
      FileInputFormat.setInputPaths(job, indir);
      return job.getConfiguration();
    }
    
    public List<InputSplit> getSplits(JobContext context)
        throws IOException, InterruptedException {
      return inf.getSplits(
                 new JobContextImpl(getConf(context.getConfiguration()), 
                                    context.getJobID()));
    }

    public ComposableRecordReader<?, ?> createRecordReader(InputSplit split, 
        TaskAttemptContext taskContext) 
        throws IOException, InterruptedException {
      try {
        if (!rrCstrMap.containsKey(ident)) {
          throw new IOException("No RecordReader for " + ident);
        }
        Configuration conf = getConf(taskContext.getConfiguration());
        TaskAttemptContext context = 
          new TaskAttemptContextImpl(conf, 
              TaskAttemptID.forName(conf.get(MRJobConfig.TASK_ATTEMPT_ID)), 
              new WrappedStatusReporter(taskContext));
        return rrCstrMap.get(ident).newInstance(id,
            inf.createRecordReader(split, context), cmpcl);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      } catch (InstantiationException e) {
        throw new IOException(e);
      } catch (InvocationTargetException e) {
        throw new IOException(e);
      }
    }

    public String toString() {
      return ident + "(" + inf.getClass().getName() + ",\"" + indir + "\")";
    }
  }

  private static class WrappedStatusReporter extends StatusReporter {

    TaskAttemptContext context;
    
    public WrappedStatusReporter(TaskAttemptContext context) {
      this.context = context; 
    }
    @Override
    public Counter getCounter(Enum<?> name) {
      return context.getCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
      return context.getCounter(group, name);
    }

    @Override
    public void progress() {
      context.progress();
    }

    @Override
    public float getProgress() {
      return context.getProgress();
    }
    
    @Override
    public void setStatus(String status) {
      context.setStatus(status);
    }
  }

  /**
   * Internal nodetype for &quot;composite&quot; InputFormats.
   */
  static class CNode extends Node {

    private static final Class<?>[] cstrSig =
      { Integer.TYPE, Configuration.class, Integer.TYPE, Class.class };

    @SuppressWarnings("unchecked")
	static void addIdentifier(String ident,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Node.addIdentifier(ident, cstrSig, CNode.class, cl);
    }

    // inst
    private ArrayList<Node> kids = new ArrayList<Node>();

    public CNode(String ident) {
      super(ident);
    }

    @Override
    public void setKeyComparator(Class<? extends WritableComparator> cmpcl) {
      super.setKeyComparator(cmpcl);
      for (Node n : kids) {
        n.setKeyComparator(cmpcl);
      }
    }

    /**
     * Combine InputSplits from child InputFormats into a
     * {@link CompositeInputSplit}.
     */
    @SuppressWarnings("unchecked")
	public List<InputSplit> getSplits(JobContext job)
        throws IOException, InterruptedException {
      List<List<InputSplit>> splits = 
        new ArrayList<List<InputSplit>>(kids.size());
      for (int i = 0; i < kids.size(); ++i) {
        List<InputSplit> tmp = kids.get(i).getSplits(job);
        if (null == tmp) {
          throw new IOException("Error gathering splits from child RReader");
        }
        if (i > 0 && splits.get(i-1).size() != tmp.size()) {
          throw new IOException("Inconsistent split cardinality from child " +
              i + " (" + splits.get(i-1).size() + "/" + tmp.size() + ")");
        }
        splits.add(i, tmp);
      }
      final int size = splits.get(0).size();
      List<InputSplit> ret = new ArrayList<InputSplit>();
      for (int i = 0; i < size; ++i) {
        CompositeInputSplit split = new CompositeInputSplit(splits.size());
        for (int j = 0; j < splits.size(); ++j) {
          split.add(splits.get(j).get(i));
        }
        ret.add(split);
      }
      return ret;
    }

    @SuppressWarnings("unchecked") // child types unknowable
    public ComposableRecordReader 
        createRecordReader(InputSplit split, TaskAttemptContext taskContext) 
        throws IOException, InterruptedException {
      if (!(split instanceof CompositeInputSplit)) {
        throw new IOException("Invalid split type:" +
                              split.getClass().getName());
      }
      final CompositeInputSplit spl = (CompositeInputSplit)split;
      final int capacity = kids.size();
      CompositeRecordReader ret = null;
      try {
        if (!rrCstrMap.containsKey(ident)) {
          throw new IOException("No RecordReader for " + ident);
        }
        ret = (CompositeRecordReader)rrCstrMap.get(ident).
          newInstance(id, taskContext.getConfiguration(), capacity, cmpcl);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      } catch (InstantiationException e) {
        throw new IOException(e);
      } catch (InvocationTargetException e) {
        throw new IOException(e);
      }
      for (int i = 0; i < capacity; ++i) {
        ret.add(kids.get(i).createRecordReader(spl.get(i), taskContext));
      }
      return (ComposableRecordReader)ret;
    }

    /**
     * Parse a list of comma-separated nodes.
     */
    public void parse(List<Token> args, Configuration conf) 
        throws IOException {
      ListIterator<Token> i = args.listIterator();
      while (i.hasNext()) {
        Token t = i.next();
        t.getNode().setID(i.previousIndex() >> 1);
        kids.add(t.getNode());
        if (i.hasNext() && !TType.COMMA.equals(i.next().getType())) {
          throw new IOException("Expected ','");
        }
      }
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(ident + "(");
      for (Node n : kids) {
        sb.append(n.toString() + ",");
      }
      sb.setCharAt(sb.length() - 1, ')');
      return sb.toString();
    }
  }

  private static Token reduce(Stack<Token> st, Configuration conf) 
      throws IOException {
    LinkedList<Token> args = new LinkedList<Token>();
    while (!st.isEmpty() && !TType.LPAREN.equals(st.peek().getType())) {
      args.addFirst(st.pop());
    }
    if (st.isEmpty()) {
      throw new IOException("Unmatched ')'");
    }
    st.pop();
    if (st.isEmpty() || !TType.IDENT.equals(st.peek().getType())) {
      throw new IOException("Identifier expected");
    }
    Node n = Node.forIdent(st.pop().getStr());
    n.parse(args, conf);
    return new NodeToken(n);
  }

  /**
   * Given an expression and an optional comparator, build a tree of
   * InputFormats using the comparator to sort keys.
   */
  static Node parse(String expr, Configuration conf) throws IOException {
    if (null == expr) {
      throw new IOException("Expression is null");
    }
    Class<? extends WritableComparator> cmpcl = conf.getClass(
      CompositeInputFormat.JOIN_COMPARATOR, null, WritableComparator.class);
    Lexer lex = new Lexer(expr);
    Stack<Token> st = new Stack<Token>();
    Token tok;
    while ((tok = lex.next()) != null) {
      if (TType.RPAREN.equals(tok.getType())) {
        st.push(reduce(st, conf));
      } else {
        st.push(tok);
      }
    }
    if (st.size() == 1 && TType.CIF.equals(st.peek().getType())) {
      Node ret = st.pop().getNode();
      if (cmpcl != null) {
        ret.setKeyComparator(cmpcl);
      }
      return ret;
    }
    throw new IOException("Missing ')'");
  }

}
