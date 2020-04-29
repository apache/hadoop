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
package org.apache.hadoop.yarn.util.constraint;

import com.google.common.base.Strings;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType;
import org.apache.hadoop.yarn.api.records.NodeAttributeOpCode;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.AbstractConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets;

import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.Set;
import java.util.TreeSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Placement constraint expression parser.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class PlacementConstraintParser {

  public static final char EXPRESSION_VAL_DELIM = ',';
  private static final char EXPRESSION_DELIM = ':';
  private static final char KV_SPLIT_DELIM = '=';
  private static final char BRACKET_START = '(';
  private static final char BRACKET_END = ')';
  private static final char NAMESPACE_DELIM = '/';
  private static final String KV_NE_DELIM = "!=";
  private static final String IN = "in";
  private static final String NOT_IN = "notin";
  private static final String AND = "and";
  private static final String OR = "or";
  private static final String CARDINALITY = "cardinality";
  private static final String SCOPE_NODE = PlacementConstraints.NODE;
  private static final String SCOPE_RACK = PlacementConstraints.RACK;

  private PlacementConstraintParser() {
    // Private constructor for this utility class.
  }

  /**
   * Constraint Parser used to parse placement constraints from a
   * given expression.
   */
  public static abstract class ConstraintParser {

    private final ConstraintTokenizer tokenizer;

    public ConstraintParser(ConstraintTokenizer tk){
      this.tokenizer = tk;
    }

    void validate() throws PlacementConstraintParseException {
      tokenizer.validate();
    }

    void shouldHaveNext()
        throws PlacementConstraintParseException {
      if (!tokenizer.hasMoreElements()) {
        throw new PlacementConstraintParseException("Expecting more tokens");
      }
    }

    String nextToken() {
      return this.tokenizer.nextElement().trim();
    }

    boolean hasMoreTokens() {
      return this.tokenizer.hasMoreElements();
    }

    int toInt(String name) throws PlacementConstraintParseException {
      try {
        return Integer.parseInt(name);
      } catch (NumberFormatException e) {
        throw new PlacementConstraintParseException(
            "Expecting an Integer, but get " + name);
      }
    }

    TargetExpression parseNameSpace(String targetTag)
        throws PlacementConstraintParseException {
      int i = targetTag.lastIndexOf(NAMESPACE_DELIM);
      if (i != -1) {
        String namespace = targetTag.substring(0, i);
        for (AllocationTagNamespaceType type :
            AllocationTagNamespaceType.values()) {
          if (type.getTypeKeyword().equals(namespace)) {
            return PlacementTargets.allocationTagWithNamespace(
                namespace, targetTag.substring(i+1));
          }
        }
        throw new PlacementConstraintParseException(
            "Invalid namespace prefix: " + namespace);
      } else {
        return PlacementTargets.allocationTag(targetTag);
      }
    }

    String parseScope(String scopeString)
        throws PlacementConstraintParseException {
      if (scopeString.equalsIgnoreCase(SCOPE_NODE)) {
        return SCOPE_NODE;
      } else if (scopeString.equalsIgnoreCase(SCOPE_RACK)) {
        return SCOPE_RACK;
      } else {
        throw new PlacementConstraintParseException(
            "expecting scope to " + SCOPE_NODE + " or " + SCOPE_RACK
                + ", but met " + scopeString);
      }
    }

    public AbstractConstraint tryParse() {
      try {
        return parse();
      } catch (PlacementConstraintParseException e) {
        // unable to parse, simply return null
        return null;
      }
    }

    public abstract AbstractConstraint parse()
        throws PlacementConstraintParseException;
  }

  /**
   * Tokenizer interface that used to parse an expression. It first
   * validates if the syntax of the given expression is valid, then traverse
   * the expression and parse it to an enumeration of strings. Each parsed
   * string can be further consumed by a {@link ConstraintParser} and
   * transformed to a {@link AbstractConstraint}.
   */
  public interface ConstraintTokenizer extends Enumeration<String> {

    /**
     * Validate the schema before actual parsing the expression.
     * @throws PlacementConstraintParseException
     */
    default void validate() throws PlacementConstraintParseException {
      // do nothing
    }
  }

  /**
   * A basic tokenizer that splits an expression by a given delimiter.
   */
  public static class BaseStringTokenizer implements ConstraintTokenizer {
    private final StringTokenizer tokenizer;
    BaseStringTokenizer(String expr, String delimiter) {
      this.tokenizer = new StringTokenizer(expr, delimiter);
    }

    @Override
    public boolean hasMoreElements() {
      return tokenizer.hasMoreTokens();
    }

    @Override
    public String nextElement() {
      return tokenizer.nextToken();
    }
  }

  /**
   * Tokenizer used to parse conjunction form of a constraint expression,
   * [AND|OR](C1:C2:...:Cn). Each Cn is a constraint expression.
   */
  public static final class ConjunctionTokenizer
      implements ConstraintTokenizer {

    private final String expression;
    private Iterator<String> iterator;

    private ConjunctionTokenizer(String expr) {
      this.expression = expr;
    }

    // Traverse the expression and try to get a list of parsed elements
    // based on schema.
    @Override
    public void validate() throws PlacementConstraintParseException {
      List<String> parsedElements = new ArrayList<>();
      // expression should start with AND or OR
      String op;
      if (expression.startsWith(AND) ||
          expression.startsWith(AND.toUpperCase())) {
        op = AND;
      } else if(expression.startsWith(OR) ||
          expression.startsWith(OR.toUpperCase())) {
        op = OR;
      } else {
        throw new PlacementConstraintParseException(
            "Excepting starting with \"" + AND + "\" or \"" + OR + "\","
                + " but met " + expression);
      }
      parsedElements.add(op);
      Pattern p = Pattern.compile("\\((.*)\\)");
      Matcher m = p.matcher(expression);
      if (!m.find()) {
        throw new PlacementConstraintParseException("Unexpected format,"
            + " expecting [AND|OR](A:B...) "
            + "but current expression is " + expression);
      }
      String childStrs = m.group(1);
      MultipleConstraintsTokenizer ct =
          new MultipleConstraintsTokenizer(childStrs);
      ct.validate();
      while(ct.hasMoreElements()) {
        parsedElements.add(ct.nextElement());
      }
      this.iterator = parsedElements.iterator();
    }

    @Override
    public boolean hasMoreElements() {
      return iterator.hasNext();
    }

    @Override
    public String nextElement() {
      return iterator.next();
    }
  }

  /**
   * Tokenizer used to parse allocation tags expression, which should be
   * in tag(numOfAllocations) syntax.
   */
  public static class SourceTagsTokenizer implements ConstraintTokenizer {

    private final String expression;
    private StringTokenizer st;
    private Iterator<String> iterator;
    public SourceTagsTokenizer(String expr) {
      this.expression = expr;
      st = new StringTokenizer(expr, String.valueOf(BRACKET_START));
    }

    @Override
    public void validate() throws PlacementConstraintParseException {
      ArrayList<String> parsedValues = new ArrayList<>();
      if (st.countTokens() != 2  || !this.expression.
          endsWith(String.valueOf(BRACKET_END))) {
        throw new PlacementConstraintParseException(
            "Expecting source allocation tag to be specified"
            + " sourceTag(numOfAllocations) syntax,"
            + " but met " + expression);
      }

      String sourceTag = st.nextToken();
      parsedValues.add(sourceTag);
      String str = st.nextToken();
      String num = str.substring(0, str.length() - 1);
      try {
        Integer.parseInt(num);
        parsedValues.add(num);
      } catch (NumberFormatException e) {
        throw new PlacementConstraintParseException("Value of the expression"
            + " must be an integer, but met " + num);
      }
      iterator = parsedValues.iterator();
    }

    @Override
    public boolean hasMoreElements() {
      return iterator.hasNext();
    }

    @Override
    public String nextElement() {
      return iterator.next();
    }
  }

  /**
   * Tokenizer used to handle a placement spec composed by multiple
   * constraint expressions. Each of them is delimited with the
   * given delimiter, e.g ':'.
   */
  public static class MultipleConstraintsTokenizer
      implements ConstraintTokenizer {

    private final String expr;
    private Iterator<String> iterator;

    public MultipleConstraintsTokenizer(String expression) {
      this.expr = expression;
    }

    @Override
    public void validate() throws PlacementConstraintParseException {
      ArrayList<String> parsedElements = new ArrayList<>();
      char[] arr = expr.toCharArray();
      // Memorize the location of each delimiter in a stack,
      // removes invalid delimiters that embraced in brackets.
      Stack<Integer> stack = new Stack<>();
      for (int i=0; i<arr.length; i++) {
        char current = arr[i];
        switch (current) {
        case EXPRESSION_DELIM:
          stack.add(i);
          break;
        case BRACKET_START:
          stack.add(i);
          break;
        case BRACKET_END:
          while(!stack.isEmpty()) {
            if (arr[stack.pop()] == BRACKET_START) {
              break;
            }
          }
          break;
        default:
          break;
        }
      }

      if (stack.isEmpty()) {
        // Single element
        parsedElements.add(expr);
      } else {
        Iterator<Integer> it = stack.iterator();
        int currentPos = 0;
        while (it.hasNext()) {
          int pos = it.next();
          String sub = expr.substring(currentPos, pos);
          if (sub != null && !sub.isEmpty()) {
            parsedElements.add(sub);
          }
          currentPos = pos+1;
        }
        if (currentPos < expr.length()) {
          parsedElements.add(expr.substring(currentPos, expr.length()));
        }
      }
      iterator = parsedElements.iterator();
    }

    @Override
    public boolean hasMoreElements() {
      return iterator.hasNext();
    }

    @Override
    public String nextElement() {
      return iterator.next();
    }
  }

  /**
   * Constraint parser used to parse a given target expression.
   */
  public static class NodeConstraintParser extends ConstraintParser {

    public NodeConstraintParser(String expression) {
      super(new BaseStringTokenizer(expression,
          String.valueOf(EXPRESSION_VAL_DELIM)));
    }

    @Override
    public AbstractConstraint parse()
        throws PlacementConstraintParseException {
      PlacementConstraint.AbstractConstraint placementConstraints = null;
      String attributeName = "";
      NodeAttributeOpCode opCode = NodeAttributeOpCode.EQ;
      String scope = SCOPE_NODE;

      Set<String> constraintEntities = new TreeSet<>();
      while (hasMoreTokens()) {
        String currentTag = nextToken();
        StringTokenizer attributeKV = getAttributeOpCodeTokenizer(currentTag);

        // Usually there will be only one k=v pair. However in case when
        // multiple values are present for same attribute, it will also be
        // coming as next token. for example, java=1.8,1.9 or python!=2.
        if (attributeKV.countTokens() > 1) {
          opCode = getAttributeOpCode(currentTag);
          attributeName = attributeKV.nextToken();
          currentTag = attributeKV.nextToken();
        }
        constraintEntities.add(currentTag);
      }

      if(attributeName.isEmpty()) {
        throw new PlacementConstraintParseException(
            "expecting valid expression like k=v or k!=v, but get "
                + constraintEntities);
      }

      TargetExpression target = null;
      if (!constraintEntities.isEmpty()) {
        target = PlacementTargets.nodeAttribute(attributeName,
            constraintEntities
            .toArray(new String[constraintEntities.size()]));
      }

      placementConstraints = PlacementConstraints
          .targetNodeAttribute(scope, opCode, target);
      return placementConstraints;
    }

    private StringTokenizer getAttributeOpCodeTokenizer(String currentTag) {
      StringTokenizer attributeKV = new StringTokenizer(currentTag,
          KV_NE_DELIM);

      // Try with '!=' delim as well.
      if (attributeKV.countTokens() < 2) {
        attributeKV = new StringTokenizer(currentTag,
            String.valueOf(KV_SPLIT_DELIM));
      }
      return attributeKV;
    }

    /**
     * Below conditions are validated.
     * java=8   : OpCode = EQUALS
     * java!=8  : OpCode = NEQUALS
     * @param currentTag tag
     * @return Attribute op code.
     */
    private NodeAttributeOpCode getAttributeOpCode(String currentTag)
        throws PlacementConstraintParseException {
      if (currentTag.contains(KV_NE_DELIM)) {
        return NodeAttributeOpCode.NE;
      } else if (currentTag.contains(String.valueOf(KV_SPLIT_DELIM))) {
        return NodeAttributeOpCode.EQ;
      }
      throw new PlacementConstraintParseException(
          "expecting valid expression like k=v or k!=v, but get "
              + currentTag);
    }
  }

  /**
   * Constraint parser used to parse a given target expression, such as
   * "NOTIN, NODE, foo, bar".
   */
  public static class TargetConstraintParser extends ConstraintParser {

    public TargetConstraintParser(String expression) {
      super(new BaseStringTokenizer(expression,
          String.valueOf(EXPRESSION_VAL_DELIM)));
    }

    @Override
    public AbstractConstraint parse()
        throws PlacementConstraintParseException {
      PlacementConstraint.AbstractConstraint placementConstraints = null;
      String op = nextToken();
      if (op.equalsIgnoreCase(IN) || op.equalsIgnoreCase(NOT_IN)) {
        String scope = nextToken();
        scope = parseScope(scope);

        Set<TargetExpression> targetExpressions = new HashSet<>();
        while(hasMoreTokens()) {
          String tag = nextToken();
          TargetExpression t = parseNameSpace(tag);
          targetExpressions.add(t);
        }
        TargetExpression[] targetArr = targetExpressions.toArray(
            new TargetExpression[targetExpressions.size()]);
        if (op.equalsIgnoreCase(IN)) {
          placementConstraints = PlacementConstraints
              .targetIn(scope, targetArr);
        } else {
          placementConstraints = PlacementConstraints
              .targetNotIn(scope, targetArr);
        }
      } else {
        throw new PlacementConstraintParseException(
            "expecting " + IN + " or " + NOT_IN + ", but get " + op);
      }
      return placementConstraints;
    }
  }

  /**
   * Constraint parser used to parse a given target expression, such as
   * "cardinality, NODE, foo, 0, 1".
   */
  public static class CardinalityConstraintParser extends ConstraintParser {

    public CardinalityConstraintParser(String expr) {
      super(new BaseStringTokenizer(expr,
          String.valueOf(EXPRESSION_VAL_DELIM)));
    }

    @Override
    public AbstractConstraint parse()
        throws PlacementConstraintParseException {
      String op = nextToken();
      if (!op.equalsIgnoreCase(CARDINALITY)) {
        throw new PlacementConstraintParseException("expecting " + CARDINALITY
            + " , but met " + op);
      }

      shouldHaveNext();
      String scope = nextToken();
      scope = parseScope(scope);

      Stack<String> resetElements = new Stack<>();
      while(hasMoreTokens()) {
        resetElements.add(nextToken());
      }

      // At least 3 elements
      if (resetElements.size() < 3) {
        throw new PlacementConstraintParseException(
            "Invalid syntax for a cardinality expression, expecting"
                + " \"cardinality,SCOPE,TARGET_TAG,...,TARGET_TAG,"
                + "MIN_CARDINALITY,MAX_CARDINALITY\" at least 5 elements,"
                + " but only " + (resetElements.size() + 2) + " is given.");
      }

      String maxCardinalityStr = resetElements.pop();
      int max = toInt(maxCardinalityStr);

      String minCardinalityStr = resetElements.pop();
      int min = toInt(minCardinalityStr);

      Set<TargetExpression> targetExpressions = new HashSet<>();
      while (!resetElements.empty()) {
        String tag = resetElements.pop();
        TargetExpression t = parseNameSpace(tag);
        targetExpressions.add(t);
      }
      TargetExpression[] targetArr = targetExpressions.toArray(
          new TargetExpression[targetExpressions.size()]);

      return PlacementConstraints.targetCardinality(scope, min, max, targetArr);
    }
  }

  /**
   * Parser used to parse conjunction form of constraints, such as
   * AND(A, ..., B), OR(A, ..., B).
   */
  public static class ConjunctionConstraintParser extends ConstraintParser {

    public ConjunctionConstraintParser(String expr) {
      super(new ConjunctionTokenizer(expr));
    }

    @Override
    public AbstractConstraint parse() throws PlacementConstraintParseException {
      // do pre-process, validate input.
      validate();
      String op = nextToken();
      shouldHaveNext();
      List<AbstractConstraint> constraints = new ArrayList<>();
      while(hasMoreTokens()) {
        // each child expression can be any valid form of
        // constraint expressions.
        String constraintStr = nextToken();
        AbstractConstraint constraint = parseExpression(constraintStr);
        constraints.add(constraint);
      }
      if (AND.equalsIgnoreCase(op)) {
        return PlacementConstraints.and(
            constraints.toArray(
                new AbstractConstraint[constraints.size()]));
      } else if (OR.equalsIgnoreCase(op)) {
        return PlacementConstraints.or(
            constraints.toArray(
                new AbstractConstraint[constraints.size()]));
      } else {
        throw new PlacementConstraintParseException(
            "Unexpected conjunction operator : " + op
                + ", expecting " + AND + " or " + OR);
      }
    }
  }

  /**
   * A helper class to encapsulate source tags and allocations in the
   * placement specification.
   */
  public static final class SourceTags {
    private String tag;
    private int num;

    private SourceTags(String sourceTag, int number) {
      this.tag = sourceTag;
      this.num = number;
    }

    public static SourceTags emptySourceTags() {
      return new SourceTags("", 0);
    }

    public boolean isEmpty() {
      return Strings.isNullOrEmpty(tag) && num == 0;
    }

    public String getTag() {
      return this.tag;
    }

    public int getNumOfAllocations() {
      return this.num;
    }

    /**
     * Parses source tags from expression "sourceTags(numOfAllocations)".
     * @param expr
     * @return source tags, see {@link SourceTags}
     * @throws PlacementConstraintParseException
     */
    public static SourceTags parseFrom(String expr)
        throws PlacementConstraintParseException {
      SourceTagsTokenizer stt = new SourceTagsTokenizer(expr);
      stt.validate();

      // During validation we already checked the number of parsed elements.
      String allocTag = stt.nextElement();
      int allocNum = Integer.parseInt(stt.nextElement());
      return new SourceTags(allocTag, allocNum);
    }
  }

  /**
   * Parses a given constraint expression to a {@link AbstractConstraint},
   * this expression can be any valid form of constraint expressions.
   *
   * @param constraintStr expression string
   * @return a parsed {@link AbstractConstraint}
   * @throws PlacementConstraintParseException when given expression
   * is malformed
   */
  public static AbstractConstraint parseExpression(String constraintStr)
      throws PlacementConstraintParseException {
    // Try parse given expression with all allowed constraint parsers,
    // fails if no one could parse it.
    TargetConstraintParser tp = new TargetConstraintParser(constraintStr);
    Optional<AbstractConstraint> constraintOptional =
        Optional.ofNullable(tp.tryParse());
    if (!constraintOptional.isPresent()) {
      CardinalityConstraintParser cp =
          new CardinalityConstraintParser(constraintStr);
      constraintOptional = Optional.ofNullable(cp.tryParse());
      if (!constraintOptional.isPresent()) {
        ConjunctionConstraintParser jp =
            new ConjunctionConstraintParser(constraintStr);
        constraintOptional = Optional.ofNullable(jp.tryParse());
      }
      if (!constraintOptional.isPresent()) {
        NodeConstraintParser np =
            new NodeConstraintParser(constraintStr);
        constraintOptional = Optional.ofNullable(np.tryParse());
      }
      if (!constraintOptional.isPresent()) {
        throw new PlacementConstraintParseException(
            "Invalid constraint expression " + constraintStr);
      }
    }
    return constraintOptional.get();
  }

  /**
   * Parses a placement constraint specification. A placement constraint spec
   * is a composite expression which is composed by multiple sub constraint
   * expressions delimited by ":". With following syntax:
   *
   * <p>Tag1(N1),P1:Tag2(N2),P2:...:TagN(Nn),Pn</p>
   *
   * where <b>TagN(Nn)</b> is a key value pair to determine the source
   * allocation tag and the number of allocations, such as:
   *
   * <p>foo(3)</p>
   *
   * Optional when using NodeAttribute Constraint.
   *
   * and where <b>Pn</b> can be any form of a valid constraint expression,
   * such as:
   *
   * <ul>
   *   <li>in,node,foo,bar</li>
   *   <li>notin,node,foo,bar,1,2</li>
   *   <li>and(notin,node,foo:notin,node,bar)</li>
   * </ul>
   *
   * and NodeAttribute Constraint such as
   *
   * <ul>
   *   <li>yarn.rm.io/foo=true</li>
   *   <li>java=1.7,1.8</li>
   * </ul>
   * @param expression expression string.
   * @return a map of source tags to placement constraint mapping.
   * @throws PlacementConstraintParseException
   */
  public static Map<SourceTags, PlacementConstraint> parsePlacementSpec(
      String expression) throws PlacementConstraintParseException {
    // Continue handling for application tag based constraint otherwise.
    // Respect insertion order.
    Map<SourceTags, PlacementConstraint> result = new LinkedHashMap<>();
    PlacementConstraintParser.ConstraintTokenizer tokenizer =
        new PlacementConstraintParser.MultipleConstraintsTokenizer(expression);
    tokenizer.validate();
    while (tokenizer.hasMoreElements()) {
      String specStr = tokenizer.nextElement();
      // each spec starts with sourceAllocationTag(numOfContainers) and
      // followed by a constraint expression.
      // foo(4),Pn
      final SourceTags st;
      PlacementConstraint constraint;
      String delimiter = new String(new char[]{'[', BRACKET_END, ']',
          EXPRESSION_VAL_DELIM});
      String[] splitted = specStr.split(delimiter, 2);
      if (splitted.length == 2) {
        st = SourceTags.parseFrom(splitted[0] + String.valueOf(BRACKET_END));
        constraint = PlacementConstraintParser.parseExpression(splitted[1]).
            build();
      } else if (splitted.length == 1) {
        // Either Node Attribute Constraint or Source Allocation Tag alone
        NodeConstraintParser np = new NodeConstraintParser(specStr);
        Optional<AbstractConstraint> constraintOptional =
            Optional.ofNullable(np.tryParse());
        if (constraintOptional.isPresent()) {
          st = SourceTags.emptySourceTags();
          constraint = constraintOptional.get().build();
        } else {
          st = SourceTags.parseFrom(specStr);
          constraint = null;
        }
      } else {
        throw new PlacementConstraintParseException(
            "Unexpected placement constraint expression " + specStr);
      }
      result.put(st, constraint);
    }

    // Validation
    Set<SourceTags> sourceTagSet = result.keySet();
    if (sourceTagSet.stream()
        .filter(sourceTags -> sourceTags.isEmpty())
        .findAny()
        .isPresent()) {
      // Source tags, e.g foo(3), is optional for a node-attribute constraint,
      // but when source tags is absent, the parser only accept single
      // constraint expression to avoid ambiguous semantic. This is because
      // DS AM is requesting number of containers per the number specified
      // in the source tags, we do overwrite when there is no source tags
      // with num_containers argument from commandline. If that is partially
      // missed in the constraints, we don't know if it is ought to
      // overwritten or not.
      if (result.size() != 1) {
        throw new PlacementConstraintParseException(
            "Source allocation tags is required for a multi placement"
                + " constraint expression.");
      }
    }
    return result;
  }
}
