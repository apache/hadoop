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
package org.apache.hadoop.yarn.api.resource;

import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.AbstractConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.And;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.SingleConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParseException;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.SourceTags;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.TargetConstraintParser;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.ConstraintParser;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.CardinalityConstraintParser;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.ConjunctionConstraintParser;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.MultipleConstraintsTokenizer;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.SourceTagsTokenizer;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.ConstraintTokenizer;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.*;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTag;

import org.junit.Assert;
import org.junit.Test;

/**
 * Class to test placement constraint parser.
 */
public class TestPlacementConstraintParser {

  @Test
  public void testTargetExpressionParser()
      throws PlacementConstraintParseException {
    ConstraintParser parser;
    AbstractConstraint constraint;
    SingleConstraint single;

    // Anti-affinity with single target tag
    // NOTIN,NDOE,foo
    parser = new TargetConstraintParser("NOTIN, NODE, foo");
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof SingleConstraint);
    single = (SingleConstraint) constraint;
    Assert.assertEquals("node", single.getScope());
    Assert.assertEquals(0, single.getMinCardinality());
    Assert.assertEquals(0, single.getMaxCardinality());

    // lower cases is also valid
    parser = new TargetConstraintParser("notin, node, foo");
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof SingleConstraint);
    single = (SingleConstraint) constraint;
    Assert.assertEquals("node", single.getScope());
    Assert.assertEquals(0, single.getMinCardinality());
    Assert.assertEquals(0, single.getMaxCardinality());

    // Affinity with single target tag
    // IN,NODE,foo
    parser = new TargetConstraintParser("IN, NODE, foo");
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof SingleConstraint);
    single = (SingleConstraint) constraint;
    Assert.assertEquals("node", single.getScope());
    Assert.assertEquals(1, single.getMinCardinality());
    Assert.assertEquals(Integer.MAX_VALUE, single.getMaxCardinality());

    // Anti-affinity with multiple target tags
    // NOTIN,NDOE,foo,bar,exp
    parser = new TargetConstraintParser("NOTIN, NODE, foo, bar, exp");
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof SingleConstraint);
    single = (SingleConstraint) constraint;
    Assert.assertEquals("node", single.getScope());
    Assert.assertEquals(0, single.getMinCardinality());
    Assert.assertEquals(0, single.getMaxCardinality());
    Assert.assertEquals(1, single.getTargetExpressions().size());
    TargetExpression exp =
        single.getTargetExpressions().iterator().next();
    Assert.assertEquals("ALLOCATION_TAG", exp.getTargetType().toString());
    Assert.assertEquals(3, exp.getTargetValues().size());

    // Invalid OP
    parser = new TargetConstraintParser("XYZ, NODE, foo");
    try {
      parser.parse();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof PlacementConstraintParseException);
      Assert.assertTrue(e.getMessage().contains("expecting in or notin"));
    }
  }

  @Test
  public void testCardinalityConstraintParser()
      throws PlacementConstraintParseException {
    ConstraintParser parser;
    AbstractConstraint constraint;
    SingleConstraint single;

    // cardinality,NODE,foo,0,1
    parser = new CardinalityConstraintParser("cardinality, NODE, foo, 0, 1");
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof SingleConstraint);
    single = (SingleConstraint) constraint;
    Assert.assertEquals("node", single.getScope());
    Assert.assertEquals(0, single.getMinCardinality());
    Assert.assertEquals(1, single.getMaxCardinality());
    Assert.assertEquals(1, single.getTargetExpressions().size());
    TargetExpression exp =
        single.getTargetExpressions().iterator().next();
    Assert.assertEquals("ALLOCATION_TAG", exp.getTargetType().toString());
    Assert.assertEquals(1, exp.getTargetValues().size());
    Assert.assertEquals("foo", exp.getTargetValues().iterator().next());

    // cardinality,NODE,foo,bar,moo,0,1
    parser = new CardinalityConstraintParser(
        "cardinality,RACK,foo,bar,moo,0,1");
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof SingleConstraint);
    single = (SingleConstraint) constraint;
    Assert.assertEquals("rack", single.getScope());
    Assert.assertEquals(0, single.getMinCardinality());
    Assert.assertEquals(1, single.getMaxCardinality());
    Assert.assertEquals(1, single.getTargetExpressions().size());
    exp = single.getTargetExpressions().iterator().next();
    Assert.assertEquals("ALLOCATION_TAG", exp.getTargetType().toString());
    Assert.assertEquals(3, exp.getTargetValues().size());
    Set<String> expectedTags = Sets.newHashSet("foo", "bar", "moo");
    Assert.assertTrue(Sets.difference(expectedTags, exp.getTargetValues())
        .isEmpty());

    // Invalid scope string
    try {
      parser = new CardinalityConstraintParser(
          "cardinality,NOWHERE,foo,bar,moo,0,1");
      parser.parse();
      Assert.fail("Expecting a parsing failure!");
    } catch (PlacementConstraintParseException e) {
      Assert.assertTrue(e.getMessage()
          .contains("expecting scope to node or rack, but met NOWHERE"));
    }

    // Invalid number of expression elements
    try {
      parser = new CardinalityConstraintParser(
          "cardinality,NODE,0,1");
      parser.parse();
      Assert.fail("Expecting a parsing failure!");
    } catch (PlacementConstraintParseException e) {
      Assert.assertTrue(e.getMessage()
          .contains("at least 5 elements, but only 4 is given"));
    }
  }

  @Test
  public void testAndConstraintParser()
      throws PlacementConstraintParseException {
    ConstraintParser parser;
    AbstractConstraint constraint;
    And and;

    parser = new ConjunctionConstraintParser(
        "AND(NOTIN,NODE,foo:NOTIN,NODE,bar)");
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof And);
    and = (And) constraint;
    Assert.assertEquals(2, and.getChildren().size());

    parser = new ConjunctionConstraintParser(
        "AND(NOTIN,NODE,foo:cardinality,NODE,foo,0,1)");
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof And);
    Assert.assertEquals(2, and.getChildren().size());

    parser = new ConjunctionConstraintParser(
        "AND(NOTIN,NODE,foo:AND(NOTIN,NODE,foo:cardinality,NODE,foo,0,1))");
    constraint = parser.parse();
    Assert.assertTrue(constraint instanceof And);
    and = (And) constraint;
    Assert.assertTrue(and.getChildren().get(0) instanceof SingleConstraint);
    Assert.assertTrue(and.getChildren().get(1) instanceof And);
    and = (And) and.getChildren().get(1);
    Assert.assertEquals(2, and.getChildren().size());
  }

  @Test
  public void testMultipleConstraintsTokenizer()
      throws PlacementConstraintParseException {
    MultipleConstraintsTokenizer ct;
    SourceTagsTokenizer st;
    TokenizerTester mp;

    ct = new MultipleConstraintsTokenizer(
        "foo=1,A1,A2,A3:bar=2,B1,B2:moo=3,C1,C2");
    mp = new TokenizerTester(ct,
        "foo=1,A1,A2,A3", "bar=2,B1,B2", "moo=3,C1,C2");
    mp.verify();

    ct = new MultipleConstraintsTokenizer(
        "foo=1,AND(A2:A3):bar=2,OR(B1:AND(B2:B3)):moo=3,C1,C2");
    mp = new TokenizerTester(ct,
        "foo=1,AND(A2:A3)", "bar=2,OR(B1:AND(B2:B3))", "moo=3,C1,C2");
    mp.verify();

    ct = new MultipleConstraintsTokenizer("A:B:C");
    mp = new TokenizerTester(ct, "A", "B", "C");
    mp.verify();

    ct = new MultipleConstraintsTokenizer("A:AND(B:C):D");
    mp = new TokenizerTester(ct, "A", "AND(B:C)", "D");
    mp.verify();

    ct = new MultipleConstraintsTokenizer("A:AND(B:OR(C:D)):E");
    mp = new TokenizerTester(ct, "A", "AND(B:OR(C:D))", "E");
    mp.verify();

    ct = new MultipleConstraintsTokenizer("A:AND(B:OR(C:D)):E");
    mp = new TokenizerTester(ct, "A", "AND(B:OR(C:D))", "E");
    mp.verify();

    st = new SourceTagsTokenizer("A=4");
    mp = new TokenizerTester(st, "A", "4");
    mp.verify();

    try {
      st = new SourceTagsTokenizer("A=B");
      mp = new TokenizerTester(st, "A", "B");
      mp.verify();
      Assert.fail("Expecting a parsing failure");
    } catch (PlacementConstraintParseException e) {
      Assert.assertTrue(e.getMessage()
          .contains("Value of the expression must be an integer"));
    }
  }

  private static class TokenizerTester {

    private ConstraintTokenizer tokenizer;
    private String[] expectedExtractions;

    protected TokenizerTester(ConstraintTokenizer tk,
        String... expctedStrings) {
      this.tokenizer = tk;
      this.expectedExtractions = expctedStrings;
    }

    void verify()
        throws PlacementConstraintParseException {
      tokenizer.validate();
      int i = 0;
      while (tokenizer.hasMoreElements()) {
        String current = tokenizer.nextElement();
        Assert.assertTrue(i < expectedExtractions.length);
        Assert.assertEquals(expectedExtractions[i], current);
        i++;
      }
    }
  }

  @Test
  public void testParsePlacementSpec()
      throws PlacementConstraintParseException {
    Map<SourceTags, PlacementConstraint> result;
    PlacementConstraint expectedPc1, expectedPc2;
    PlacementConstraint actualPc1, actualPc2;
    SourceTags tag1, tag2;

    // A single anti-affinity constraint
    result = PlacementConstraintParser
        .parsePlacementSpec("foo=3,notin,node,foo");
    Assert.assertEquals(1, result.size());
    tag1 = result.keySet().iterator().next();
    Assert.assertEquals("foo", tag1.getTag());
    Assert.assertEquals(3, tag1.getNumOfAllocations());
    expectedPc1 = targetNotIn("node", allocationTag("foo")).build();
    actualPc1 = result.values().iterator().next();
    Assert.assertEquals(expectedPc1, actualPc1);

    // Upper case
    result = PlacementConstraintParser
        .parsePlacementSpec("foo=3,NOTIN,NODE,foo");
    Assert.assertEquals(1, result.size());
    tag1 = result.keySet().iterator().next();
    Assert.assertEquals("foo", tag1.getTag());
    Assert.assertEquals(3, tag1.getNumOfAllocations());
    expectedPc1 = targetNotIn("node", allocationTag("foo")).build();
    actualPc1 = result.values().iterator().next();
    Assert.assertEquals(expectedPc1, actualPc1);

    // A single cardinality constraint
    result = PlacementConstraintParser
        .parsePlacementSpec("foo=10,cardinality,node,foo,bar,0,100");
    Assert.assertEquals(1, result.size());
    tag1 = result.keySet().iterator().next();
    Assert.assertEquals("foo", tag1.getTag());
    Assert.assertEquals(10, tag1.getNumOfAllocations());
    expectedPc1 = cardinality("node", 0, 100, "foo", "bar").build();
    Assert.assertEquals(expectedPc1, result.values().iterator().next());

    // Two constraint expressions
    result = PlacementConstraintParser
        .parsePlacementSpec("foo=3,notin,node,foo:bar=2,in,node,foo");
    Assert.assertEquals(2, result.size());
    Iterator<SourceTags> keyIt = result.keySet().iterator();
    tag1 = keyIt.next();
    Assert.assertEquals("foo", tag1.getTag());
    Assert.assertEquals(3, tag1.getNumOfAllocations());
    tag2 = keyIt.next();
    Assert.assertEquals("bar", tag2.getTag());
    Assert.assertEquals(2, tag2.getNumOfAllocations());
    Iterator<PlacementConstraint> valueIt = result.values().iterator();
    expectedPc1 = targetNotIn("node", allocationTag("foo")).build();
    expectedPc2 = targetIn("node", allocationTag("foo")).build();
    Assert.assertEquals(expectedPc1, valueIt.next());
    Assert.assertEquals(expectedPc2, valueIt.next());

    // And constraint
    result = PlacementConstraintParser
        .parsePlacementSpec("foo=1000,and(notin,node,bar:in,node,foo)");
    Assert.assertEquals(1, result.size());
    keyIt = result.keySet().iterator();
    tag1 = keyIt.next();
    Assert.assertEquals("foo", tag1.getTag());
    Assert.assertEquals(1000, tag1.getNumOfAllocations());
    actualPc1 = result.values().iterator().next();
    expectedPc1 = and(targetNotIn("node", allocationTag("bar")),
        targetIn("node", allocationTag("foo"))).build();
    Assert.assertEquals(expectedPc1, actualPc1);

    // Multiple constraints with nested forms.
    result = PlacementConstraintParser.parsePlacementSpec(
            "foo=1000,and(notin,node,bar:or(in,node,foo:in,node,moo))"
                + ":bar=200,notin,node,foo");
    Assert.assertEquals(2, result.size());
    keyIt = result.keySet().iterator();
    tag1 = keyIt.next();
    tag2 = keyIt.next();
    Assert.assertEquals("foo", tag1.getTag());
    Assert.assertEquals(1000, tag1.getNumOfAllocations());
    Assert.assertEquals("bar", tag2.getTag());
    Assert.assertEquals(200, tag2.getNumOfAllocations());
    valueIt = result.values().iterator();
    actualPc1 = valueIt.next();
    actualPc2 = valueIt.next();

    expectedPc1 = and(targetNotIn("node", allocationTag("bar")),
        or(targetIn("node", allocationTag("foo")),
            targetIn("node", allocationTag("moo")))).build();
    Assert.assertEquals(actualPc1, expectedPc1);
    expectedPc2 = targetNotIn("node", allocationTag("foo")).build();
    Assert.assertEquals(expectedPc2, actualPc2);
  }
}
