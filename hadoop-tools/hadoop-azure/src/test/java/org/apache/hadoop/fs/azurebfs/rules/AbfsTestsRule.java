package org.apache.hadoop.fs.azurebfs.rules;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class AbfsTestsRule implements TestRule {

    private AbfsTestable testObj;

    public AbfsTestsRule(AbfsTestable testObj){
        this.testObj = testObj;
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return new AbfsTestStatement(statement, description, testObj);
    }
}
