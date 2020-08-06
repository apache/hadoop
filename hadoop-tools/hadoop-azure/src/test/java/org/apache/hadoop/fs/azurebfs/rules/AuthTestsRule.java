package org.apache.hadoop.fs.azurebfs.rules;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class AuthTestsRule implements TestRule {

    private AbstractAbfsIntegrationTest testObj;

    public AuthTestsRule(AbstractAbfsIntegrationTest testObj){
        this.testObj = testObj;
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return new AuthTestStatement(statement, description, testObj);
    }
}
