package org.apache.hadoop.fs.azurebfs.rules;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class AuthTestsRule implements TestRule {

    private AuthTypesTestable testObj;

    public AuthTestsRule(AuthTypesTestable testObj){
        this.testObj = testObj;
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return new AuthTestStatement(statement, description, testObj);
    }
}
