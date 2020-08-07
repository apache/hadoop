package org.apache.hadoop.fs.azurebfs.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;

public class AuthTestStatement extends Statement {

    private static final Logger LOG =
            LoggerFactory.getLogger(AuthTestStatement.class);

    private final Statement base;
    private final Description description;
    private final AuthTypesTestable testObj;

    public AuthTestStatement(Statement base, Description description, AuthTypesTestable testObj) {
        this.base = base;
        this.description = description;
        this.testObj = testObj;
    }

    private Collection<AuthType> authTypesToTest() {
        AuthTypesToTest authTypesToTest = description
            .getAnnotation(AuthTypesToTest.class);
        if (authTypesToTest != null) {
            AuthType[] values = authTypesToTest.values();
            if (values != null && values.length > 0) {
                return Arrays.asList(values);
            }
        }
        final List authTypes = new ArrayList();
        authTypes.add(AuthType.OAuth);
        authTypes.add(AuthType.SharedKey);
        //authTypes.add(AuthType.SAS);
        return authTypes;
    }

    @Override
    public void evaluate() throws Throwable {
        if(testObj.isAuthTypeTestsEnabled()){
            testForAuthTypes();
        }else{
            base.evaluate();
        }
    }

    private void testForAuthTypes() throws Throwable {
        String testMethod = description.getTestClass() + "#" + description.getMethodName() + "-";
        String test = "";
        try {
            for (AuthType authType : authTypesToTest()) {
                test = testMethod + authType;
                System.out.println(test);
                testObj.setAuthType(authType);
                testObj.getConfiguration()
                    .set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME,
                        authType.name());
                testObj.initFSEndpointForNewFS();
                base.evaluate();
            }
        } catch (Exception e) {
            LOG.debug(test + " failed. ", e);
            throw e;
        }
    }
}

