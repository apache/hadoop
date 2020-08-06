package org.apache.hadoop.fs.azurebfs.rules;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;

public class AuthTestStatement extends Statement {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractAbfsIntegrationTest.class);

    private final Statement base;
    private final Description description;
    private final AbstractAbfsIntegrationTest testObj;

    public AuthTestStatement(Statement base, Description description, AbstractAbfsIntegrationTest testObj) {
        this.base = base;
        this.description = description;
        this.testObj = testObj;
    }

    @Override
    public void evaluate() throws Throwable {
        String testMethod = description.getTestClass() + "#" + description.getMethodName() + "-";
        String test = "";
        try {
            for (AuthType authType : testObj.authTypesToTest()) {
                test = testMethod + authType;
                System.out.println(test);
                testObj.initFSEndpointForNewFS();
                testObj.setAuthTypeForTest(authType);
                AbfsConfiguration conf = testObj.getConfiguration();
                conf.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, authType.name());
                base.evaluate();
            }
        } catch (Exception e) {
            LOG.debug(test + " failed. ", e);
        }
    }
}

