package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.Abfs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AbfsClientThrottlingInterceptFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AbfsClientThrottlingInterceptFactory.class);
    private static Map<String, AbfsClientThrottlingIntercept> instanceMapping = new ConcurrentHashMap<>();

    public static synchronized AbfsClientThrottlingIntercept getInstance(String accountName, boolean isAutoThrottlingEnabled,
                                                                         boolean isSingletonEnabled) {
        AbfsClientThrottlingIntercept instance;
        if (isSingletonEnabled) {
            instance = AbfsClientThrottlingIntercept.initializeSingleton(isAutoThrottlingEnabled);
        } else {
            if (!isAutoThrottlingEnabled) {
                return null;
            }
            if (instanceMapping.get(accountName) == null) {
                LOG.debug("The accountNameis: {} ", accountName);
                instance = new AbfsClientThrottlingIntercept(accountName);
                instanceMapping.put(accountName, instance);
            } else {
                instance = instanceMapping.get(accountName);
            }
        }
        return instance;
    }
}
