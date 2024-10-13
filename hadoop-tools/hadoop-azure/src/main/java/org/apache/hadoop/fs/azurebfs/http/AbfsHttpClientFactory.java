package org.apache.hadoop.fs.azurebfs.http;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class AbfsHttpClientFactory {

    public static AbfsHttpClient getInstance(AbfsConfiguration abfsConfiguration) {
        Class<? extends AbfsHttpClient> httpClientClass = abfsConfiguration.getHttpClientClass();
        Constructor<? extends AbfsHttpClient> constructor = null;
        try {
            constructor = httpClientClass.getDeclaredConstructor(AbfsConfiguration.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to find constructor " + httpClientClass.getName() + "(AbfsConfiguration)", e);
        }
        try {
            return constructor.newInstance(abfsConfiguration);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
