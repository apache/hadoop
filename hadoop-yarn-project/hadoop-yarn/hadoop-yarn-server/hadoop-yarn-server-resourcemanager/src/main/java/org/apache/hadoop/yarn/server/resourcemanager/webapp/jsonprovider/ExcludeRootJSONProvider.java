package org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import com.google.gson.Gson;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.jaxb.rs.MOXyJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

@Provider
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ExcludeRootJSONProvider extends MOXyJsonProvider {

    private final static Logger LOG = LoggerFactory.getLogger(ExcludeRootJSONProvider.class);
    private final ClassSerialisationConfig classSerialisationConfig;

    public ExcludeRootJSONProvider() {
        this(new Configuration());
    }

    @Inject
    public ExcludeRootJSONProvider(@javax.inject.Named("conf") Configuration conf) {
        classSerialisationConfig = new ClassSerialisationConfig(conf);
    }

    @Override
    public boolean isReadable(Class<?> type, Type genericType,
            Annotation[] annotations, MediaType mediaType) {
        boolean match = classSerialisationConfig.getUnWrappedClasses().contains(type);
        LOG.trace("ExcludeRootJSONProvider compatibility with {} is {}", type, match);
        return match;
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType,
            Annotation[] annotations, MediaType mediaType) {
        return isReadable(type, genericType, annotations, mediaType);
    }

    @Override
    protected void preReadFrom(Class<Object> type, Type genericType,
            Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders,
            Unmarshaller unmarshaller) throws JAXBException {
        LOG.trace("ExcludeRootJSONProvider preReadFrom with {}", type);
        unmarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
    }

    @Override
    protected void preWriteTo(Object object, Class<?> type, Type genericType,
            Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders, Marshaller marshaller)
            throws JAXBException {
        LOG.trace("ExcludeRootJSONProvider preWriteTo with {}", type);
        marshaller.setProperty(MarshallerProperties.JSON_MARSHAL_EMPTY_COLLECTIONS, false);
        marshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
    }
}