package org.apache.hadoop.yarn.server.nodemanager.webapp.jsonprovider;

import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfoes;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.jaxb.rs.MOXyJsonProvider;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

@Provider
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class NMJsonProvider extends MOXyJsonProvider {

  private boolean isRootElementNeeded (Class<?> type) {
    return type.equals(ContainerLogsInfoes.class) ? false : true;
  }

  @Override
  protected void preReadFrom(Class<Object> type, Type genericType,
                             Annotation[] annotations, MediaType mediaType,
                             MultivaluedMap<String, String> httpHeaders,
                             Unmarshaller unmarshaller) throws JAXBException {
    unmarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, isRootElementNeeded(type));
  }

  @Override
  protected void preWriteTo(Object object, Class<?> type, Type genericType,
                            Annotation[] annotations, MediaType mediaType,
                            MultivaluedMap<String, Object> httpHeaders, Marshaller marshaller)
      throws JAXBException {
    marshaller.setProperty(MarshallerProperties.JSON_MARSHAL_EMPTY_COLLECTIONS, false);
    marshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, isRootElementNeeded(type));
  }
}
