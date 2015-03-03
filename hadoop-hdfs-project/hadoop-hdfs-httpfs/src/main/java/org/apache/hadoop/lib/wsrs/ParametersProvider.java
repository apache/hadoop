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

package org.apache.hadoop.lib.wsrs;

import com.google.common.collect.Lists;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.StringUtils;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import java.lang.reflect.Type;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Jersey provider that parses the request parameters based on the
 * given parameter definition. 
 */
@InterfaceAudience.Private
public class ParametersProvider
  extends AbstractHttpContextInjectable<Parameters>
  implements InjectableProvider<Context, Type> {

  private String driverParam;
  private Class<? extends Enum> enumClass;
  private Map<Enum, Class<Param<?>>[]> paramsDef;

  public ParametersProvider(String driverParam, Class<? extends Enum> enumClass,
                            Map<Enum, Class<Param<?>>[]> paramsDef) {
    this.driverParam = driverParam;
    this.enumClass = enumClass;
    this.paramsDef = paramsDef;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Parameters getValue(HttpContext httpContext) {
    Map<String, List<Param<?>>> map = new HashMap<String, List<Param<?>>>();
    Map<String, List<String>> queryString =
      httpContext.getRequest().getQueryParameters();
    String str = ((MultivaluedMap<String, String>) queryString).
        getFirst(driverParam);
    if (str == null) {
      throw new IllegalArgumentException(
        MessageFormat.format("Missing Operation parameter [{0}]",
                             driverParam));
    }
    Enum op;
    try {
      op = Enum.valueOf(enumClass, StringUtils.toUpperCase(str));
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(
        MessageFormat.format("Invalid Operation [{0}]", str));
    }
    if (!paramsDef.containsKey(op)) {
      throw new IllegalArgumentException(
        MessageFormat.format("Unsupported Operation [{0}]", op));
    }
    for (Class<Param<?>> paramClass : paramsDef.get(op)) {
      Param<?> param = newParam(paramClass);
      List<Param<?>> paramList = Lists.newArrayList();
      List<String> ps = queryString.get(param.getName());
      if (ps != null) {
        for (String p : ps) {
          try {
            param.parseParam(p);
          }
          catch (Exception ex) {
            throw new IllegalArgumentException(ex.toString(), ex);
          }
          paramList.add(param);
          param = newParam(paramClass);
        }
      } else {
        paramList.add(param);
      }

      map.put(param.getName(), paramList);
    }
    return new Parameters(map);
  }

  private Param<?> newParam(Class<Param<?>> paramClass) {
    try {
      return paramClass.newInstance();
    } catch (Exception ex) {
      throw new UnsupportedOperationException(
        MessageFormat.format(
          "Param class [{0}] does not have default constructor",
          paramClass.getName()));
    }
  }

  @Override
  public ComponentScope getScope() {
    return ComponentScope.PerRequest;
  }

  @Override
  public Injectable getInjectable(ComponentContext componentContext, Context context, Type type) {
    return (type.equals(Parameters.class)) ? this : null;
  }
}
