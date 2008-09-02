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

package org.apache.hadoop.hive.serde.dynamic_type;

import org.apache.hadoop.hive.serde.*;
import java.lang.reflect.*;

/**
 * The default implementation of Hive Field based on Java Reflection.
 */

public class DynamicSerDeHiveField implements SerDeField {

    protected DynamicSerDeTypeBase _parentMetaType;
    protected DynamicSerDeTypeBase _metaType;
    protected DynamicSerDeTypeBase _valueMetaType;
    protected DynamicSerDeTypeBase _keyMetaType;

    protected Class _parentClass;
    protected Class _class;
    protected String _fieldName;
    protected boolean _isList;
    protected boolean _isMap;
    protected boolean _isClassPrimitive;
    protected Class _valueClass;
    protected Class _keyClass;


    public static boolean isClassPrimitive(Class c) {
        return ((c == String.class) || (c == Boolean.class) ||
                (c == Character.class) ||
                java.lang.Number.class.isAssignableFrom(c) ||
                c.isPrimitive());
    }

    public DynamicSerDeHiveField(DynamicSerDeStructBase parent, String fieldName) throws SerDeException {
        try {
            _parentClass = parent.getRealType();
            _parentMetaType = parent;

            _fieldName = fieldName;
            _metaType =  parent.getFieldList().getFieldByName(fieldName);

            _isList = _metaType.isList();
            _isMap = _metaType.isMap();
            _isClassPrimitive = _metaType.isPrimitive();

            if(_isList) {
                DynamicSerDeTypeList type = (DynamicSerDeTypeList)_metaType;
                _valueClass = type.getElementType().getRealType();
                _valueMetaType = type.getElementType();
            }
            if(_isMap) {
                DynamicSerDeTypeMap type = (DynamicSerDeTypeMap)_metaType;
                _keyClass = type.getKeyType().getRealType();
                _valueClass = type.getValueType().getRealType();
                _keyMetaType = type.getKeyType();
                _valueMetaType = type.getValueType();
            }

            _class = _metaType.getRealType();

            if(_class == null) {
                System.err.println("_metaType.getClass().getName()=" + _metaType.getClass().getName());
                throw new SerDeException("could not get the real type for " + _metaType.name + ":" + _metaType);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new SerDeException("Illegal class or member:" + e.getMessage());
        }
    }

    public Object get(Object obj) throws SerDeException {
        try {
            DynamicSerDeTypeContainer container = (DynamicSerDeTypeContainer)obj;
            return container.fields.get(_fieldName);
        } catch (Exception e) {
            throw new SerDeException("Illegal object or access error", e);
        }
    }

    public boolean isList() {
        return _isList;
    }

    public boolean isMap() {
        return _isMap;
    }

    public boolean isPrimitive() {
        if(_isList || _isMap)
            return false;

        return _isClassPrimitive;
    }

    public Class getType() {
        return _class;
    }

    public DynamicSerDeTypeBase getMetaType() {
        return _metaType;
    }

    public DynamicSerDeTypeBase getListElementMetaType() {
        if(_isList) {
            return _valueMetaType;
        } else {
            throw new RuntimeException("Not a list field ");
        }
    }

    public DynamicSerDeTypeBase getMapKeyMetaType() {
        if(_isMap) {
            return _keyMetaType;
        } else {
            throw new RuntimeException("Not a list field ");
        }
    }

    public DynamicSerDeTypeBase getValueMetaType() {
        if(_isMap) {
            return _valueMetaType;
        } else {
            throw new RuntimeException("Not a list field ");
        }
    }

    public Class getListElementType() {
        if(_isList) {
            return _valueClass;
        } else {
            throw new RuntimeException("Not a list field ");
        }
    }

    public Class getMapKeyType() {
        if(_isMap) {
            return _keyClass;
        } else {
            throw new RuntimeException("Not a map field ");
        }
    }

    public Class getMapValueType() {
        if(_isMap) {
            return _valueClass;
        } else {
            throw new RuntimeException("Not a map field ");
        }
    }

    public String getName() {
        return _fieldName;
    }

    public static String fieldToString(SerDeField hf) {
        return("Field= "+hf.getName() +
               ", isPrimitive="+hf.isPrimitive()+
               ", isList="+hf.isList()+(hf.isList()?" of "+hf.getListElementType().getName():"")+
               ", isMap="+hf.isMap()+(hf.isMap()?" of <"+hf.getMapKeyType().getName()+","
                                      +hf.getMapValueType().getName()+">":"")+
               ", type="+hf.getType().getName());
    }
}
