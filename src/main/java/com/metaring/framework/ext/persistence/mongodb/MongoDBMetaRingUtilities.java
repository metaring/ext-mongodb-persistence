/**
 *    Copyright 2019 MetaRing s.r.l.
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.metaring.framework.ext.persistence.mongodb;

import com.metaring.framework.Tools;
import com.metaring.framework.type.DataRepresentation;
import com.metaring.framework.type.factory.DataRepresentationFactory;
import com.metaring.framework.util.StringUtil;

public final class MongoDBMetaRingUtilities {

    private static final DataRepresentationFactory FACTORY = Tools.FACTORY_DATA_REPRESENTATION;
    private static final String ID_FIELD_STANDARD_TYPE = "$oid";
    private static final String ID_FIELD_STANDARD_NAME = "_id";
    private static final String ID_FIELD_REPLACED_NAME = ID_FIELD_STANDARD_NAME.substring(1);

    public static final DataRepresentation getIdForQuery(String idString, String fieldName) {
        if(StringUtil.isNullOrEmpty(idString)) {
            return null;
        }
        DataRepresentation id = FACTORY.create().add(ID_FIELD_STANDARD_TYPE, idString.startsWith("\"") && idString.endsWith("\"") ? FACTORY.create().add(ID_FIELD_STANDARD_TYPE, idString) : FACTORY.fromJson(idString));
        if(!StringUtil.isNullOrEmpty(fieldName)) {
            id = FACTORY.create().add(fieldName, id);
        }
        return id;
    }

    public static final DataRepresentation getIdForQuery(String idString) {
        return getIdForQuery(idString, null);
    }

    public static final DataRepresentation normalizeIdFields(DataRepresentation data) {
        if(data == null || data.isNull()) {
            return data;
        }
        if(data.hasLength()) {
            for(int i = 0; i < data.length(); i++) {
                DataRepresentation item = data.get(i);
                if(item.hasProperties() && item.hasProperty(ID_FIELD_STANDARD_TYPE) && item.isText(ID_FIELD_STANDARD_TYPE)) {
                    data.set(i, item.getText(ID_FIELD_STANDARD_TYPE));
                } else {
                    data.set(i, normalizeIdFields(item));
                }
            }
        }
        if(data.hasProperties()) {
            for(String property : data.getProperties()) {
                DataRepresentation item = data.get(property);
                if(item.hasProperties() && item.hasProperty(ID_FIELD_STANDARD_TYPE) && item.isText(ID_FIELD_STANDARD_TYPE)) {
                    data.add(property.equals(ID_FIELD_STANDARD_NAME) ? ID_FIELD_REPLACED_NAME : property, item.getText(ID_FIELD_STANDARD_TYPE));
                    if(property.equals(ID_FIELD_STANDARD_NAME)) {
                        data.remove(property);
                    }
                } else if(item.hasLength() || item.hasProperties()) {
                    data.add(property, normalizeIdFields(item));
                }
            }
        }
        return data;
    }
}