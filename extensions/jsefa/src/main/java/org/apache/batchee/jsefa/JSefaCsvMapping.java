/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.jsefa;

import net.sf.jsefa.csv.annotation.CsvDataType;
import net.sf.jsefa.csv.annotation.CsvField;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class JSefaCsvMapping {

    /**
     * storage for CsvHeaders
     */
    private final List<String> headers = new ArrayList<String>();


    private JSefaCsvMapping(Class<?> type) {
        calculateHeaders(type);
    }


    public Iterable<String> getHeader() {
        return headers;
    }



    private void calculateHeaders(Class<?> type) {

        SortedMap<Integer, SortedMap<Integer, String>> allHeaders = new TreeMap<Integer, SortedMap<Integer, String>>();

        calculateHeaders(type, allHeaders, 0);

        for (SortedMap<Integer, String> headerMap : allHeaders.values()) {

            for (String header : headerMap.values()) {
                headers.add(header);
            }
        }
    }

    private void calculateHeaders(Class<?> type, SortedMap<Integer, SortedMap<Integer, String>> allHeaders, int index) {

        if (type.getSuperclass() != Object.class) {
            // decrement index for inheritance, because index will be the same
            // but if we have another object within OR the same positions specified in the @CsvField annotation
            // we override the values
            calculateHeaders(type.getSuperclass(), allHeaders, index - 1);
        }

        SortedMap<Integer, String> typeHeaders = new TreeMap<Integer, String>();

        for (Field field : type.getDeclaredFields()) {

            CsvField fieldAnnotation = field.getAnnotation(CsvField.class);
            if (fieldAnnotation == null) {
                continue;
            }

            if (field.getType().getAnnotation(CsvDataType.class) != null) {
                calculateHeaders(field.getType(), allHeaders, fieldAnnotation.pos());
            } else {

                String header = null;

                Header headerAnnotation = field.getAnnotation(Header.class);
                if (headerAnnotation != null) {
                    header = headerAnnotation.value();
                }

                // use field.getName() as default
                if (header == null || header.isEmpty()) {
                    header = field.getName();
                }

                String previousField = typeHeaders.put(fieldAnnotation.pos(), header);
                if (previousField != null) {
                    throw new IllegalArgumentException(String.format("multiple fields for position %d defined! Fields: %s! Type: %s", fieldAnnotation.pos(),
                                                                                                                                      previousField + ", " + field.getName(),
                                                                                                                                      type.getName()));
                }
            }
        }

        allHeaders.put(index, typeHeaders);
    }


    public static List<JSefaCsvMapping> forTypes(Class<?>... types) {
        List<JSefaCsvMapping> mappings = new ArrayList<JSefaCsvMapping>(types.length);

        for (Class<?> type : types) {
            mappings.add(new JSefaCsvMapping(type));
        }

        return mappings;
    }

}
