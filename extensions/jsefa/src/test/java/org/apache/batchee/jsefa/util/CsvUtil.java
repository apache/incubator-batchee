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
package org.apache.batchee.jsefa.util;

import net.sf.jsefa.csv.annotation.CsvDataType;
import net.sf.jsefa.csv.annotation.CsvField;
import org.testng.Assert;

import java.lang.reflect.Field;
import java.util.SortedMap;
import java.util.TreeMap;

public final class CsvUtil {

    private CsvUtil() {
        // no instantiation
    }


    /**
     * Converts the given Object annotated with {@link CsvDataType} to a csv-{@link String}
     *
     * @param object to convert
     *
     * @return the csv-{@link String}
     */
    public static String toCsv(Object object) {
        return toCsv(object, false);
    }

    /**
     * Converts the given Object annotated with {@link CsvDataType} to a csv-{@link String}
     *
     * @param object to convert
     * @param ignorePrefix if {@code true} the {@link CsvDataType#defaultPrefix()} will be ignored for conversion
     *
     * @return the csv-{@link String}
     */
    public static String toCsv(Object object, boolean ignorePrefix) {
        StringBuilder csvBuilder = new StringBuilder(30);

        Class<?> clazz = object.getClass();
        CsvDataType dataType = clazz.getAnnotation(CsvDataType.class);

        // dataType can never be null, otherwise it can't be serialized with jsefa
        if (!ignorePrefix && !"".equals(dataType.defaultPrefix())) {
            csvBuilder.append(dataType.defaultPrefix());
        }

        for (String value : getValues(object, object.getClass(), new TreeMap<Integer, String>()).values()) {
            if (csvBuilder.length() > 0) {
                csvBuilder.append(";");
            }

            csvBuilder.append(value);
        }

        return csvBuilder.toString();

    }


    private static SortedMap<Integer, String> getValues(Object object, Class<?> clazz, SortedMap<Integer, String> values) {

        if (clazz.getSuperclass() != Object.class) {
            values = getValues(object, clazz.getSuperclass(), values);
        }

        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            CsvField annotation = field.getAnnotation(CsvField.class);
            if (annotation == null) {
                continue;
            }

            try {
                field.setAccessible(true);
                Object value = field.get(object);

                if (value != null) {

                    String valuetoPut;
                    if (value.getClass().getAnnotation(CsvDataType.class) != null) {
                        valuetoPut = toCsv(value, true);
                    }
                    else {
                        valuetoPut = value.toString();
                    }

                    values.put(annotation.pos(), valuetoPut);
                }
            }
            catch (IllegalAccessException e) {
                Assert.fail("Cannot read from field " + field.getName(), e);
            }
        }

        return values;
    }

}
