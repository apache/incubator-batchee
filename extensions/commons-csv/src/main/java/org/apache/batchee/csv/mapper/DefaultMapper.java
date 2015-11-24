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
package org.apache.batchee.csv.mapper;

import org.apache.batchee.csv.CsvReaderMapper;
import org.apache.batchee.csv.CsvWriterMapper;
import org.apache.commons.csv.CSVRecord;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class DefaultMapper<T> implements CsvReaderMapper<T>, CsvWriterMapper<T> {
    private final Class<T> type;
    private final CoercingConverter coercingConverter;

    private final SortedMap<Integer, Field> fieldByPosition = new TreeMap<Integer, Field>();
    private final SortedMap<String, Field> fieldByName = new TreeMap<String, Field>();
    private final SortedMap<Integer, String> headers = new TreeMap<Integer, String>();
    private final int maxIndex;

    public DefaultMapper(final Class<T> type) {
        this(type, loadConverter());
    }

    protected DefaultMapper(final Class<T> type, final CoercingConverter coercingConverter) {
        this.type = type;
        this.coercingConverter = coercingConverter;

        int higherIdx = -1;

        Class<?> current = type;
        while (current != Object.class) {
            for (final Field field : type.getDeclaredFields()) {
                final Csv csv = field.getAnnotation(Csv.class);
                if (csv != null) {
                    final int pos = csv.index();
                    final String name = csv.name();

                    final boolean defaultName = Csv.DEFAULT_NAME.equals(name);

                    // put each field a single time to avoid to set it twice even if position and name are filled for header output
                    if (pos >= 0) {
                        if (fieldByPosition.put(pos, field) != null) {
                            throw new IllegalArgumentException("multiple field for index " + pos + " in " + type);
                        }
                        if (!defaultName) {
                            headers.put(pos, name);
                        }
                    } else if (!defaultName) {
                        if (fieldByName.put(name, field) != null) {
                            throw new IllegalArgumentException("multiple field for name '" + name + "' in " + type);
                        }
                    }
                    if (pos > higherIdx) {
                        higherIdx = pos;
                    }


                    if (!field.isAccessible()) {
                        field.setAccessible(true);
                    }
                }
            }
            current = current.getSuperclass();
        }

        maxIndex = higherIdx;
    }

    public Iterable<String> getHeaders() {
        return headers.values();
    }

    @Override
    public T fromRecord(final CSVRecord record) {
        try {
            final T instance = type.newInstance();
            for (final Map.Entry<Integer, Field> f : fieldByPosition.entrySet()) {
                final String obj = record.get(f.getKey());
                final Field field = f.getValue();
                setField(instance, obj, field);
            }
            for (final Map.Entry<String, Field> f : fieldByName.entrySet()) {
                final String obj = record.get(f.getKey());
                final Field field = f.getValue();
                setField(instance, obj, field);
            }
            return instance;
        } catch (final InstantiationException e) {
            throw new IllegalStateException(e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Iterable<String> toRecord(final T instance) {
        final Collection<String> record = new LinkedList<String>();

        for (int i = 0; i < maxIndex + 1; i++) {
            final Field f = fieldByPosition.get(i);
            if (f == null) {
                record.add(null);
                continue;
            }

            try {
                final Object val = f.get(instance);
                record.add(val == null ? "" : val.toString());
            } catch (final IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }

        return record;
    }

    private void setField(final T instance, final String obj, final Field field) throws IllegalAccessException {
        if (obj != null) {
            field.set(instance, convert(field.getType(), obj));
        }
    }

    private Object convert(final Class<?> type, final String value) {
        if (String.class == type) {
            return value;
        }

        if (coercingConverter != null) {
            final Object val = coercingConverter.valueFor(type, value);
            if (val != null) {
                return val;
            }
        }

        throw new IllegalArgumentException("Unsupported type " + type);
    }

    private static CoercingConverter loadConverter() {
        try {
            Thread.currentThread().getContextClassLoader().loadClass("org.apache.xbean.propertyeditor.PropertyEditors");
            return XBeanConverter.INSTANCE;
        } catch (ClassNotFoundException e) {
            return Primitives.INSTANCE;
        }
    }
}
