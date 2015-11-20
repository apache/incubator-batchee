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

import java.util.HashMap;
import java.util.Map;

public enum Primitives {
    BooleanType(Boolean.class, boolean.class, new Converter() {
        @Override
        public Object convert(final String o) {
            return Boolean.parseBoolean(o);
        }
    }),
    IntegerType(Integer.class, int.class, new Converter() {
        @Override
        public Object convert(final String o) {
            return Integer.parseInt(o);
        }
    }),
    ShortType(Short.class, short.class, new Converter() {
        @Override
        public Object convert(final String o) {
            return Short.parseShort(o);
        }
    }),
    LongType(Long.class, long.class, new Converter() {
        @Override
        public Object convert(final String o) {
            return Long.parseLong(o);
        }
    }),
    ByteType(Byte.class, byte.class, new Converter() {
        @Override
        public Object convert(final String o) {
            return Byte.parseByte(o);
        }
    }),
    FloatType(Float.class, float.class, new Converter() {
        @Override
        public Object convert(final String o) {
            return Float.parseFloat(o);
        }
    }),
    DoubleType(Double.class, double.class, new Converter() {
        @Override
        public Object convert(final String o) {
            return Double.parseDouble(o);
        }
    }),
    CharacterType(Character.class, char.class, new Converter() {
        @Override
        public Object convert(final String o) {
            return o.length() == 0 ? null : o.charAt(0);
        }
    });

    private static final class Mapping {
        private static final Map<Class<?>, Class<?>> WRAPPERS = new HashMap<Class<?>, Class<?>>();
        private static final Map<Class<?>, Converter> CONVERTERS = new HashMap<Class<?>, Converter>();

        private Mapping() {
            // no-op
        }
    }

    private interface Converter {
        Object convert(String o);
    }

    Primitives(final Class<?> wrapper, final Class<?> primitive, final Converter converter) {
        Mapping.WRAPPERS.put(wrapper, primitive);
        Mapping.CONVERTERS.put(wrapper, converter);
    }

    public static Object valueFor(final Class<?> type, final String value) {
        if (value == null) {
            return null;
        }
        final Class<?> currentType = value.getClass();
        final Class<?> primitive = Mapping.WRAPPERS.get(currentType);
        if (primitive != null && type == primitive) {
            return Mapping.CONVERTERS.get(currentType).convert(value);
        }
        return null;
    }
}
