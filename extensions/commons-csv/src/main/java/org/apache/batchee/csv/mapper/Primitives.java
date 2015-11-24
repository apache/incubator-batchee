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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class Primitives implements CoercingConverter {
    static {
        register(Boolean.class, boolean.class, false, new Converter() {
            @Override
            public Object convert(final String o) {
                return Boolean.parseBoolean(o);
            }
        });
        register(Integer.class, int.class, (int) 0, new Converter() {
            @Override
            public Object convert(final String o) {
                return Integer.parseInt(o);
            }
        });
        register(Short.class, short.class, (short) 0, new Converter() {
            @Override
            public Object convert(final String o) {
                return Short.parseShort(o);
            }
        });
        register(Long.class, long.class, 0L, new Converter() {
            @Override
            public Object convert(final String o) {
                return Long.parseLong(o);
            }
        });
        register(Byte.class, byte.class, (byte) 0, new Converter() {
            @Override
            public Object convert(final String o) {
                return Byte.parseByte(o);
            }
        });
        register(Float.class, float.class, 0.f, new Converter() {
            @Override
            public Object convert(final String o) {
                return Float.parseFloat(o);
            }
        });
        register(Double.class, double.class, 0., new Converter() {
            @Override
            public Object convert(final String o) {
                return Double.parseDouble(o);
            }
        });
        register(Character.class, char.class, (char) 0, new Converter() {
            @Override
            public Object convert(final String o) {
                return o.length() == 0 ? null : o.charAt(0);
            }
        });
    }

    public static final CoercingConverter INSTANCE = new Primitives();

    private static final class Mapping {
        private static final Map<Class<?>, Method> PRIMITIVES = new HashMap<Class<?>, Method>();
        private static final Map<Class<?>, Object> PRIMITIVE_DEFAULTS = new HashMap<Class<?>, Object>();
        private static final Map<Class<?>, Class<?>> WRAPPERS = new HashMap<Class<?>, Class<?>>();
        private static final Map<Class<?>, Converter> CONVERTERS = new HashMap<Class<?>, Converter>();

        private Mapping() {
            // no-op
        }
    }

    private interface Converter {
        Object convert(String o);
    }

    public static Object primitiveDefaultValue(final Class<?> type) {
        return Mapping.PRIMITIVE_DEFAULTS.get(type);
    }

    private static void register(final Class<?> wrapper, final Class<?> primitive, final Object defaultValue, final Converter converter) {
        Mapping.WRAPPERS.put(wrapper, primitive);
        Mapping.CONVERTERS.put(wrapper, converter);
        Mapping.PRIMITIVE_DEFAULTS.put(primitive, defaultValue);
        if (wrapper != Character.class) {
            try {
                Mapping.PRIMITIVES.put(primitive, wrapper.getMethod("valueOf", String.class));
            } catch (final NoSuchMethodException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public Object valueFor(final Class<?> type, final String value) {
        final Method valueOf = Mapping.PRIMITIVES.get(type);
        if (valueOf != null) {
            if (value == null || value.trim().isEmpty()) {
                return Mapping.PRIMITIVE_DEFAULTS.get(type);
            }

            try {
                return valueOf.invoke(null, value);
            } catch (final IllegalAccessException e) {
                throw new IllegalArgumentException(e);
            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException(e.getCause());
            }
        } else if (char.class == type) {
            if (value == null || value.trim().isEmpty()) {
                return Mapping.PRIMITIVE_DEFAULTS.get(type);
            }
            return value.charAt(0);
        }

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
