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
package org.apache.batchee.tools.maven;

import org.apache.maven.plugin.logging.Log;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;

// we'd need maven plugin harness 2.2 which is not yet released,
// while we don't depend a lot on maven we can write our own simple utility class for tests
public final class BatchEEMojoTestFactory {
    static <T extends BatchEEMojoBase> String execute(final Class<T> clazz, String... config) {
        try {
            final T mojo = mojo(clazz, config);
            mojo.execute();
            return output(mojo);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    static <T extends BatchEEMojoBase> T mojo(final Class<T> clazz, String... config) {
        try {
            final T t = clazz.newInstance();
            if (config != null) {
                t.properties = new HashMap<String, String>();
                for (int i = 0; i < config.length; i++) {
                    try {
                        setProperty(t, config[i], config[i + 1]);
                    } catch (final Exception e) {
                        t.properties.put(config[i], config[i + 1]);
                    }
                    i++;
                }
                t.setLog(new InMemoryLog());
            }
            return t;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void setProperty(final Object instance, final String field, final String value) {
        Class<?> clazz = instance.getClass();
        do {
            try {
                final Field f = clazz.getDeclaredField(field);
                f.setAccessible(true);
                if (long.class.isInstance(f.getType())) {
                    f.set(instance, Long.parseLong(value));
                } else {
                    f.set(instance, value);
                }
                return;
            } catch (final Exception ignored) {
                // no-op
            }
            clazz = clazz.getSuperclass();
        } while (clazz != null);
        throw new IllegalArgumentException("Field not found");
    }

    public static String output(final BatchEEMojoBase mojo) {
        return InMemoryLog.class.cast(mojo.getLog()).output();
    }

    private BatchEEMojoTestFactory() {
        // no-op
    }

    private static class InMemoryLog implements Log {
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        private final String ln = System.getProperty("line.separator");

        private void save(final CharSequence charSequence) {
            System.out.println(charSequence);
            try {
                baos.write((charSequence + ln).getBytes());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        public String output() {
            return new String(baos.toByteArray());
        }

        @Override
        public boolean isDebugEnabled() {
            return true;
        }

        @Override
        public void debug(final CharSequence charSequence) {
            save(charSequence);
        }

        @Override
        public void debug(final CharSequence charSequence, final Throwable throwable) {
            save(charSequence);
        }

        @Override
        public void debug(final Throwable throwable) {
            save(throwable.getMessage());
        }

        @Override
        public boolean isInfoEnabled() {
            return true;
        }

        @Override
        public void info(final CharSequence charSequence) {
            save(charSequence);
        }

        @Override
        public void info(final CharSequence charSequence, final Throwable throwable) {
            save(charSequence);
        }

        @Override
        public void info(final Throwable throwable) {
            save(throwable.getMessage());
        }

        @Override
        public boolean isWarnEnabled() {
            return true;
        }

        @Override
        public void warn(final CharSequence charSequence) {
            save(charSequence);
        }

        @Override
        public void warn(final CharSequence charSequence, final Throwable throwable) {
            save(charSequence);
        }

        @Override
        public void warn(final Throwable throwable) {
            save(throwable.getMessage());
        }

        @Override
        public boolean isErrorEnabled() {
            return true;
        }

        @Override
        public void error(final CharSequence charSequence) {
            save(charSequence);
        }

        @Override
        public void error(final CharSequence charSequence, final Throwable throwable) {
            save(charSequence);
        }

        @Override
        public void error(final Throwable throwable) {
            save(throwable.getMessage());
        }
    }
}
