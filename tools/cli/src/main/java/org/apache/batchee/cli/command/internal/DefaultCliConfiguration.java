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
package org.apache.batchee.cli.command.internal;

import org.apache.batchee.cli.command.api.CliConfiguration;
import org.apache.batchee.cli.command.api.UserCommand;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;

import static java.lang.ClassLoader.getSystemClassLoader;

public class DefaultCliConfiguration implements CliConfiguration {
    @Override
    public String name() {
        return "batchee";
    }

    @Override
    public String description() {
        return "BatchEE CLI";
    }

    @Override
    public boolean addDefaultCommands() {
        return true;
    }

    @Override
    public Iterator<Class<? extends UserCommand>> userCommands() {
        final Collection<Class<? extends UserCommand>> classes = new ArrayList<Class<? extends UserCommand>>();
        try { // read manually cause we dont want to instantiate them there, so no ServiceLoader
            final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
            final ClassLoader loader = tccl != null ? tccl : getSystemClassLoader();
            final Enumeration<URL> uc = loader.getResources("META-INF/services/org.apache.batchee.cli.command.UserCommand");
            while (uc.hasMoreElements()) {
                final URL url = uc.nextElement();
                BufferedReader r = null;
                try {
                    r = new BufferedReader(new InputStreamReader(url.openStream()));
                    String line;
                    while ((line = r.readLine()) != null) {
                        if (line.startsWith("#") || line.trim().isEmpty()) {
                            continue;
                        }
                        classes.add(Class.class.cast(loader.loadClass(line.trim())));
                    }
                } catch (final IOException ioe) {
                    throw new IllegalStateException(ioe);
                } catch (final ClassNotFoundException cnfe) {
                    throw new IllegalArgumentException(cnfe);
                } finally {
                    if (r != null) {
                        r.close();
                    }
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
        return classes.iterator();
    }

    @Override
    public Runnable decorate(final Runnable task) {
        return task;
    }

    @Override
    public Object coerce(String value, Type expected) {
        if (String.class == expected) {
            return value;
        }
        if (long.class == expected) {
            return Long.parseLong(value);
        }
        if (int.class == expected) {
            return Integer.parseInt(value);
        }
        if (boolean.class == expected) {
            return Boolean.parseBoolean(value);
        }
        if (short.class == expected) {
            return Short.parseShort(value);
        }
        if (byte.class == expected) {
            return Byte.parseByte(value);
        }
        if (char.class == expected) {
            return value.charAt(0);
        }
        if (Class.class.isInstance(expected)) {
            try {
                return Class.class.cast(expected).getMethod("fromString", String.class).invoke(null, value);
            } catch (final Exception e) {
                // no-op
            }
        }
        throw new IllegalArgumentException(expected + " not supported as option with value '" + value + "'");
    }
}
