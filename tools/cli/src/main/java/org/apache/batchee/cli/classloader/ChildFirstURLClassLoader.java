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
package org.apache.batchee.cli.classloader;

import java.net.URL;
import java.net.URLClassLoader;

public class ChildFirstURLClassLoader  extends URLClassLoader {
    private final ClassLoader system;

    public ChildFirstURLClassLoader(final URL[] urls, final ClassLoader parent) {
        super(urls, parent);
        system = ClassLoader.getSystemClassLoader();
    }

    @Override
    public Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
        // already loaded?
        Class<?> clazz = findLoadedClass(name);
        if (clazz != null) {
            if (resolve) {
                resolveClass(clazz);
            }
            return clazz;
        }

        // JSE classes
        try {
            clazz = system.loadClass(name);
            if (clazz != null) {
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            }
        } catch (ClassNotFoundException ignored) {
            // no-op
        }

        final boolean ok = shouldSkip(name);
        clazz = loadInternal(name, resolve);
        if (clazz != null) {
            return clazz;
        }

        // finally delegate
        clazz = loadFromParent(name, resolve);
        if (clazz != null) {
            return clazz;
        }

        if (!ok) {
            clazz = loadInternal(name, resolve);
            if (clazz != null) {
                return clazz;
            }
        }

        throw new ClassNotFoundException(name);
    }

    // extension point for next releases but here to don't need to re-implement the login in loadClass
    private boolean shouldSkip(final String name) {
        return false;
    }

    private Class<?> loadFromParent(final String name, final boolean resolve) {
        ClassLoader parent = getParent();
        if (parent == null) {
            parent = system;
        }
        try {
            final Class<?> clazz = Class.forName(name, false, parent);
            if (clazz != null) {
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            }
        } catch (ClassNotFoundException ignored) {
            // no-op
        }
        return null;
    }

    private Class<?> loadInternal(final String name, final boolean resolve) {
        try {
            final Class<?> clazz = findClass(name);
            if (clazz != null) {
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            }
        } catch (final ClassNotFoundException ignored) {
            // no-op
        }
        return null;
    }
}
