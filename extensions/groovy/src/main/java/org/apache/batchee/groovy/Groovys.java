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
package org.apache.batchee.groovy;

import groovy.lang.GroovyClassLoader;

import javax.batch.operations.BatchRuntimeException;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

public final class Groovys {
    public static <T> GroovyInstance<T> newInstance(final Class<T> expected, final String path, final JobContext jobContext, final StepContext stepContext)
            throws IllegalAccessException, InstantiationException {
        if (path == null) {
            throw new BatchRuntimeException("no script configured expected");
        }
        final File script = new File(path);
        if (!script.exists()) {
            throw new BatchRuntimeException("Can't find script: " + path);
        }

        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        final GroovyClassLoader loader = new GroovyClassLoader(tccl);

        final Class<?> clazz;
        try {
            clazz = loader.parseClass(script);
        } catch (final IOException e) {
            throw new BatchRuntimeException(e);
        }

        final T delegate = expected.cast(clazz.newInstance());
        injectIfBatcheeIfAvailable(tccl, delegate, jobContext, stepContext);
        return new GroovyInstance<T>(loader, delegate);
    }

    private static <T> void injectIfBatcheeIfAvailable(final ClassLoader tccl, final T delegate, final JobContext jobContext, final StepContext stepContext) {
        try {
            final Class<?> dependencyInjections = tccl.loadClass("org.apache.batchee.container.util.DependencyInjections");
            final Class<?> injectionReferences = tccl.loadClass("org.apache.batchee.container.proxy.InjectionReferences");
            final Method inject = dependencyInjections.getMethod("injectReferences", Object.class, injectionReferences);
            final Constructor<?> injectionReferencesConstructor = injectionReferences.getConstructors()[0]; // there is a unique constructor
            inject.invoke(null, delegate, injectionReferencesConstructor.newInstance(jobContext, stepContext, null));
        } catch (final Throwable th) {
            // no-op: BatchEE is not available, injections will not be available
        }
    }

    public static class GroovyInstance<T> {
        private final GroovyClassLoader loader;
        private final T instance;

        public GroovyInstance(final GroovyClassLoader loader, final T instance) {
            this.loader = loader;
            this.instance = instance;
        }

        public void release() throws IOException {
            loader.clearCache();
            if (Closeable.class.isInstance(loader)) {
                Closeable.class.cast(loader).close();
            }
        }

        public T getInstance() {
            return instance;
        }
    }

    private Groovys() {
        // no-op
    }
}
