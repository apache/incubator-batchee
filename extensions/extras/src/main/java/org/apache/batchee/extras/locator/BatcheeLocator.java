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
package org.apache.batchee.extras.locator;

import javax.batch.operations.BatchRuntimeException;
import java.io.Closeable;
import java.lang.reflect.Method;

// just a class reusing BatchArtifactFactory, all is done by reflection and would worj only with batchee implementation
// but that's to not depend on Batchee in this module
public class BatcheeLocator implements BeanLocator {
    public static final BeanLocator INSTANCE = new BatcheeLocator();

    private Method getValue = null;
    private Method getReleasable = null;
    private Method instantiationMethod = null;
    private Object delegate = null;

    private BatcheeLocator() {
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try {
            final Class<?> service = loader.loadClass("org.apache.batchee.spi.BatchArtifactFactory");
            instantiationMethod = service.getMethod("load", String.class);

            final Class<?> returnType = loader.loadClass("org.apache.batchee.spi.BatchArtifactFactory$Instance");
            getValue = returnType.getMethod("getValue");
            getReleasable = returnType.getMethod("getReleasable");

            final Class<?> clazz = loader.loadClass("org.apache.batchee.container.services.factory.CDIBatchArtifactFactory");
            delegate = clazz.newInstance();
        } catch (final Throwable th) {
            try {
                final Class<?> clazz = loader.loadClass("org.apache.batchee.container.services.factory.DefaultBatchArtifactFactory");
                delegate = clazz.newInstance();
            } catch (final Throwable th2) {
                delegate = null;
                instantiationMethod = null;
            }
        }
    }

    @Override
    public <T> BeanLocator.LocatorInstance<T> newInstance(final Class<T> expected, final String batchId) {
        if (delegate == null) {
            return null;
        }
        try {
            final Object result = instantiationMethod.invoke(delegate, batchId);
            return new BeanLocator.LocatorInstance<T>(expected.cast(getValue.invoke(result)), Closeable.class.cast(getReleasable.invoke(result)));
        } catch (final Exception e) {
            throw new BatchRuntimeException(e);
        }
    }
}
