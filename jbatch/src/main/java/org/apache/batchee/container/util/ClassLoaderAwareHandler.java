/**
 * Copyright 2012 International Business Machines Corp.
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.container.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ClassLoaderAwareHandler implements InvocationHandler {
    private static final Class<?>[] RUNNABLE_API = new Class<?>[]{Runnable.class};

    private final ClassLoader loader;
    private final Object delegate;

    public ClassLoaderAwareHandler(final ClassLoader contextClassLoader, final Object instance) {
        loader = contextClassLoader;
        delegate = instance;
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        final ClassLoader current = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            return method.invoke(delegate, args);
        } finally {
            Thread.currentThread().setContextClassLoader(current);
        }
    }

    public static <T> T makeLoaderAware(final Class<T> mainApi, final Class<?>[] clazz, final Object instance) {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        return mainApi.cast(Proxy.newProxyInstance(contextClassLoader, clazz, new ClassLoaderAwareHandler(contextClassLoader, instance)));
    }

    public static Runnable runnableLoaderAware(final Object instance) { // too common to not get a shortcut
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        return Runnable.class.cast(Proxy.newProxyInstance(contextClassLoader, RUNNABLE_API, new ClassLoaderAwareHandler(contextClassLoader, instance)));
    }
}
