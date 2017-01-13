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
package org.apache.batchee.cdi.impl;

import javax.batch.runtime.BatchRuntime;
import javax.enterprise.context.ContextNotActiveException;
import javax.enterprise.context.spi.Context;
import javax.enterprise.context.spi.Contextual;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.BeanManager;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class BaseContext implements Context {

    protected final BeanManager bm;

    /**
     * key == either the stepExecutionId or the jobExecutionId
     */
    private ConcurrentMap<Long, ConcurrentMap<Contextual<?>, Instance<?>>> storages = new ConcurrentHashMap<Long, ConcurrentMap<Contextual<?>, Instance<?>>>();

    private ContextResolver contextResolver;


    public BaseContext(BeanManager bm) {
        this.bm = bm;
    }


    /**
     * @return current keys (we inherit contexts here) sorted by order (the last is the most specific)
     */
    protected abstract Long currentKey();


    @Override
    public <T> T get(final Contextual<T> component, final CreationalContext<T> creationalContext) {
        checkActive();

        final ConcurrentMap<Contextual<?>, Instance<?>> storage = getOrCreateCurrentStorage();
        Instance<T> instance = (Instance<T>) storage.get(component);
        if (instance == null) {
            synchronized (this) {
                instance = (Instance<T>)  storage.get(component);
                if (instance == null) {
                    final T value = component.create(creationalContext);
                    instance = new Instance<T>(value, creationalContext);
                    storage.putIfAbsent(component, instance);
                }
            }
        }

        return instance.value;
    }

    @Override
    public <T> T get(final Contextual<T> component) {
        checkActive();

        final ConcurrentMap<Contextual<?>, Instance<?>> storage = storages.get(currentKey());
        if (storage != null) {
            final Instance<?> instance = storage.get(component);
            if (instance == null) {
                return null;
            }
            return (T) instance.value;
        }
        return null;
    }

    @Override
    public boolean isActive() {
        return currentKey() != null;
    }

    public void endContext(Long key) {
        final ConcurrentMap<Contextual<?>, Instance<?>> storage = storages.remove(key);
        if (storage == null) {
            return;
        }

        for (final Map.Entry<Contextual<?>, Instance<?>> entry : storage.entrySet()) {
            final Instance<?> instance = entry.getValue();
            Contextual.class.cast(entry.getKey()).destroy(instance.value, instance.cc);
        }
        storage.clear();
    }


    protected ContextResolver getContextResolver() {

        // lazy initialisation to ensure BatchRuntime.getJobOperator()
        // and all dependents are there and ready

        if (contextResolver == null) {

            if (BatchRuntime.getJobOperator().getClass().getName().contains("batchee")) {
                contextResolver = new ThreadLocalContextResolver();
            } else {
                contextResolver = new DynamicContextResolver(bm);
            }
        }

        return contextResolver;
    }

    private void checkActive() {
        if (!isActive()) {
            throw new ContextNotActiveException("CDI context with scope annotation @" + getScope().getName() + " is not active with respect to the current thread");
        }
    }

    private ConcurrentMap<Contextual<?>, Instance<?>> getOrCreateCurrentStorage() {
        final Long key = currentKey();

        ConcurrentMap<Contextual<?>, Instance<?>> storage = storages.get(key);
        if (storage == null) {
            storage = new ConcurrentHashMap<Contextual<?>, Instance<?>>();
            final ConcurrentMap<Contextual<?>, Instance<?>> existing = storages.putIfAbsent(key, storage);
            if (existing != null) {
                storage = existing;
            }
        }
        return storage;
    }

    private static class Instance<T> {
        private final T value;
        private final CreationalContext<T> cc;

        private Instance(final T value, final CreationalContext<T> context) {
            this.value = value;
            this.cc = context;
        }
    }
}
