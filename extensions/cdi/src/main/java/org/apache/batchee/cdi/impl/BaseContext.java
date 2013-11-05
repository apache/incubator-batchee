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

import javax.enterprise.context.ContextNotActiveException;
import javax.enterprise.context.spi.Context;
import javax.enterprise.context.spi.Contextual;
import javax.enterprise.context.spi.CreationalContext;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class BaseContext<K> implements Context {
    private final ConcurrentMap<K, ConcurrentMap<Contextual<?>, Instance<?>>> storages = new ConcurrentHashMap<K, ConcurrentMap<Contextual<?>, Instance<?>>>();

    /**
     * @return current keys (we inherit contexts here) sorted by order (the last is the most specific)
     */
    protected abstract K[] currentKeys();

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

        for (final K key : currentKeys()) {
            final ConcurrentMap<Contextual<?>, Instance<?>> storage = storages.get(key);
            if (storage != null) {
                final Instance<?> instance = storage.get(component);
                if (instance == null) {
                    return null;
                }
                return (T) instance.value;
            }
        }
        return null;
    }

    @Override
    public boolean isActive() {
        final K[] ks = currentKeys();
        return ks != null && ks.length != 0;
    }

    public void endContext() {
        final ConcurrentMap<Contextual<?>, Instance<?>> storage = storages.remove(lastKey());
        if (storage == null) {
            return;
        }

        for (final Map.Entry<Contextual<?>, Instance<?>> entry : storage.entrySet()) {
            final Instance<?> instance = entry.getValue();
            Contextual.class.cast(entry.getKey()).destroy(instance.value, instance.context);
        }
        storage.clear();
    }

    private K lastKey() {
        final K[] keys = currentKeys();
        if (keys == null || keys.length == 0) {
            return null;
        }
        return keys[keys.length - 1];
    }

    private void checkActive() {
        if (!isActive()) {
            throw new ContextNotActiveException("CDI context with scope annotation @" + getScope().getName() + " is not active with respect to the current thread");
        }
    }

    private ConcurrentMap<Contextual<?>, Instance<?>> getOrCreateCurrentStorage() {
        final K key = lastKey();

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
        private final CreationalContext<T> context;

        private Instance(final T value, final CreationalContext<T> context) {
            this.value = value;
            this.context = context;
        }
    }
}
