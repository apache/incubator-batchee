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
package org.apache.batchee.extras.chain;

import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.locator.BeanLocator;

import javax.batch.api.BatchProperty;
import javax.inject.Inject;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;

public abstract class Chain<T> {
    @Inject
    @BatchProperty
    @Documentation("The locator to use to create chain components")
    protected String locator;

    @Inject
    @BatchProperty
    @Documentation("The list of components of the chain")
    protected String chain;

    @Inject
    @BatchProperty
    @Documentation("The separator to use in chain string (default to ',')")
    protected String separator;

    @Inject
    @BatchProperty
    @Documentation("Should release phase of the lifecycle be called (default to false), use true for @Dependent beans.")
    protected String forceRelease;

    private Collection<BeanLocator.LocatorInstance<T>> runtimeChain = new ArrayList<BeanLocator.LocatorInstance<T>>();

    public Object runChain(final Object item) throws Exception {
        if (chain == null) {
            return item;
        }
        if (separator == null) {
            separator = ",";
        }

        if (runtimeChain.isEmpty()) {
            final BeanLocator beanLocator = getBeanLocator();
            final Class<T> type = findType();
            for (final String next : chain.split(separator)) {
                runtimeChain.add(beanLocator.newInstance(type, next));
            }
        }

        Object current = item;
        for (final BeanLocator.LocatorInstance<T> next : runtimeChain) {
            current = invoke(next, current);
        }

        if (Boolean.parseBoolean(forceRelease)) {
            for (final BeanLocator.LocatorInstance<?> next : runtimeChain) {
                next.release();
            }
            runtimeChain.clear();
        }

        return current;
    }

    protected BeanLocator getBeanLocator() {
        return BeanLocator.Finder.get(locator);
    }

    protected Class<T> findType() {
        return Class.class.cast(ParameterizedType.class.cast(getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
    }

    protected abstract Object invoke(BeanLocator.LocatorInstance<T> next, Object current)  throws Exception;
}
