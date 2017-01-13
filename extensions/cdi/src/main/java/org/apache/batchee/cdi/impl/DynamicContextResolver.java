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

import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;

/**
 * This implementation of {@link ContextResolver} is needed to enable BatchEE CDI-Scopes
 * for other JBatch implementations then BatchEE.
 * <p>
 * It uses {@link BeanManager} for the current {@link JobContext} and {@link StepContext} lookup.
 * <p>
 * Every time a *Context is needed, it will be resolved via {@link BeanManager}.
 */
class DynamicContextResolver implements ContextResolver {

    private final BeanManager bm;


    DynamicContextResolver(BeanManager bm) {
        this.bm = bm;
    }


    @Override
    public JobContext getJobContext() {
        return resolve(JobContext.class);
    }

    @Override
    public StepContext getStepContext() {
        return resolve(StepContext.class);
    }


    private <T> T resolve(Class<T> contextClass) {
        Bean<?> bean = bm.resolve(bm.getBeans(contextClass));
        return (T) bm.getReference(bean, contextClass, bm.createCreationalContext(bean));

    }
}
