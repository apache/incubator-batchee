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

import org.apache.batchee.container.proxy.InjectionReferences;
import org.apache.batchee.container.proxy.ProxyFactory;

import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;

/**
 * Implementation of {@link ContextResolver} which is used to enable BatchEE CDI-Scopes for BatchEE.
 * <p>
 * Uses {@link ProxyFactory#getInjectionReferences()}, which are basically {@link ThreadLocal ThreadLocals} to resolve
 * the current {@link JobContext} and {@link StepContext}
 */
class ThreadLocalContextResolver implements ContextResolver {

    @Override
    public JobContext getJobContext() {

        InjectionReferences references = getInjectionReferences();
        if (references != null) {
            return references.getJobContext();
        }

        return null;
    }


    @Override
    public StepContext getStepContext() {

        InjectionReferences references = getInjectionReferences();
        if (references != null) {
            return references.getStepContext();
        }

        return null;
    }


    private static InjectionReferences getInjectionReferences() {
        return ProxyFactory.getInjectionReferences();
    }
}
