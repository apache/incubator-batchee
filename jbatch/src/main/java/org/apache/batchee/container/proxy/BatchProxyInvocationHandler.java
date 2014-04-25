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
package org.apache.batchee.container.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.apache.batchee.container.exception.BatchContainerRuntimeException;

/**
 * Proxy handler for all our Batch parts like ItemReader,
 * ItemWriter, etc.
 * Most batch part methods store away the exception in the StepContext. A few methods
 * need to be excluded from that rule in order to provide skip/retry logic.
 *
 */
public class BatchProxyInvocationHandler implements InvocationHandler {

    private final InjectionReferences injectionRefs;
    private final Object delegate;
    private final String[] nonExceptionHandlingMethods;

    /**
     *
     * @param delegate the internal delegate which does the real job
     * @param injectionRefs used to store any Exceptions and for CDI support
     * @param nonExceptionHandlingMethods array of methods which should not cause the Exceptions to be stored in the StepContext
     */
    public BatchProxyInvocationHandler(Object delegate, InjectionReferences injectionRefs, String... nonExceptionHandlingMethods) {
        this.delegate = delegate;
        this.injectionRefs = injectionRefs;
        this.nonExceptionHandlingMethods = nonExceptionHandlingMethods;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        InjectionReferences oldInjectionRefs = ProxyFactory.setInjectionReferences(this.injectionRefs);
        try {
            return method.invoke(delegate, args);
        } catch (Throwable e) {
            if (e instanceof InvocationTargetException) {
                e = e.getCause();
            }

            if (nonExceptionHandlingMethods == null || Arrays.binarySearch(nonExceptionHandlingMethods, method.getName()) < 0) {
                if (e instanceof Exception) {
                    if (injectionRefs.getStepContext() != null) {
                        injectionRefs.getStepContext().setException((Exception) e);
                    }
                    throw new BatchContainerRuntimeException(e);
                }
            }

            throw e;
        } finally {
            ProxyFactory.setInjectionReferences(oldInjectionRefs);
        }

    }
}
