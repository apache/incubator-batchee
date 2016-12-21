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

import org.apache.batchee.cdi.scope.JobScoped;

import java.lang.annotation.Annotation;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.inject.Typed;


@Typed
public class JobContextImpl extends BaseContext {

    private ConcurrentMap<Long, AtomicInteger> jobReferences = new ConcurrentHashMap<Long, AtomicInteger>();
    private ThreadLocal<Long> currentJobExecutionId = new ThreadLocal<Long>();

    @Override
    public Class<? extends Annotation> getScope() {
        return JobScoped.class;
    }

    @Override
    protected Long currentKey() {
        return currentJobExecutionId.get();
    }


    public void enterJobExecution(Long jobExecutionId) {
        AtomicInteger jobRefs = jobReferences.get(jobExecutionId);
        if (jobRefs == null) {
            jobRefs = new AtomicInteger(0);
            AtomicInteger oldJobRefs = jobReferences.putIfAbsent(jobExecutionId, jobRefs);
            if (oldJobRefs != null) {
                jobRefs = oldJobRefs;
            }
        }
        jobRefs.incrementAndGet();

        currentJobExecutionId.set(jobExecutionId);
    }

    public void exitJobExecution() {
        Long jobExecutionId = currentJobExecutionId.get();
        AtomicInteger jobRefs = jobReferences.get(jobExecutionId);
        if (jobRefs != null) {
            int references = jobRefs.decrementAndGet();
            if (references == 0) {
                endContext(jobExecutionId);
            }
        }

        currentJobExecutionId.set(null);
        currentJobExecutionId.remove();
    }
}
