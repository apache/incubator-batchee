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

import static org.apache.batchee.cdi.impl.LocationHolder.currentJob;

public class JobContextImpl extends BaseContext<JobContextImpl.JobKey> {
    public static final BaseContext<?> INSTANCE = new JobContextImpl();

    private JobContextImpl() {
        // no-op
    }

    @Override
    public Class<? extends Annotation> getScope() {
        return JobScoped.class;
    }

    @Override
    protected JobKey[] currentKeys() {
        return new JobKey[] { new JobKey(currentJob().getExecutionId()) };
    }

    public static class JobKey {
        private final long executionId;

        private final int hashCode;

        public JobKey(final long executionId) {
            this.executionId = executionId;

            hashCode = (int) (executionId ^ (executionId >>> 32));
        }

        @Override
        public boolean equals(final Object o) {
            return this == o
                || (!(o == null || getClass() != o.getClass()) && executionId == JobKey.class.cast(o).executionId);

        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
