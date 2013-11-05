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

import org.apache.batchee.cdi.scope.StepScoped;

import javax.batch.runtime.context.StepContext;
import java.lang.annotation.Annotation;
import java.util.List;

import static org.apache.batchee.cdi.impl.LocationHolder.currentSteps;

public class StepContextImpl extends BaseContext<StepContextImpl.StepKey> {
    public static final BaseContext<?> INSTANCE = new StepContextImpl();

    private StepContextImpl() {
        // no-op
    }

    @Override
    public Class<? extends Annotation> getScope() {
        return StepScoped.class;
    }

    @Override
    protected StepKey[] currentKeys() {
        final List<StepContext> stepContexts = currentSteps();
        final StepKey[] keys = new StepKey[stepContexts.size()];

        int i = 0;
        for (final StepContext stepContext : stepContexts) {
            keys[i++] = new StepKey(stepContext.getStepExecutionId());
        }
        return keys;
    }

    public static class StepKey {
        private final long stepExecutionId;

        private final int hashCode;

        public StepKey(final long stepExecutionId) {
            this.stepExecutionId = stepExecutionId;

            hashCode = (int) (stepExecutionId ^ (stepExecutionId >>> 32));
        }

        @Override
        public boolean equals(final Object o) {
            return this == o
                || (!(o == null || getClass() != o.getClass()) && stepExecutionId == StepKey.class.cast(o).stepExecutionId);

        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
