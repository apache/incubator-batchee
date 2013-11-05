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
package org.apache.batchee.container.impl.jobinstance;

import org.apache.batchee.container.status.ExecutionStatus;

import javax.batch.runtime.JobInstance;

public class RuntimeFlowInSplitExecution extends RuntimeJobExecution {
    public RuntimeFlowInSplitExecution(final JobInstance jobInstance, final long executionId) {
        super(jobInstance, executionId);
    }

    private ExecutionStatus flowStatus;

    public ExecutionStatus getFlowStatus() {
        return flowStatus;
    }

    public void setFlowStatus(ExecutionStatus flowStatus) {
        this.flowStatus = flowStatus;
    }
}
