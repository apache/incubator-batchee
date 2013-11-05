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
package org.apache.batchee.container.util;

import org.apache.batchee.jaxb.JSLJob;

import java.util.concurrent.BlockingQueue;

public class FlowInSplitBuilderConfig {

    private JSLJob jobModel;
    private BlockingQueue<BatchFlowInSplitWorkUnit> completedQueue;
    private long rootJobExecutionId;

    public FlowInSplitBuilderConfig(JSLJob jobModel,
                                    BlockingQueue<BatchFlowInSplitWorkUnit> completedQueue,
                                    long rootJobExecutionId) {
        super();
        this.jobModel = jobModel;
        this.completedQueue = completedQueue;
        this.rootJobExecutionId = rootJobExecutionId;
    }

    public JSLJob getJobModel() {
        return jobModel;
    }

    public BlockingQueue<BatchFlowInSplitWorkUnit> getCompletedQueue() {
        return completedQueue;
    }

    public long getRootJobExecutionId() {
        return rootJobExecutionId;
    }

    public void setRootJobExecutionId(long rootJobExecutionId) {
        this.rootJobExecutionId = rootJobExecutionId;
    }
}
