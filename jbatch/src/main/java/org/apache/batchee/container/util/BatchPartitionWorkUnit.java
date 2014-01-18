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

import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.controller.PartitionThreadRootController;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.ServicesManager;

import java.util.concurrent.BlockingQueue;

public class BatchPartitionWorkUnit extends BatchParallelWorkUnit {
    protected final BlockingQueue<BatchPartitionWorkUnit> completedThreadQueue;

    public BatchPartitionWorkUnit(final RuntimeJobExecution jobExecution,
                                  final PartitionsBuilderConfig config,
                                  final ServicesManager manager) {
        super(jobExecution, true, manager);
        this.completedThreadQueue = config.getCompletedQueue();
        this.controller = new PartitionThreadRootController(jobExecution, config, manager);
    }

    @Override
    protected void markThreadCompleted() {
        if (this.completedThreadQueue != null) {
            completedThreadQueue.add(this);
        }
    }

    public void inheritStepContext(final StepContextImpl sc) {
        this.controller.setParentStepContext(sc);
    }
}
