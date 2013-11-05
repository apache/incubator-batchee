/*
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
package org.apache.batchee.container.impl.controller.batchlet;

import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.impl.controller.SingleThreadedStepController;
import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.proxy.BatchletProxy;
import org.apache.batchee.container.proxy.InjectionReferences;
import org.apache.batchee.container.proxy.ProxyFactory;
import org.apache.batchee.container.util.PartitionDataWrapper;
import org.apache.batchee.jaxb.Batchlet;
import org.apache.batchee.jaxb.Property;
import org.apache.batchee.jaxb.Step;

import javax.batch.runtime.BatchStatus;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class BatchletStepController extends SingleThreadedStepController {
    private BatchletProxy batchletProxy;

    public BatchletStepController(final RuntimeJobExecution jobExecutionImpl, final Step step,
                                  final StepContextImpl stepContext, final long rootJobExecutionId,
                                  final BlockingQueue<PartitionDataWrapper> analyzerStatusQueue) {
        super(jobExecutionImpl, step, stepContext, rootJobExecutionId, analyzerStatusQueue);
    }

    private void invokeBatchlet(final Batchlet batchlet) throws BatchContainerServiceException {
        final String batchletId = batchlet.getRef();
        final List<Property> propList = (batchlet.getProperties() == null) ? null : batchlet.getProperties().getPropertyList();
        final InjectionReferences injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, propList);
        batchletProxy = ProxyFactory.createBatchletProxy(batchletId, injectionRef, stepContext, jobExecutionImpl);

        if (!wasStopIssued()) {
            final String processRetVal = batchletProxy.process();
            stepContext.setBatchletProcessRetVal(processRetVal);
        }
    }

    protected synchronized boolean wasStopIssued() {
        // Might only be set to stopping at the job level.  Use the lock for this object on this
        // method along with the stop() method
        if (jobExecutionImpl.getJobContext().getBatchStatus().equals(BatchStatus.STOPPING)) {
            stepContext.setBatchStatus(BatchStatus.STOPPING);
            return true;
        }
        return false;
    }

    @Override
    protected void invokeCoreStep() throws BatchContainerServiceException {
        //TODO If this step is partitioned create partition artifacts
        /*
        Partition partition = step.getPartition();
		if (partition != null) {
			//partition.getConcurrencyElements();
		}
		*/
        try {
            invokeBatchlet(step.getBatchlet());
        } finally {
            invokeCollectorIfPresent();
        }
    }

    @Override
    public synchronized void stop() {
        // It is possible for stop() to be issued before process()
        if (BatchStatus.STARTING.equals(stepContext.getBatchStatus()) ||
            BatchStatus.STARTED.equals(stepContext.getBatchStatus())) {

            stepContext.setBatchStatus(BatchStatus.STOPPING);

            if (batchletProxy != null) {
                batchletProxy.stop();
            }
        }/* else {
			// TODO do we need to throw an error if the batchlet is already stopping/stopped
			// a stop gets issued twice
		}*/
    }


}
