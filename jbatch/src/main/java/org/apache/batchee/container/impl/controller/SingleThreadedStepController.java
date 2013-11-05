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
package org.apache.batchee.container.impl.controller;

import org.apache.batchee.container.Controller;
import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.proxy.InjectionReferences;
import org.apache.batchee.container.proxy.PartitionCollectorProxy;
import org.apache.batchee.container.proxy.ProxyFactory;
import org.apache.batchee.container.proxy.StepListenerProxy;
import org.apache.batchee.container.util.PartitionDataWrapper;
import org.apache.batchee.container.util.PartitionDataWrapper.PartitionEventType;
import org.apache.batchee.jaxb.Collector;
import org.apache.batchee.jaxb.Property;
import org.apache.batchee.jaxb.Step;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * When a partitioned step is run, this controller will only be used for the partition threads,
 * NOT the top-level main thread that the step executes upon.
 * <p/>
 * When a non-partitioned step is run this controller will be used as well (and there will be no
 * separate main thread with controller).
 */
public abstract class SingleThreadedStepController extends BaseStepController implements Controller {
    // Collector only used from partition threads, not main thread
    protected PartitionCollectorProxy collectorProxy = null;

    protected SingleThreadedStepController(RuntimeJobExecution jobExecutionImpl, Step step, StepContextImpl stepContext, long rootJobExecutionId, BlockingQueue<PartitionDataWrapper> analyzerStatusQueue) {
        super(jobExecutionImpl, step, stepContext, rootJobExecutionId, analyzerStatusQueue);
    }

    List<StepListenerProxy> stepListeners = null;

    protected void setupStepArtifacts() {
        // set up listeners

        InjectionReferences injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, null);
        this.stepListeners = jobExecutionImpl.getListenerFactory().getStepListeners(step, injectionRef, stepContext, jobExecutionImpl);

        // set up collectors if we are running a partitioned step
        if (step.getPartition() != null) {
            Collector collector = step.getPartition().getCollector();
            if (collector != null) {
                List<Property> propList = (collector.getProperties() == null) ? null : collector.getProperties().getPropertyList();
                /**
                 * Inject job flow, split, and step contexts into partition
                 * artifacts like collectors and listeners some of these
                 * contexts may be null
                 */
                injectionRef = new InjectionReferences(jobExecutionImpl.getJobContext(), stepContext, propList);
                this.collectorProxy = ProxyFactory.createPartitionCollectorProxy(collector.getRef(), injectionRef, this.stepContext, jobExecutionImpl);
            }
        }
    }

    @Override
    protected void invokePreStepArtifacts() {
        // Don't call beforeStep() in the partitioned case, since we are now on a partition thread, and
        // have already called beforeStep() on the main thread as the spec says.
        if ((stepListeners != null) && (this.jobExecutionImpl.getPartitionInstance() == null)) {
            for (StepListenerProxy listenerProxy : stepListeners) {
                listenerProxy.beforeStep();
            }
        }
    }

    @Override
    protected void invokePostStepArtifacts() {
        // Don't call beforeStep() in the partitioned case, since we are now on a partition thread, and
        // have already called beforeStep() on the main thread as the spec says.
        if ((stepListeners != null) && (this.jobExecutionImpl.getPartitionInstance() == null)) {
            for (StepListenerProxy listenerProxy : stepListeners) {
                listenerProxy.afterStep();
            }
        }
    }

    protected void invokeCollectorIfPresent() {
        if (collectorProxy != null) {
            final Serializable data = collectorProxy.collectPartitionData();
            sendCollectorDataToAnalyzerIfPresent(data);
        }
    }

    // Useless to have collector without analyzer but let's check so we don't hang or blow up.
    protected void sendCollectorDataToAnalyzerIfPresent(Serializable data) {
        if (analyzerStatusQueue != null) {
            final PartitionDataWrapper dataWrapper = new PartitionDataWrapper();
            dataWrapper.setCollectorData(data);
            dataWrapper.setEventType(PartitionEventType.ANALYZE_COLLECTOR_DATA);
            analyzerStatusQueue.add(dataWrapper);
        }
    }

    // Useless to have collector without analyzer but let's check so we don't hang or blow up.
    @Override
    protected void sendStatusFromPartitionToAnalyzerIfPresent() {
        if (analyzerStatusQueue != null) {
            final PartitionDataWrapper dataWrapper = new PartitionDataWrapper();
            dataWrapper.setBatchStatus(stepStatus.getBatchStatus());
            dataWrapper.setExitStatus(stepStatus.getExitStatus());
            dataWrapper.setEventType(PartitionEventType.ANALYZE_STATUS);
            analyzerStatusQueue.add(dataWrapper);
        }
    }
}
