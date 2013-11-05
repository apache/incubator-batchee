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

import org.apache.batchee.container.jsl.CloneUtility;
import org.apache.batchee.jaxb.Flow;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jaxb.ObjectFactory;
import org.apache.batchee.jaxb.Partition;
import org.apache.batchee.jaxb.PartitionPlan;
import org.apache.batchee.jaxb.Split;
import org.apache.batchee.jaxb.Step;

import javax.batch.runtime.context.JobContext;

public class PartitionedStepBuilder {
    public static final String JOB_ID_SEPARATOR = ":";  // Use something permissible in NCName to allow us to key off of.

    /*
     * Build a generated job with only one flow in it to submit to the
     * BatchKernel. This is used to build subjobs from splits.
     * 
     */
    public static JSLJob buildFlowInSplitSubJob(Long parentJobExecutionId, JobContext jobContext, Split split, Flow flow) {

        ObjectFactory jslFactory = new ObjectFactory();
        JSLJob subJob = jslFactory.createJSLJob();

        // Set the generated subjob id
        String subJobId = generateSubJobId(parentJobExecutionId, split.getId(), flow.getId());
        subJob.setId(subJobId);


        //Copy all properties from parent JobContext to flow threads
        subJob.setProperties(CloneUtility.javaPropsTojslProperties(jobContext.getProperties()));


        //We don't need to do a deep copy here since each flow is already independent of all others, unlike in a partition
        //where one step instance can be executed with different properties on multiple threads.

        subJob.getExecutionElements().add(flow);

        return subJob;
    }

    /*
     * Build a generated job with only one step in it to submit to the
     * BatchKernel. This is used for partitioned steps.
     * 
     */
    public static JSLJob buildPartitionSubJob(Long parentJobInstanceId, JobContext jobContext, Step step, int partitionInstance) {

        ObjectFactory jslFactory = new ObjectFactory();
        JSLJob subJob = jslFactory.createJSLJob();


        // Set the generated subjob id
        String subJobId = generateSubJobId(parentJobInstanceId, step.getId(), partitionInstance);
        subJob.setId(subJobId);


        //Copy all properties from parent JobContext to partitioned step threads
        subJob.setProperties(CloneUtility.javaPropsTojslProperties(jobContext.getProperties()));

        // Add one step to job
        Step newStep = jslFactory.createStep();

        //set id
        newStep.setId(step.getId());


        /***
         * deep copy all fields in a step
         */
        newStep.setAllowStartIfComplete(step.getAllowStartIfComplete());

        if (step.getBatchlet() != null) {
            newStep.setBatchlet(CloneUtility.cloneBatchlet(step.getBatchlet()));
        }

        if (step.getChunk() != null) {
            newStep.setChunk(CloneUtility.cloneChunk(step.getChunk()));
        }

        // Do not copy next attribute and control elements.  Transitioning should ONLY
        // take place on the main thread.

        //Do not add step listeners, only call them on parent thread.

        //Add partition artifacts and set instances to 1 as the base case 
        Partition partition = step.getPartition();
        if (partition != null) {
            if (partition.getCollector() != null) {

                Partition basePartition = jslFactory.createPartition();

                PartitionPlan partitionPlan = jslFactory.createPartitionPlan();
                partitionPlan.setPartitions(null);
                basePartition.setPlan(partitionPlan);

                basePartition.setCollector(partition.getCollector());
                newStep.setPartition(basePartition);

            }
        }

        newStep.setStartLimit(step.getStartLimit());
        newStep.setProperties(CloneUtility.cloneJSLProperties(step.getProperties()));

        // Don't try to only clone based on type (e.g. ChunkListener vs. StepListener).
        // We don't know the type at the model level, and a given artifact could implement more
        // than one listener interface (e.g. ChunkListener AND StepListener).
        newStep.setListeners(CloneUtility.cloneListeners(step.getListeners()));

        //Add Step properties, need to be careful here to remember the right precedence

        subJob.getExecutionElements().add(newStep);


        return subJob;
    }

    /**
     * @param parentJobInstanceId the execution id of the parent job
     * @param splitId             this is the split id where the flows are nested
     * @param flowId              this is the id of the partitioned control element, it can be a
     *                            step id or flow id
     * @return a String of the form
     * <parentJobExecutionId>:<parentId>:<splitId>:<flowId>
     */
    private static String generateSubJobId(Long parentJobInstanceId, String splitId, String flowId) {
        return JOB_ID_SEPARATOR + parentJobInstanceId.toString() + JOB_ID_SEPARATOR + splitId + JOB_ID_SEPARATOR + flowId;
    }

    /**
     * @param parentJobInstanceId the execution id of the parent job
     * @param stepId              this is the id of the partitioned control element, it can be a
     *                            step id or flow id
     * @param partitionInstance   the instance number of the partitioned element
     * @return a String of the form
     * <parentJobExecutionId>:<parentId>:<partitionInstance>
     */
    private static String generateSubJobId(Long parentJobInstanceId, String stepId, int partitionInstance) {
        return JOB_ID_SEPARATOR + parentJobInstanceId.toString() + JOB_ID_SEPARATOR + stepId + JOB_ID_SEPARATOR + partitionInstance;
    }

}
