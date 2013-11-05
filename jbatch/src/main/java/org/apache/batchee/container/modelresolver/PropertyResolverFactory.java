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
package org.apache.batchee.container.modelresolver;


import org.apache.batchee.container.jsl.TransitionElement;
import org.apache.batchee.container.modelresolver.impl.AnalyzerPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.BatchletPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.CheckpointAlgorithmPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.ChunkPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.CollectorPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.ControlElementPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.DecisionPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.ExceptionClassesPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.FlowPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.ItemProcessorPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.ItemReaderPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.ItemWriterPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.JobPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.ListenerPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.PartitionMapperPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.PartitionPlanPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.PartitionPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.PartitionReducerPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.SplitPropertyResolver;
import org.apache.batchee.container.modelresolver.impl.StepPropertyResolver;
import org.apache.batchee.jaxb.Analyzer;
import org.apache.batchee.jaxb.Batchlet;
import org.apache.batchee.jaxb.Chunk;
import org.apache.batchee.jaxb.Collector;
import org.apache.batchee.jaxb.Decision;
import org.apache.batchee.jaxb.ExceptionClassFilter;
import org.apache.batchee.jaxb.Flow;
import org.apache.batchee.jaxb.ItemProcessor;
import org.apache.batchee.jaxb.ItemReader;
import org.apache.batchee.jaxb.ItemWriter;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jaxb.Listener;
import org.apache.batchee.jaxb.Partition;
import org.apache.batchee.jaxb.PartitionMapper;
import org.apache.batchee.jaxb.PartitionPlan;
import org.apache.batchee.jaxb.PartitionReducer;
import org.apache.batchee.jaxb.Split;
import org.apache.batchee.jaxb.Step;

public class PropertyResolverFactory {
    public static PropertyResolver<JSLJob> createJobPropertyResolver(boolean isPartitionedStep) {
        return new JobPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<Step> createStepPropertyResolver(boolean isPartitionedStep) {
        return new StepPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<Batchlet> createBatchletPropertyResolver(boolean isPartitionedStep) {
        return new BatchletPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<Split> createSplitPropertyResolver(boolean isPartitionedStep) {
        return new SplitPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<Flow> createFlowPropertyResolver(boolean isPartitionedStep) {
        return new FlowPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<Chunk> createChunkPropertyResolver(boolean isPartitionedStep) {
        return new ChunkPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<TransitionElement> createTransitionElementPropertyResolver(boolean isPartitionedStep) {
        return new ControlElementPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<Decision> createDecisionPropertyResolver(boolean isPartitionedStep) {
        return new DecisionPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<Listener> createListenerPropertyResolver(boolean isPartitionedStep) {
        return new ListenerPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<Partition> createPartitionPropertyResolver(boolean isPartitionedStep) {
        return new PartitionPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<PartitionMapper> createPartitionMapperPropertyResolver(boolean isPartitionedStep) {
        return new PartitionMapperPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<PartitionPlan> createPartitionPlanPropertyResolver(boolean isPartitionedStep) {
        return new PartitionPlanPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<PartitionReducer> createPartitionReducerPropertyResolver(boolean isPartitionedStep) {
        return new PartitionReducerPropertyResolver(isPartitionedStep);
    }

    public static CheckpointAlgorithmPropertyResolver createCheckpointAlgorithmPropertyResolver(boolean isPartitionedStep) {
        return new CheckpointAlgorithmPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<Collector> createCollectorPropertyResolver(boolean isPartitionedStep) {
        return new CollectorPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<Analyzer> createAnalyzerPropertyResolver(boolean isPartitionedStep) {
        return new AnalyzerPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<ItemReader> createReaderPropertyResolver(boolean isPartitionedStep) {
        return new ItemReaderPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<ItemProcessor> createProcessorPropertyResolver(boolean isPartitionedStep) {
        return new ItemProcessorPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<ItemWriter> createWriterPropertyResolver(boolean isPartitionedStep) {
        return new ItemWriterPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<ExceptionClassFilter> createSkippableExceptionClassesPropertyResolver(boolean isPartitionedStep) {
        return new ExceptionClassesPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<ExceptionClassFilter> createRetryableExceptionClassesPropertyResolver(boolean isPartitionedStep) {
        return new ExceptionClassesPropertyResolver(isPartitionedStep);
    }

    public static PropertyResolver<ExceptionClassFilter> createNoRollbackExceptionClassesPropertyResolver(boolean isPartitionedStep) {
        return new ExceptionClassesPropertyResolver(isPartitionedStep);
    }

    private PropertyResolverFactory() {
        // no-op
    }
}
