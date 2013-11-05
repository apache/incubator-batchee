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
package org.apache.batchee.container.modelresolver.impl;

import org.apache.batchee.container.jsl.TransitionElement;
import org.apache.batchee.container.modelresolver.PropertyResolverFactory;
import org.apache.batchee.jaxb.Listener;
import org.apache.batchee.jaxb.Step;

import java.util.Properties;


public class StepPropertyResolver extends AbstractPropertyResolver<Step> {

    public StepPropertyResolver(boolean isPartitionStep) {
        super(isPartitionStep);
    }

    @Override
    public Step substituteProperties(final Step step, final Properties submittedProps, final Properties parentProps) {

        // resolve all the properties used in attributes and update the JAXB
        // model
        step.setId(this.replaceAllProperties(step.getId(), submittedProps, parentProps));

        step.setAllowStartIfComplete(this.replaceAllProperties(step.getAllowStartIfComplete(), submittedProps, parentProps));
        step.setNextFromAttribute(this.replaceAllProperties(step.getNextFromAttribute(), submittedProps, parentProps));
        step.setStartLimit(this.replaceAllProperties(step.getStartLimit(), submittedProps, parentProps));

        // Resolve all the properties defined for this step
        Properties currentProps = parentProps;
        if (step.getProperties() != null) {
            currentProps = this.resolveElementProperties(step.getProperties().getPropertyList(), submittedProps, parentProps);
        }

        // Resolve partition
        if (step.getPartition() != null) {
            PropertyResolverFactory.createPartitionPropertyResolver(this.isPartitionedStep).substituteProperties(step.getPartition(), submittedProps, currentProps);
        }

        // Resolve Listener properties, this is list of listeners List<Listener>
        if (step.getListeners() != null) {
            for (final Listener listener : step.getListeners().getListenerList()) {
                PropertyResolverFactory.createListenerPropertyResolver(this.isPartitionedStep).substituteProperties(listener, submittedProps, currentProps);
            }
        }

        if (step.getTransitionElements() != null) {
            for (final TransitionElement controlElement : step.getTransitionElements()) {
                PropertyResolverFactory.createTransitionElementPropertyResolver(this.isPartitionedStep).substituteProperties(controlElement, submittedProps, currentProps);
            }
        }


        // Resolve Batchlet properties
        if (step.getBatchlet() != null) {
            PropertyResolverFactory.createBatchletPropertyResolver(this.isPartitionedStep).substituteProperties(step.getBatchlet(), submittedProps, currentProps);
        }

        // Resolve Chunk properties
        if (step.getChunk() != null) {
            PropertyResolverFactory.createChunkPropertyResolver(this.isPartitionedStep).substituteProperties(step.getChunk(), submittedProps, currentProps);
        }

        return step;
    }

}
