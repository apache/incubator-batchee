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
package org.apache.batchee.container.navigator;

import org.apache.batchee.container.jsl.ExecutionElement;
import org.apache.batchee.container.jsl.IllegalTransitionException;
import org.apache.batchee.container.jsl.Transition;
import org.apache.batchee.container.status.ExecutionStatus;
import org.apache.batchee.jaxb.Flow;

public class FlowNavigator extends AbstractNavigator<Flow> implements ModelNavigator<Flow> {
    private Flow flow = null;

    public FlowNavigator(final Flow flow) {
        this.flow = flow;
    }

    @Override
    public ExecutionElement getFirstExecutionElement(String restartOn) throws IllegalTransitionException {
        return getFirstExecutionElement(flow.getExecutionElements(), restartOn);
    }

    @Override
    public Transition getNextTransition(ExecutionElement currentExecutionElem, ExecutionStatus currentStatus) throws IllegalTransitionException {
        return getNextTransition(currentExecutionElem, flow.getExecutionElements(), currentStatus);
    }

    @Override
    public Flow getRootModelElement() {
        return flow;
    }

    @Override
    public String toString() {
        return "FlowNavigator for flow id = " + flow.getId();
    }
}