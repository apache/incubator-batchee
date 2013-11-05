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
import org.apache.batchee.jaxb.JSLJob;

public class JobNavigator extends AbstractNavigator<JSLJob> implements ModelNavigator<JSLJob> {
    private JSLJob job = null;

    public JobNavigator(final JSLJob job) {
        this.job = job;
    }

    @Override
    public ExecutionElement getFirstExecutionElement(String restartOn) throws IllegalTransitionException {
        return getFirstExecutionElement(job.getExecutionElements(), restartOn);
    }

    @Override
    public Transition getNextTransition(ExecutionElement currentExecutionElem, ExecutionStatus currentStatus) throws IllegalTransitionException {
        return getNextTransition(currentExecutionElem, job.getExecutionElements(), currentStatus);
    }

    @Override
    public JSLJob getRootModelElement() {
        return job;
    }

    @Override
    public String toString() {
        return "JobNavigator for job id = " + job.getId();
    }
}

	
