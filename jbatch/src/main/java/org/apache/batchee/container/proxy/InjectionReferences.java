/**
 * Copyright 2013 International Business Machines Corp.
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
package org.apache.batchee.container.proxy;

import org.apache.batchee.container.impl.JobContextImpl;
import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.jaxb.Property;

import java.util.List;


/**
 * This is a container class that holds on to the property and context injections
 * that should be injected into a batch artifact.
 */
public class InjectionReferences {

    private final JobContextImpl jobContext;
    private final StepContextImpl stepContext;

    private List<Property> props;

    public InjectionReferences(final JobContextImpl jobContext, final StepContextImpl stepContext,
                               final List<Property> props) {

        this.jobContext = jobContext;
        this.stepContext = stepContext;
        this.props = props;
    }

    public JobContextImpl getJobContext() {
        return jobContext;
    }

    public StepContextImpl getStepContext() {
        return stepContext;
    }

    public List<Property> getProps() {
        return props;
    }

    public void setProps(final List<Property> props) {
        this.props = props;
    }

}
