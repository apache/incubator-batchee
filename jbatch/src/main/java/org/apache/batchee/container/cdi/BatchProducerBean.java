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
package org.apache.batchee.container.cdi;

import org.apache.batchee.container.proxy.ProxyFactory;
import org.apache.batchee.container.util.DependencyInjections;

import javax.batch.api.BatchProperty;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;

public class BatchProducerBean {
    @Produces
    @BatchProperty
    public String produceProperty(final InjectionPoint injectionPoint) {
        if (injectionPoint != null && ProxyFactory.getInjectionReferences() != null) {
            final BatchProperty batchPropAnnotation = injectionPoint.getAnnotated().getAnnotation(BatchProperty.class);
            final String batchPropName;
            if (batchPropAnnotation.name().equals("")) {
                batchPropName = injectionPoint.getMember().getName();
            } else {
                batchPropName = batchPropAnnotation.name();
            }

            return DependencyInjections.getPropertyValue(ProxyFactory.getInjectionReferences().getProps(), batchPropName);
        }
        return null;

    }

    @Produces
    public JobContext getJobContext() {
        if (ProxyFactory.getInjectionReferences() != null) {
            return ProxyFactory.getInjectionReferences().getJobContext();
        }
        return null;
    }

    @Produces
    public StepContext getStepContext() {
        if (ProxyFactory.getInjectionReferences() != null) {
            return ProxyFactory.getInjectionReferences().getStepContext();
        }
        return null;
    }
}
