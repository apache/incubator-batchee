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

import org.apache.batchee.jaxb.Batchlet;

import java.util.Properties;


public class BatchletPropertyResolver extends AbstractPropertyResolver<Batchlet> {


    public BatchletPropertyResolver(boolean isPartitionStep) {
        super(isPartitionStep);
    }

    @Override
    public Batchlet substituteProperties(final Batchlet batchlet, final Properties submittedProps, final Properties parentProps) {

        //resolve all the properties used in attributes and update the JAXB model
        batchlet.setRef(replaceAllProperties(batchlet.getRef(), submittedProps, parentProps));

        // Resolve all the properties defined for this batchlet
        if (batchlet.getProperties() != null) {
            resolveElementProperties(batchlet.getProperties().getPropertyList(), submittedProps, parentProps);
        }

        return batchlet;
    }

}
