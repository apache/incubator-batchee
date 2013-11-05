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
import org.apache.batchee.jaxb.End;
import org.apache.batchee.jaxb.Fail;
import org.apache.batchee.jaxb.Next;
import org.apache.batchee.jaxb.Stop;

import java.util.Properties;

public class ControlElementPropertyResolver extends AbstractPropertyResolver<TransitionElement> {

    public ControlElementPropertyResolver(boolean isPartitionStep) {
        super(isPartitionStep);
    }

    @Override
    public TransitionElement substituteProperties(final TransitionElement controlElement, final Properties submittedProps,
                                                  final Properties parentProps) {

        controlElement.setOn(this.replaceAllProperties(controlElement.getOn(), submittedProps, parentProps));
        if (controlElement instanceof End) {
            final End end = (End) controlElement;
            end.setExitStatus(replaceAllProperties(end.getExitStatus(), submittedProps, parentProps));
        } else if (controlElement instanceof Fail) {
            final Fail fail = (Fail) controlElement;
            fail.setExitStatus(replaceAllProperties(fail.getExitStatus(), submittedProps, parentProps));
        } else if (controlElement instanceof Next) {
            final Next next = (Next) controlElement;
            next.setTo(replaceAllProperties(next.getTo(), submittedProps, parentProps));
        } else if (controlElement instanceof Stop) {
            final Stop stop = (Stop) controlElement;
            stop.setExitStatus(replaceAllProperties(stop.getExitStatus(), submittedProps, parentProps));
            stop.setRestart(replaceAllProperties(stop.getRestart(), submittedProps, parentProps));
        }

        return controlElement;
    }

}
