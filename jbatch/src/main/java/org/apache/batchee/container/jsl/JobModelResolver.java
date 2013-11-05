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
package org.apache.batchee.container.jsl;

import org.apache.batchee.jaxb.JSLJob;

import javax.batch.operations.BatchRuntimeException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class JobModelResolver {
    private static JAXBContext JOB_CONTEXT;
    static {
        try {
            JOB_CONTEXT = JAXBContext.newInstance(JSLJob.class.getPackage().getName());
        } catch (final JAXBException e) {
            throw new BatchRuntimeException(e);
        }
    }

    private JSLJob unmarshalJobXML(final String jobXML) {
        final JSLJob result;
        final JSLValidationEventHandler handler = new JSLValidationEventHandler();
        try {
            final Unmarshaller u = JOB_CONTEXT.createUnmarshaller();
            u.setSchema(Xsds.jobXML());
            u.setEventHandler(handler);
            result = u.unmarshal(new StreamSource(new StringReader(jobXML)), JSLJob.class).getValue();
        } catch (final JAXBException e) {
            throw new IllegalArgumentException("Exception unmarshalling jobXML", e);
        }
        if (handler.eventOccurred()) {
            throw new IllegalArgumentException("xJCL invalid per schema");
        }
        return result;
    }

    public JSLJob resolveModel(final String jobXML) {
        if (System.getSecurityManager() == null) {
            return unmarshalJobXML(jobXML);
        }
        return AccessController.doPrivileged(
            new PrivilegedAction<JSLJob>() {
                public JSLJob run() {
                    return unmarshalJobXML(jobXML);
                }
            });
    }
}
