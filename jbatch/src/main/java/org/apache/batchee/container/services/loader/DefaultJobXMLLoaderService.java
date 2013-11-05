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
package org.apache.batchee.container.services.loader;

import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.spi.JobXMLLoaderService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DefaultJobXMLLoaderService implements JobXMLLoaderService {
    private static final String PREFIX = "META-INF/batch-jobs/";

    @Override
    public String loadJSL(final String id) {
        final String jobXML = loadJobFromBatchJobs(id);
        if (jobXML == null) {
            throw new BatchContainerServiceException("Could not load job xml with id: " + id);
        }
        return jobXML;

    }


    private static String loadJobFromBatchJobs(final String id) {
        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        final String relativePath = PREFIX + id + ".xml";
        final InputStream stream = tccl.getResourceAsStream(relativePath);
        if (stream == null) {
            throw new BatchContainerRuntimeException(new FileNotFoundException(
                "Cannot find an XML file under " + PREFIX + " with the following name " + id + ".xml"));
        }

        return readJobXML(stream);
    }


    private static String readJobXML(final InputStream stream) {
        final StringBuilder out = new StringBuilder();
        try {
            final byte[] b = new byte[4096];
            for (int i; (i = stream.read(b)) != -1; ) {
                out.append(new String(b, 0, i));
            }
        } catch (final FileNotFoundException e) {
            throw new BatchContainerServiceException(e);
        } catch (final IOException e) {
            throw new BatchContainerServiceException(e);
        }
        return out.toString();
    }


    @Override
    public void init(final Properties batchConfig) throws BatchContainerServiceException {
        // no-op
    }
}
