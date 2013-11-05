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
package org.apache.batchee.container.proxy;

import org.apache.batchee.container.exception.BatchContainerRuntimeException;

import javax.batch.api.partition.PartitionAnalyzer;
import javax.batch.runtime.BatchStatus;
import java.io.Serializable;

public class PartitionAnalyzerProxy extends AbstractProxy<PartitionAnalyzer> implements PartitionAnalyzer {
    PartitionAnalyzerProxy(final PartitionAnalyzer delegate) {
        super(delegate);

    }

    @Override
    public synchronized void analyzeCollectorData(final Serializable data) {
        try {
            this.delegate.analyzeCollectorData(data);
        } catch (final Exception e) {
            this.stepContext.setException(e);
            throw new BatchContainerRuntimeException(e);
        }
    }

    @Override
    public synchronized void analyzeStatus(final BatchStatus batchStatus, final String exitStatus) {
        try {
            this.delegate.analyzeStatus(batchStatus, exitStatus);
        } catch (final Exception e) {
            this.stepContext.setException(e);
            throw new BatchContainerRuntimeException(e);
        }
    }

}
