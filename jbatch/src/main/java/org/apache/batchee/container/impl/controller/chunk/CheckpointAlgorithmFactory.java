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
package org.apache.batchee.container.impl.controller.chunk;


import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.proxy.CheckpointAlgorithmProxy;
import org.apache.batchee.container.proxy.InjectionReferences;
import org.apache.batchee.container.proxy.ProxyFactory;
import org.apache.batchee.jaxb.Chunk;
import org.apache.batchee.jaxb.Step;

public final class CheckpointAlgorithmFactory {
    public static CheckpointAlgorithmProxy getCheckpointAlgorithmProxy(final Step step, final InjectionReferences injectionReferences, final StepContextImpl stepContext, final RuntimeJobExecution jobExecution) {
        final Chunk chunk = step.getChunk();
        final String checkpointType = chunk.getCheckpointPolicy();
        final CheckpointAlgorithmProxy proxy;
        if ("custom".equalsIgnoreCase(checkpointType)) {
            proxy = ProxyFactory.createCheckpointAlgorithmProxy(chunk.getCheckpointAlgorithm().getRef(), injectionReferences, stepContext, jobExecution);
        } else /* "item" */ {
            proxy = new CheckpointAlgorithmProxy(new ItemCheckpointAlgorithm());
        }
        return proxy;

    }

    private CheckpointAlgorithmFactory() {
        // no-op
    }
}
