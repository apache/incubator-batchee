/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.groovy;

import org.apache.batchee.doc.api.Documentation;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemReader;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.inject.Inject;
import java.io.Serializable;

@Documentation("Reads and executes a reader from a groovy script")
public class GroovyItemReader implements ItemReader {
    @Inject
    @BatchProperty
    @Documentation("The script to execute")
    private String scriptPath;

    @Inject
    private JobContext jobContext;

    @Inject
    private StepContext stepContext;

    private ItemReader delegate;
    private Groovys.GroovyInstance<ItemReader> groovyInstance;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        groovyInstance = Groovys.newInstance(ItemReader.class, scriptPath, jobContext, stepContext);
        delegate = groovyInstance.getInstance();
        delegate.open(checkpoint);
    }

    @Override
    public void close() throws Exception {
        if (delegate == null) {
            return;
        }
        delegate.close();
        groovyInstance.release();
    }

    @Override
    public Object readItem() throws Exception {
        if (delegate == null) {
            return null;
        }
        return delegate.readItem();
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        if (delegate == null) {
            return null;
        }
        return delegate.checkpointInfo();
    }
}
