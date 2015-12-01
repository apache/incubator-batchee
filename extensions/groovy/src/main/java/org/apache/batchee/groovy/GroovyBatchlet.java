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
import javax.batch.api.Batchlet;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.inject.Inject;

@Documentation("Reads and executes a batchlet from a groovy script")
public class GroovyBatchlet implements Batchlet {
    @Inject
    @BatchProperty
    @Documentation("The script to execute")
    private String scriptPath;

    @Inject
    private JobContext jobContext;

    @Inject
    private StepContext stepContext;

    private Groovys.GroovyInstance<Batchlet> groovyInstance;
    private Batchlet delegate = null;

    @Override
    public String process() throws Exception {
        synchronized (this) {
            groovyInstance = Groovys.newInstance(Batchlet.class, scriptPath, jobContext, stepContext);
            delegate = groovyInstance.getInstance();
        }

        return delegate.process();
    }

    @Override
    public synchronized void stop() throws Exception {
        if (delegate != null) {
            delegate.stop();
        }
    }
}
