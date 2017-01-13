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
package org.apache.batchee.cdi.listener;

import org.apache.batchee.cdi.impl.BatchEEScopeExtension;

import javax.batch.api.listener.StepListener;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

/**
 * This Listener is important for cleanup the {@link org.apache.batchee.cdi.impl.StepContextImpl}.
 * Otherwise the {@link org.apache.batchee.cdi.impl.StepContextImpl} will leak.
 */
@Named
@Dependent
public class AfterStepScopeListener implements StepListener {

    private @Inject BatchEEScopeExtension scopeExtension;

    @Override
    public void beforeStep() throws Exception {
        // no-op
    }

    @Override
    public void afterStep() throws Exception {
        scopeExtension.getStepContext().exitStep();
    }
}
