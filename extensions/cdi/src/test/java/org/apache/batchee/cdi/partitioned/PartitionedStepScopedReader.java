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
package org.apache.batchee.cdi.partitioned;

import org.apache.batchee.cdi.component.StepScopedBean;
import org.apache.batchee.cdi.scope.StepScoped;

import javax.batch.api.chunk.AbstractItemReader;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

@Named
@Dependent
public class PartitionedStepScopedReader extends AbstractItemReader {

    @Inject
    private StepScopedBean bean;

    private int count;

    @Override
    public Object readItem() throws Exception {

        if (++count < 11) {
            return "continue - BeanId: " + bean.getId();
        }

        return null;
    }
}
