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

import org.apache.batchee.cdi.component.JobScopedBean;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.AbstractItemWriter;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@Named
@Dependent
public class PartitionedJobScopedWriter extends AbstractItemWriter {

    private static final Logger LOG = Logger.getLogger(PartitionedJobScopedWriter.class.getName());

    @Inject
    @BatchProperty
    private Integer partition;


    @Override
    public void writeItems(List<Object> items) throws Exception {
        LOG.log(Level.INFO, "Writing {0} items from partition {1}", new Object[]{items.size(), partition});
        LOG.log(Level.INFO, "JobScopedBean destroyed? {0}", JobScopedBean.isDestroyed());
    }
}
