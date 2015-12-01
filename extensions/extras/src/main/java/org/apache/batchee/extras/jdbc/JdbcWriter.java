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
package org.apache.batchee.extras.jdbc;

import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.locator.BeanLocator;
import org.apache.batchee.extras.transaction.integration.Synchronizations;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemWriter;
import javax.inject.Inject;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

@Documentation("Store the items in a database.")
public class JdbcWriter extends JdbcConnectionConfiguration implements ItemWriter {
    @Inject
    @BatchProperty
    @Documentation("The locator to lookup the mapper")
    private String locator;

    @Inject
    @BatchProperty(name = "mapper")
    @Documentation("The mapper to convert items to JDBC domain")
    private String mapperStr;

    @Inject
    @BatchProperty
    @Documentation("The update query")
    private String sql;

    private BeanLocator.LocatorInstance<ObjectMapper> mapper;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        mapper = BeanLocator.Finder.get(locator).newInstance(ObjectMapper.class, mapperStr);
    }

    @Override
    public void close() throws Exception {
        if (mapper != null) {
            mapper.release();
        }
    }

    @Override
    public void writeItems(final List<Object> items) throws Exception {
        final Connection c = connection();
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection().prepareStatement(sql);
            for (final Object item : items) {
                mapper.getValue().map(item, preparedStatement);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            if (!Synchronizations.hasTransaction()) {
                c.commit();
            }
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            c.close();
        }
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return null; // datasource can be JtaManaged in a container supporting it
    }
}
