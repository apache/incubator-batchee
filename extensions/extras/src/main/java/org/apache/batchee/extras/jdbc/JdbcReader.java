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

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemReader;
import javax.inject.Inject;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;

@Documentation("Reads data from a SQL query")
public class JdbcReader extends JdbcConnectionConfiguration implements ItemReader {
    @Inject
    @BatchProperty(name = "mapper")
    @Documentation("The ResultSet mapper")
    private String mapperStr;

    @Inject
    @BatchProperty
    @Documentation("The locator to lookup the mapper")
    private String locator;

    @Inject
    @BatchProperty
    @Documentation("The query to execute to find data")
    private String query;

    private LinkedList<Object> items;
    private BeanLocator.LocatorInstance<RecordMapper> mapper;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        mapper = BeanLocator.Finder.get(locator).newInstance(RecordMapper.class, mapperStr);
        items = new LinkedList<Object>();
    }

    @Override
    public void close() throws Exception {
        if (mapper != null) {
            mapper.release();
        }
    }

    @Override
    public Object readItem() throws Exception {
        if (items.isEmpty()) {
            final Connection conn = connection();
            try {
                final PreparedStatement preparedStatement = conn.prepareStatement(query,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);

                ResultSet resultSet = null;
                try {
                    resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        items.add(mapper.getValue().map(resultSet));
                    }
                    if (items.isEmpty()) {
                        return null;
                    }
                } finally {
                    if (resultSet != null) {
                        resultSet.close();
                    }
                    preparedStatement.close();
                }
            } finally {
                conn.close();
            }
        }
        return items.pop();
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return null; // datasource can be JtaManaged in a container supporting it
    }
}
