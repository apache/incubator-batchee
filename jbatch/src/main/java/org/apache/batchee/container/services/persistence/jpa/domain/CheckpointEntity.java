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
package org.apache.batchee.container.services.persistence.jpa.domain;

import org.apache.batchee.container.impl.controller.chunk.CheckpointType;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

@Entity
@NamedQueries({
    @NamedQuery(name = CheckpointEntity.Queries.FIND,
                query = "select c from CheckpointEntity c where c.instance.jobInstanceId = :jobInstanceId and c.stepName = :stepName and c.type = :type"),
    @NamedQuery(name = CheckpointEntity.Queries.DELETE_BY_INSTANCE_ID, query = "delete from CheckpointEntity e where e.instance.jobInstanceId = :id"),
    @NamedQuery(
        name = CheckpointEntity.Queries.DELETE_BY_DATE,
        query = "delete from CheckpointEntity e where (select max(x.endTime) from JobExecutionEntity x where x.instance.jobInstanceId = e.instance.jobInstanceId) < :date")
})
@Table(name=CheckpointEntity.TABLE_NAME)
public class CheckpointEntity {
    public static interface Queries {
        String FIND = "org.apache.batchee.container.services.persistence.jpa.domain.CheckpointEntity.find";
        String DELETE_BY_INSTANCE_ID = "org.apache.batchee.container.services.persistence.jpa.domain.CheckpointEntity.deleteByInstanceId";
        String DELETE_BY_DATE = "org.apache.batchee.container.services.persistence.jpa.domain.CheckpointEntity.deleteBydate";
    }

    public static final String TABLE_NAME = "BATCH_CHECKPOINT";

    @Id
    @GeneratedValue
    private String id;

    private String stepName;

    @Enumerated(EnumType.STRING)
    private CheckpointType type;

    @Lob
    private byte[] data;

    @ManyToOne
    private JobInstanceEntity instance;

    public String getId() {
        return id;
    }

    public String getStepName() {
        return stepName;
    }

    public void setStepName(final String stepName) {
        this.stepName = stepName;
    }

    public CheckpointType getType() {
        return type;
    }

    public void setType(final CheckpointType type) {
        this.type = type;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(final byte[] data) {
        this.data = data;
    }

    public JobInstanceEntity getInstance() {
        return instance;
    }

    public void setInstance(final JobInstanceEntity instance) {
        this.instance = instance;
    }
}
