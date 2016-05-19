<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Configuration
#### batchee.properties
A batchee.properties file can be created to configure the JBatch container. 
Since it will be looked up in the classloader, the advice is to place it inside the container loader. 
For Maven projects, this means the file can be created inside the resources folder. 

## Configurable Services
To override a service implementation just set the key name (from the list below) to a qualified name. 
For instance to use shiro security service create a batchee.properties with:   

<code>SecurityService = org.apache.batchee.shiro.ShiroSecurityService</code>


* BatchArtifactFactory
* BatchKernelService
* BatchThreadPoolService
* JobStatusManagerService
* JobXMLLoaderService
* PersistenceManagerService
    * Provided Implementations:
        * `org.apache.batchee.container.services.persistence.MemoryPersistenceManagerService` (default)
        * `org.apache.batchee.container.services.persistence.JDBCPersistenceManagerService`
        * `org.apache.batchee.container.services.persistence.JPAPersistenceManagerService`
    * Persistence Properties:
        * persistence.database.has-schema: if the database needs to use a schema or not
        * persistence.database.schema: schema to use for persistence when using JDBC
        * persistence.database.jndi: jndi name of the datasource to use for persistence when using JDBC default implementation
        * persistence.database.driver: jdbc driver to use for persistence when using JDBC default implementation if no jndi name is provided
        * persistence.database.url: jdbc url to use for persistence when using JDBC default implementation if no jndi name is provided
        * persistence.database.user: jdbc user to use for persistence when using JDBC default implementation if no jndi name is provided
        * persistence.database.password: jdbc password to use for persistence when using JDBC default implementation if no jndi name is provided
        * persistence.database.tables.checkpoint: checkpoints table name
        * persistence.database.tables.job-instance: job instances table name
        * persistence.database.tables.job-execution: job executions table name
        * persistence.database.tables.step-execution: step executions table name
        * persistence.database.db-dictionary: the `org.apache.batchee.container.services.persistence.jdbc.database.Database` class to use
        * persistence.database.ddl: `create` to create the database if it doesn't exist
        * persistence.memory.global: storing statically data when using in memory persistence
        * persistence.memory.max-jobs-instances: number of job instance data to store, default to 1000, -1 means keep all in memory
        * persistence.jpa.entity-manager-provider: in case of `org.apache.batchee.container.services.persistence.JPAPersistenceService` the `org.apache.batchee.container.services.persistence.jpa.EntityManagerProvider` qualified name
        * persistence.jpa.transaction-provider: for JPA persistence service the `org.apache.batchee.container.services.persistence.jpa.TransactionProvider` qualified name
        * persistence.jpa.unit-name: for JPA persistence service the unit name (default `batchee`)
        * persistence.jpa.property..*: for JPA persistence service the persistence-unit properties
* SecurityService
* TransactionManagementService
    * transaction.user-transaction.jndi: when using default TransactionManagementService override the default UserTransaction jndi name
    

Note about JPA persistence service: to stay portable entities are not enhanced.
Therefore you need to use a javaagent to do so.

#### Additional Properties
* `org.apache.batchee.jmx`: a boolean activating (by default) or not the JMX facade for the `JobOperator`
* `org.apache.batchee.jmx.application`: a name to distinguish job operator between applications when batchee is not shared (will be shown in JMX name)
* `org.apache.batchee.init.verbose`: boolean activating BatchEE logo print at startup
* `org.apache.batchee.init.verbose.sysout`: use `System.out` to print BatchEE logo instead of JUL
* `org.apache.batchee.step.listeners.before`: global step listener references executed before all others
* `org.apache.batchee.step.listeners.after`: global step listener references executed after all others
* `org.apache.batchee.job.listeners.before`: global job listener references executed before all others
* `org.apache.batchee.job.listeners.after`: global job listener references executed after all others

## Database Support
Batchee uses an embedded Apache Derby database by default.
Other databases can be configured by setting the persistence.database.db-dictionary property,
and switching to the JDBC or JPA PersistenceManagerService implementation.