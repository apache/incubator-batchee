[![Build Status](https://secure.travis-ci.org/rmannibucau/batchee.png)](http://travis-ci.org/rmannibucau/batchee)

## Modules
### jbatch (aka batchee-jbatch)
#### Dependency

```xml
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-jbatch</artifactId>
  <version>${batchee.version}</version>
</dependency>
```

#### Goal

Implements JBatch (aka JSR 352).

### Shiro
#### Dependency

```xml
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-shiro</artifactId>
  <version>${batchee.version}</version>
</dependency>
```

#### Goal

A simple integration with Apache Shiro to check permissions when running a batch.


### Hazelcast
#### Dependency

```xml
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-hazelcast</artifactId>
  <version>${batchee.version}</version>
</dependency>
```

#### Goal

A module based on Hazelcast API to allow distributed locks.


### GUI/Web module
#### Dependency

```xml
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-servlet</artifactId>
  <version>${batchee.version}</version>
</dependency>
```

```xml
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-jaxrs-client</artifactId>
  <version>${batchee.version}</version>
</dependency>
```

```xml
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-jaxrs-server</artifactId>
  <version>${batchee.version}</version>
</dependency>
```

#### Goal

A simple web front to visualize JBatch information and expose as JAX-RS resource JBatch `JobOperator`.

### Extras
#### Dependency

```xml
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-extras</artifactId>
  <version>${batchee.version}</version>
</dependency>
```

#### Goal

Basic implementations for Readers/Writers/Processors/.... More on it in extensions part.

### BeanIO
#### Dependency

```xml
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-beanio</artifactId>
  <version>${batchee.version}</version>
</dependency>
```

#### Goal

Basic implementations of a reader and a writer using BeanIO library. Details in extensions part.

### Groovy
#### Dependency

```xml
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-groovy</artifactId>
  <version>${batchee.version}</version>
</dependency>
```

#### Goal

Basic implementations of a reader/processor/writer/batchlet delegating to a groovy script the processing. It allows
to add some dynamicity to batches.


### Camel
#### Dependency

```xml
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-camel</artifactId>
  <version>${batchee.version}</version>
</dependency>
```

#### Goal

A simple integration with Apache Camel.


### CDI
#### Dependency

```xml
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-cdi</artifactId>
  <version>${batchee.version}</version>
</dependency>
```

#### Goal

Provides basic batch oriented scopes (`@JobScoped` and `@StepScoped`).

# Configuration
## batchee.properties

`batchee.properties` can configure the JBatch container. It will look up in the classloader (advise: put it in container loader).

Here are the available configurable services:

* TransactionManagementService
    * transaction.user-transaction.jndi: when using default TransactionManagementService override the default UserTransaction jndi name
* PersistenceManagerService (service) - here are referenced default implementations configuration. They are exclusive.
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
* JobStatusManagerService
* BatchThreadPoolService
* BatchKernelService
* JobXMLLoaderService
* BatchArtifactFactory
* SecurityService

Note about JPA persistence service: to stay portable entities are not enhanced. Therefore you need to use a javaagent to do so.

To override a service implementation just set the key name (from the previous list) to a qualified name.
For instance to use shiro security service create a batchee.properties with:

    SecurityService = org.apache.batchee.shiro.ShiroSecurityService

Some more configuration is available in batchee.properties:

* `org.apache.batchee.jmx`: a boolean activating (by default) or not the JMX facade for the `JobOperator`
* `org.apache.batchee.jmx.application`: a name to distinguish job operator between applications when batchee is not shared (will be shown in JMX name)
* `org.apache.batchee.init.verbose`: boolean activating BatchEE logo print at startup
* `org.apache.batchee.init.verbose.sysout`: use `System.out` to print BatchEE logo instead of JUL
* `org.apache.batchee.step.listeners.before`: global step listener references executed before all others
* `org.apache.batchee.step.listeners.after`: global step listener references executed after all others
* `org.apache.batchee.job.listeners.before`: global job listener references executed before all others
* `org.apache.batchee.job.listeners.after`: global job listener references executed after all others

# Extensions
## Extras
### `org.apache.batchee.extras.locator.BeanLocator`

Each time an implementation/reference needs to be resolved this API is used. The default one respects the same
rules as the implementation used to resolve ref attributes of the batch xml file (it means you can use qualified names,
CDI names if you are in a CDI container...).

### A word on extensions

Readers, writers, processors have always a shortname which will only work with batchee implementation.
To use it with other JBatch implementation use the full qualified name.

### `org.apache.batchee.extras.chain.ChainProcessor`

Allow to set multiple `javax.batch.api.chunk.ItemProcessor` through a single processor. The n+1 processor processes the
returned value of the n processor.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="..." />
    <processor ref="org.apache.batchee.extras.chain.ChainProcessor">
      <properties>
        <property name="chain" value="ref1,ref2,ref3"/>
      </properties>
    </processor>
    <writer ref="..." />
</chunk>
</step>
```

Note: `org.apache.batchee.extras.chain.ChainBatchlet` does the same for `javax.batch.api.Batchlet`.

Shortname: `chainProcessor`

### `org.apache.batchee.extras.flat.FlatFileItemReader`

A reader reading line by line a file. By default the line is returned as a `java.lang.String`. To return another object
just override `protected Object preReturn(String line, long lineNumber)` method:

```java
public class MyFlatReader extends FlatFileItemReader {
    @Override
    protected Object preReturn(String line, long lineNumber) {
        return new Person(line);
    }
}
```

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="org.apache.batchee.extras.flat.FlatFileItemReader">
      <properties>
        <property name="input" value="#{jobParameters['input']}" />
      </properties>
    </reader>
    <processor ref="..." />
    <writer ref="..." />
  </chunk>
</step>
```

Configuration:

* comments: a comma separated list of prefixes marking comment lines
* input: the input file path
* locator: the `BeanLocator` used to find the lineMapper
* lineMapper: an implementation of `org.apache.batchee.extras.flat.LineMapper`. If noone is provided the read object will be the line (as a `String`)

Shortname: `flatReader`

### `org.apache.batchee.extras.flat.FlatFileItemWriter`

A writer writing an item by line. By default `toString()` is used on items, to change it
just override `protected String preWrite(Object object)` method:

```java
public class MyFlatReader extends FlatFileItemReader {
    @Override
    protected String preWrite(final Object object) {
        final Person person = (Person) object;
        return person.getName() + "," + person.getAge();
    }
}
```

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="..."/>
    <processor ref="..." />
    <writer ref="org.apache.batchee.extras.flat.FlatFileItemWriter">
      <properties>
        <property name="output" value="#{jobParameters['output']}"/>
      </properties>
    </writer>
  </chunk>
</step>
```

Configuration:

* encoding: the output file encoding
* output: the output file path
* line.separator: the separator to use, the "line.separator" system property by default

Shortname: `flatWriter`

### `org.apache.batchee.extras.jdbc.JdbcBatchlet`

A simple `Batchlet` to execute sql.

Sample:

```xml
<step id="step1">
  <batchlet ref="jdbcBatchlet">
    <properties>
      <property name="sql" value="delete from Person p where p.name = 'forbidden'" />

      <!-- connection info -->
      <property name="driver" value="org.apache.derby.jdbc.EmbeddedDriver" />
      <property name="url" value="jdbc:derby:memory:jdbcbatchlet;create=true" />
      <property name="user" value="app" />
      <property name="password" value="app" />
    </properties>
  </batchlet>
</step>
```

Configuration:

* jndi: jndi name of the datasource to use
* driver: jdbc driver to use if no jndi name was provided
* url: jdbc url to use if no jndi name was provided
* user: jdbc user to use if no jndi name was provided
* password: jdbc password to use if no jndi name was provided
* sql: the sql to execute

Shortname: `jdbcBatchlet`

### `org.apache.batchee.extras.jdbc.JdbcReader`

This reader execute a query while the query returns items.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="org.apache.batchee.extras.jdbc.JdbcReader">
      <properties>
        <property name="mapper" value="org.apache.batchee.extras.JdbcReaderTest$SimpleMapper" />
        <property name="query" value="select * from FOO where name like 't%'" />
        <property name="driver" value="org.apache.derby.jdbc.EmbeddedDriver" />
        <property name="url" value="jdbc:derby:memory:jdbcreader;create=true" />
        <property name="user" value="app" />
        <property name="password" value="app" />
      </properties>
    </reader>
    <processor ref="..." />
    <writer ref="..." />
  </chunk>
</step>
```

Configuration:

* jndi: jndi name of the datasource to use
* driver: jdbc driver to use if no jndi name was provided
* url: jdbc url to use if no jndi name was provided
* user: jdbc user to use if no jndi name was provided
* password: jdbc password to use if no jndi name was provided
* mapper: the implementation of `org.apache.batchee.extras.jdbc.RecordMapper` to use to convert `java.sql.ResultSet` to objects
* locator: the `org.apache.batchee.extras.locator.BeanLocator` to use to create the mapper
* query: the query used to find items

Here is a sample record mapper deleting items once read (Note: you probably don't want to do so or at least not without a managed datasource):

```java
public class SimplePersonMapper implements RecordMapper {
    @Override
    public Object map(final ResultSet resultSet) throws SQLException {
        final String name = resultSet.getString("name"); // extract some fields to create an object
        resultSet.deleteRow();
        return new Person(name);
    }
}
```

Shortname: `jdbcReader`

### `org.apache.batchee.extras.jdbc.JdbcWriter`

A writer storing items in a database.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="..."/>
    <processor ref="..." />
    <writer ref="org.apache.batchee.extras.jdbc.JdbcWriter">
      <properties>
        <property name="mapper" value="org.apache.batchee.extras.JdbcWriterTest$SimpleMapper" />
        <property name="sql" value="insert into FOO (name) values(?)" />
        <property name="driver" value="org.apache.derby.jdbc.EmbeddedDriver" />
        <property name="url" value="jdbc:derby:memory:jdbcwriter;create=true" />
        <property name="user" value="app" />
        <property name="password" value="app" />
      </properties>
    </writer>
  </chunk>
</step>
```

Configuration:

* jndi: jndi name of the datasource to use
* driver: jdbc driver to use if no jndi name was provided
* url: jdbc url to use if no jndi name was provided
* user: jdbc user to use if no jndi name was provided
* password: jdbc password to use if no jndi name was provided
* mapper: the implementation of `org.apache.batchee.extras.jdbc.ObjectMapper` to use to convert objects to JDBC through `java.sql.PreparedStatement`
* locator: the `org.apache.batchee.extras.locator.BeanLocator` to use to create the mapper
* sql: the sql used to insert records

Here is a sample object mapper:

```java
public class SimpleMapper implements ObjectMapper {
    @Override
    public void map(final Object item, final PreparedStatement statement) throws SQLException {
        statement.setString(1, item.toString()); // 1 because our insert statement uses values(?)
    }
}
```

Shortname: `jdbcWriter`

### `org.apache.batchee.extras.jpa.JpaItemReader`

Reads items from a JPA query.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="org.apache.batchee.extras.jpa.JpaItemReader">
      <properties>
        <property name="entityManagerProvider" value="org.apache.batchee.extras.util.MyProvider" />
        <property name="query" value="select e from Person e" />
      </properties>
    </reader>
    <processor ref="..." />
    <writer ref="..." />
  </chunk>
</step>
```

Configuration:

* entityManagerProvider: the `org.apache.batchee.extras.jpa.EntityManagerProvider` (`BeanLocator` semantic), note you can set a jndi name if the value starts with jndi (for instance jndi:java:comp/env/em).
* parameterProvider: the `org.apache.batchee.extras.jpa.ParameterProvider` (`BeanLocator` semantic)
* locator: the `org.apache.batchee.extras.locator.BeanLocator` used to create `org.apache.batchee.extras.jpa.EntityManagerProvider` and `org.apache.batchee.extras.jpa.ParameterProvider`
* namedQuery: the named query to use
* query: the JPQL query to use if no named query was provided
* pageSize: the paging size
* detachEntities: a boolean to ask the reader to detach entities
* jpaTransaction: should em.getTransaction() be used or not

Shortname: `jpaReader`

### `org.apache.batchee.extras.jpa.JpaItemWriter`

Write items through JPA API.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="..." />
    <processor ref="..." />
    <writer ref="org.apache.batchee.extras.jpa.JpaItemWriter">
      <properties>
        <property name="entityManagerProvider" value="org.apache.batchee.extras.util.MyProvider" />
        <property name="jpaTransaction" value="true" />
      </properties>
    </writer>
  </chunk>
</step>
```

Configuration:

* entityManagerProvider: the `org.apache.batchee.extras.jpa.EntityManagerProvider` (`BeanLocator` semantic), note you can set a jndi name if the value starts with jndi (for instance jndi:java:comp/env/em).
* locator: the `org.apache.batchee.extras.locator.BeanLocator` used to create `org.apache.batchee.extras.jpa.EntityManagerProvider`
* useMerge: a boolean to force using merge instead of persist
* jpaTransaction: should em.getTransaction() be used or not

Shortname: `jpaWriter`

### `org.apache.batchee.extras.noop.NoopItemWriter`

A writer doing nothing (in <chunk/> a writer is mandatory so it can mock one if you don't need one).

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="..." />
    <processor ref="..." />
    <writer ref="org.apache.batchee.extras.noop.NoopItemWriter" />
  </chunk>
</step>
```

Shortname: `noopWriter`

### `org.apache.batchee.extras.typed.Typed[Reader|Processor|Writer]`

Just abstract class allowing to use typed items instead of `Object` from the JBatch API.

### `org.apache.batchee.extras.stax.StaxItemReader`

A reader using StAX API to read a XML file.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="org.apache.batchee.extras.stax.StaxItemReader">
      <properties>
        <property name="input" value="#{jobParameters['input']}"/>
        <property name="marshallingClasses" value="org.apache.batchee.extras.StaxItemReaderTest$Bar"/>
        <property name="tag" value="bar"/>
      </properties>
    </reader>
    <processor ref="..." />
    <writer ref="..." />
  </chunk>
</step>
```

Configuration:

* input: the input file
* tag: the tag marking an object to unmarshall
* marshallingClasses: the comma separated list of JAXB classes to use to create the JAXBContext
* marshallingPackage: if no marshallingClasses are provided this package is used to create the JAXBContext

Shortname: `staxReader`

### `org.apache.batchee.extras.stax.StaxItemWriter`

A writer using StAX API to write a XML file.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="..." />
    <processor ref="..." />
    <writer ref="org.apache.batchee.extras.stax.StaxItemWriter">
      <properties>
        <property name="output" value="#{jobParameters['output']}"/>
        <property name="marshallingClasses" value="org.apache.batchee.extras.StaxItemWriterTest$Foo"/>
      </properties>
    </writer>
  </chunk>
</step>
```

Configuration:

* output: the output file
* encoding: the output file encoding (default UTF-8)
* version: the output file version (XML, default 1.0)
* rootTag: the output rootTag (default "root")
* marshallingClasses: the comma separated list of JAXB classes to use to create the JAXBContext
* marshallingPackage: if no marshallingClasses are provided this package is used to create the JAXBContext

Shortname: `staxWriter`

### `org.apache.batchee.beanio.BeanIOReader`

A reader using BeanIO.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="org.apache.batchee.beanio.BeanIOReader">
      <properties>
        <property name="file" value="#{jobParameters['input']}"/>
        <property name="streamName" value="readerCSV"/>
        <property name="configuration" value="beanio.xml"/>
      </properties>
    </reader>
    <processor ref="..." />
    <writer ref="..." />
  </chunk>
</step>
```

Here is the associated beanio.xml:

```xml
<beanio xmlns="http://www.beanio.org/2012/03"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://www.beanio.org/2012/03 http://www.beanio.org/2012/03/mapping.xsd">
  <stream name="readerCSV" format="csv">
    <record name="record1" class="org.apache.batchee.beanio.bean.Record">
      <field name="field1"/>
      <field name="field2"/>
    </record>
  </stream>
</beanio>
```

Configuration:

* file: the input file
* streamName: the stream name (from beanio xml file)
* configuration: the beanio xml configuration file
* locale: the locale to use
* errorHandler: the BeanIO error handler to use

Shortname: `beanIOReader`

### `org.apache.batchee.beanio.BeanIOWriter`

A writer using BeanIO.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="..." />
    <processor ref="..." />
    <writer ref="org.apache.batchee.beanio.BeanIOWriter">
      <properties>
        <property name="file" value="#{jobParameters['output']}"/>
        <property name="streamName" value="writerCSV"/>
        <property name="configuration" value="beanio.xml"/>
      </properties>
    </writer>
  </chunk>
</step>
```

Configuration:

* file: the output file
* streamName: the stream name (from beanio xml file)
* configuration: the beanio xml configuration file (from the classloader)
* encoding: the output file encoding
* templateLocator: the `org.apache.batchee.camel.CamelTemplateLocator` to find the `org.apache.camel.ProducerTemplate` to use

Shortname: `beanIOWriter`

### `org.apache.batchee.camel.CamelItemProcessor`

A processor reusing Camel logic.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="..." />
    <processor ref="org.apache.batchee.camel.CamelItemProcessor">
      <properties>
        <property name="endpoint" value="direct:processor"/>
      </properties>
    </processor>
    <writer ref="..." />
  </chunk>
</step>
```

Configuration:

* endpoint: the endpoint to use
* templateLocator: the `org.apache.batchee.camel.CamelTemplateLocator` to find the `org.apache.camel.ProducerTemplate` to use

Shortname: `camelProcessor`

### `org.apache.batchee.camel.CamelChainItemProcessor`

Same as previous one but with a chain

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="..." />
    <processor ref="org.apache.batchee.camel.CamelChainItemProcessor">
      <properties>
        <property name="chain" value="test:foo?value=first,test:bar?value=second"/>
      </properties>
    </processor>
    <writer ref="..." />
  </chunk>
</step>
```

Configuration: mainly the chain configuration excepted "chain" value is a list of endpoints.

Shortname: `camelChainProcessor`

### `org.apache.batchee.camel.CamelItemReader`

A reader using camel consumers.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="org.apache.batchee.camel.CamelItemReader">
      <properties>
        <property name="endpoint" value="direct:reader"/>
      </properties>
    </reader>
    <processor ref="..." />
    <writer ref="..." />
  </chunk>
</step>
```

Configuration:

* endpoint: the input camel endpoint URI
* templateLocator: the `org.apache.batchee.camel.CamelTemplateLocator` to find the `org.apache.camel.ConsumerTemplate` to use

Shortname: `camelReader`

### `org.apache.batchee.camel.CamelItemWriter`

A writer using camel producer.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="..." />
    <processor ref="..." />
    <writer ref="org.apache.batchee.camel.CamelItemWriter">
      <properties>
        <property name="endpoint" value="direct:writer"/>
      </properties>
    </writer>
  </chunk>
</step>
```

Configuration:

* endpoint: the input camel endpoint URI
* templateLocator: the `org.apache.batchee.camel.CamelTemplateLocator` to find the `org.apache.camel.ProducerTemplate` to use

Shortname: `camelWriter`

### Camel component

batchee-camel includes a Camel component. Here is its format:

    jbatch:name[?synchronous=xxx]

with name the batch name. By default it is not intended to be synchronous but it can be forced (by polling) using synchronous attribute.
Synchronous attribute is the polling period and needs to be > 0 to be active.


After this endpoint (even in asynchrnous mode) the exchange will get the headers:

* JBatchOperator: the operator used to launch this job (normally not useful but some implmentations can depend on it)
* JBatchExecutionId: the job execution id

Note: if you set JBatchExecutionId in the headers before this endpoint you can use ?restart=true or ?stop=true or ?abandon=true
to restart/stop/abandon the job instead of starting it.

### `org.apache.batchee.groovy.GroovyItemReader`

A reader delegating to a groovy script.

Sample:

```xml
<step id="step1">
  <chunk>
   <reader ref="groovyReader">
     <properties>
       <property name="scriptPath" value="target/work/reader.groovy"/>
     </properties>
   </reader>
   <processor ref="..." />
   <writer ref="..." />
  </chunk>
</step>
```

Configuration:

* scriptPath: path to the groovy file

Shortname: `groovyReader`

### `org.apache.batchee.groovy.GroovyItemProcessor`

A processor delegating to a groovy script.

Sample:

```xml
 <step id="step1">
 <chunk>
   <reader ref="..." />
   <processor ref="groovyProcessor">
     <properties>
       <property name="scriptPath" value="/groovy/processor.groovy"/>
     </properties>
   </processor>
   <writer ref="..." />
 </chunk>
</step>
```

Configuration:

* scriptPath: path to the groovy file

Shortname: `groovyProcessor`

### `org.apache.batchee.groovy.GroovyItemWriter`

A writer delegating to a groovy script.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="..." />
    <processor ref="..." />
    <writer ref="groovyWriter">
      <properties>
        <property name="scriptPath" value="/groovy/writer.groovy"/>
      </properties>
    </writer>
  </chunk>
</step>
```

Configuration:

* scriptPath: path to the groovy file

Shortname: `groovyWriter`

### `org.apache.batchee.groovy.GroovyBatchlet`

A batchlet delegating to a groovy script.

Sample:

```xml
<step id="step1">
  <batchlet ref="groovyBatchlet">
    <properties>
      <property name="scriptPath" value="/groovy/batchlet.groovy"/>
    </properties>
  </batchlet>
</step>
```

Configuration:

* scriptPath: path to the groovy file

Shortname: `groovyBatchlet`

### `org.apache.batchee.extras.validation.BeanValidationItemProcessor` (JSR 330/349)

A simple processor validating an item using bean validation.

Sample:

```xml
<step id="step1">
  <chunk>
   <reader ref="..." />
   <processor ref="beanValidationProcessor" />
   <writer ref="..." />
 </chunk>
</step>
```

Configuration:

* group: the group to use to validate the item
* skipNotValidated: set to true not validated items are skipped (replaced by null)

Shortname: `beanValidationProcessor`

###  `org.apache.batchee.jsefa.JSefaCsvReader`

Use JSefa to read a CSV file.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="jsefaCsvReader">
      <properties>
        <property name="file" value="#{jobParameters['input']}"/>
        <property name="objectTypes" value="org.superbiz.Record"/>
      </properties>
    </reader>
    <writer ref="..." />
  </chunk>
</step>
```

Configuration (excepted for file see org.jsefa.csv.config.CsvConfiguration for detail):

* file: the file to read
* objectTypes: type to take into account in the unmarshalling
* validationMode: see `org.jsefa.common.config.ValidationMode`
* lineFilter: `org.jsefa.common.lowlevel.filter.LineFilter`
* lowLevelConfiguration; `org.jsefa.csv.lowlevel.config.CsvLowLevelConfiguration`
* objectAccessorProvider: `org.jsefa.common.accessor.ObjectAccessorProvider`
* simpleTypeProvider: `org.jsefa.common.converter.provider.SimpleTypeConverterProvider`
* typeMappingRegistry; `org.jsefa.common.mapping.TypeMappingRegistry`
* validationProvider; `org.jsefa.common.validator.provider.ValidatorProvider`
* quoteCharacterEscapeMode: `org.jsefa.csv.lowlevel.config.EscapeMode`
* lineFilterLimit
* specialRecordDelimiter
* lineBreak
* defaultNoValueString
* defaultQuoteMode
* fieldDelimiter
* quoteCharacter
* useDelimiterAfterLastField

Shortname: `jsefaCsvReader`

###  `org.apache.batchee.jsefa.JSefaCsvWriter`

Use JSefa to write a CSV file.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="..." />
    <writer ref="jsefaCsvWriter">
      <properties>
        <property name="file" value="#{jobParameters['output']}"/>
        <property name="objectTypes" value="org.superbiz.Record"/>
      </properties>
    </writer>
  </chunk>
</step>
```

Configuration (excepted for file and encoding see org.jsefa.csv.config.CsvConfiguration for detail):

* file: the file to write
* encoding: the output file encoding
* objectTypes: type to take into account in the marshalling
* validationMode: see `org.jsefa.common.config.ValidationMode`
* lineFilter: `org.jsefa.common.lowlevel.filter.LineFilter`
* lowLevelConfiguration; `org.jsefa.csv.lowlevel.config.CsvLowLevelConfiguration`
* objectAccessorProvider: `org.jsefa.common.accessor.ObjectAccessorProvider`
* simpleTypeProvider: `org.jsefa.common.converter.provider.SimpleTypeConverterProvider`
* typeMappingRegistry; `org.jsefa.common.mapping.TypeMappingRegistry`
* validationProvider; `org.jsefa.common.validator.provider.ValidatorProvider`
* quoteCharacterEscapeMode: `org.jsefa.csv.lowlevel.config.EscapeMode`
* lineFilterLimit
* specialRecordDelimiter
* lineBreak
* defaultNoValueString
* defaultQuoteMode
* fieldDelimiter
* quoteCharacter
* useDelimiterAfterLastField

Shortname: `jsefaCsvWriter`

###  `org.apache.batchee.jsefa.JSefaFlrReader`

Use JSefa to read a FLR file.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="jsefaFlrReader">
      <properties>
        <property name="file" value="#{jobParameters['input']}"/>
        <property name="objectTypes" value="org.superbiz.Record"/>
      </properties>
    </reader>
    <writer ref="..." />
  </chunk>
</step>
```

Configuration (excepted for file see org.jsefa.flr.config.FlrConfiguration for detail):

* file: the file to read
* objectTypes: type to take into account in the unmarshalling
* validationMode: see `org.jsefa.common.config.ValidationMode`
* lineFilter: `org.jsefa.common.lowlevel.filter.LineFilter`
* lowLevelConfiguration; `org.jsefa.flr.lowlevel.config.FlrLowLevelConfiguration`
* objectAccessorProvider: `org.jsefa.common.accessor.ObjectAccessorProvider`
* simpleTypeProvider: `org.jsefa.common.converter.provider.SimpleTypeConverterProvider`
* typeMappingRegistry; `org.jsefa.common.mapping.TypeMappingRegistry`
* validationProvider; `org.jsefa.common.validator.provider.ValidatorProvider`
* lineFilterLimit
* specialRecordDelimiter
* lineBreak
* defaultPadCharacter

Shortname: `jsefaFlrReader`

###  `org.apache.batchee.jsefa.JSefaFlrWriter`

Use JSefa to write a FLR file.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="..." />
    <writer ref="jsefaFlrWriter">
      <properties>
        <property name="file" value="#{jobParameters['output']}"/>
        <property name="objectTypes" value="org.superbiz.Record"/>
      </properties>
    </writer>
  </chunk>
</step>
```

Configuration (excepted for file see org.jsefa.flr.config.FlrConfiguration for detail):

* file: the file to write
* encoding: output file encoding
* objectTypes: type to take into account in the marshalling
* validationMode: see `org.jsefa.common.config.ValidationMode`
* lineFilter: `org.jsefa.common.lowlevel.filter.LineFilter`
* lowLevelConfiguration; `org.jsefa.flr.lowlevel.config.FlrLowLevelConfiguration`
* objectAccessorProvider: `org.jsefa.common.accessor.ObjectAccessorProvider`
* simpleTypeProvider: `org.jsefa.common.converter.provider.SimpleTypeConverterProvider`
* typeMappingRegistry; `org.jsefa.common.mapping.TypeMappingRegistry`
* validationProvider; `org.jsefa.common.validator.provider.ValidatorProvider`
* lineFilterLimit
* specialRecordDelimiter
* lineBreak
* defaultPadCharacter

Shortname: `jsefaFlrWriter`

###  `org.apache.batchee.jsefa.JSefaXmlReader`

Use JSefa to read a XML file.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="jsefaXmlReader">
      <properties>
        <property name="file" value="#{jobParameters['input']}"/>
        <property name="objectTypes" value="org.apache.batchee.jsefa.bean.Record"/>
      </properties>
    </reader>
    <processor ref="org.apache.batchee.jsefa.JSefaXmlReaderTest$StoreItems" />
    <writer ref="noopWriter" />
  </chunk>
</step>
```

Configuration (excepted for file see org.jsefa.flr.config.FlrConfiguration for detail):

* file: the file to read
* objectTypes: type to take into account in the unmarshalling
* validationMode: see `org.jsefa.common.config.ValidationMode`
* objectAccessorProvider: `org.jsefa.common.accessor.ObjectAccessorProvider`
* simpleTypeProvider: `org.jsefa.common.converter.provider.SimpleTypeConverterProvider`
* typeMappingRegistry; `org.jsefa.common.mapping.TypeMappingRegistry`
* validationProvider; `org.jsefa.common.validator.provider.ValidatorProvider`
* lowLevelConfiguration: `org.jsefa.xml.lowlevel.config.XmlLowLevelConfiguration`
* dataTypeDefaultNameRegistry: `org.jsefa.xml.mapping.support.XmlDataTypeDefaultNameRegistry`
* namespaceManager: `org.jsefa.xml.namespace.NamespaceManager`
* dataTypeAttributeName: QName with the format {uri}localName
* lineBreak
* lineIndentation

Shortname: `jsefaXmlReader`

###  `org.apache.batchee.jsefa.JSefaXmlWriter`

Use JSefa to write a XML file.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="org.apache.batchee.jsefa.JSefaXmlWriterTest$TwoItemsReader" />
    <writer ref="jsefaXmlWriter">
      <properties>
        <property name="file" value="#{jobParameters['output']}"/>
        <property name="objectTypes" value="org.apache.batchee.jsefa.bean.Record"/>
      </properties>
    </writer>
  </chunk>
</step>
```

Configuration (excepted for file see org.jsefa.flr.config.FlrConfiguration for detail):

* file: the file to write
* encoding: the output file encoding
* objectTypes: type to take into account in the marshalling
* validationMode: see `org.jsefa.common.config.ValidationMode`
* objectAccessorProvider: `org.jsefa.common.accessor.ObjectAccessorProvider`
* simpleTypeProvider: `org.jsefa.common.converter.provider.SimpleTypeConverterProvider`
* typeMappingRegistry; `org.jsefa.common.mapping.TypeMappingRegistry`
* validationProvider; `org.jsefa.common.validator.provider.ValidatorProvider`
* lowLevelConfiguration: `org.jsefa.xml.lowlevel.config.XmlLowLevelConfiguration`
* dataTypeDefaultNameRegistry: `org.jsefa.xml.mapping.support.XmlDataTypeDefaultNameRegistry`
* namespaceManager: `org.jsefa.xml.namespace.NamespaceManager`
* dataTypeAttributeName: QName with the format {uri}localName
* lineBreak
* lineIndentation

Shortname: `jsefaXmlWriter`

###  `org.apache.batchee.jackson.JacksonJsonReader`

Use JSefa to read a XML file.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="jacksonJSonReader">
      <properties>
        <property name="type" value="..."/>
        <property name="file" value="work/jackson-input.json"/>
      </properties>
    </reader>
    <writer ref="org.apache.batchee.jackson.JacksonJsonReaderTest$Writer" />
  </chunk>
</step>
```

Configuration (excepted for file see org.jsefa.flr.config.FlrConfiguration for detail):

* file: the file to read
* type: the type to use to unmarshall objects, note: without it readValueAsTree will be used
* configuration: the ObjectMapper configuration (comma separated list with for each config the syntax name=value. Name can be in DeserializationFeature, SerializationFeature and MapperFeature values and value is a boolean (true/false))
* skipRoot: should root be read as an item or not

Shortname: `jacksonJSonReader`

###  `org.apache.batchee.jackson.JacksonJSonWriter`

Use JSefa to write a XML file.

Sample:

```xml
<step id="step1">
  <chunk>
    <reader ref="org.apache.batchee.jackson.JacksonJSonWriterTest$Reader" />
    <writer ref="jacksonJSonWriter">
      <properties>
        <property name="file" value="target/work/jackson-field-output.json"/>
        <property name="fieldNameGeneratorClass" value="default"/> <!-- item1, item2, ... -->
      </properties>
    </writer>
  </chunk>
</step>
```

Configuration (excepted for file see org.jsefa.flr.config.FlrConfiguration for detail):

* file: the file to write
* encoding: output file encoding
* configuration: the ObjectMapper configuration (comma separated list with for each config the syntax name=value. Name can be in DeserializationFeature, SerializationFeature and MapperFeature values and value is a boolean (true/false))
* skipRoot: should root be written. If fieldNameGeneratorClass is not null it will use a root object and if not set root will be an array.
* fieldNameGeneratorClass: if skipRoot is not true or null it will be used to generate the field name (and force the root to be an object). "default" means use "item1", "item2", .... Otherwise set a qualified name `org.apache.batchee.jackson.FieldNameGenerator`.

Shortname: `jacksonJSonWriter`

###  `org.apache.batchee.hazelcast.HazelcastLockBatchlet`

A batchlet getting a hazelcast lock.

Sample:

```xml
<step id="lock" next="check-lock">
  <batchlet ref="hazelcastLock">
    <properties>
      <property name="instanceName" value="batchee-test"/>
      <property name="lockName" value="batchee-lock"/>
    </properties>
  </batchlet>
</step>
```

Configuration (excepted for file see org.jsefa.flr.config.FlrConfiguration for detail):

* local: a boolean to determine if the `HazelcastInstance` is local or remote (ie if the `HazelcastInstance` is a member of the cluster or a client)
* instanceName: the instance name when `local = true`
* xmlConfiguration: the xml configuration of the instance
* lockName: the lock name to use
* tryDuration: if set the lock is tried to be hold for this duration
* tryDurationUnit: the unit to use to try to get the lock (see `tryDuration`)

Shortname: `hazelcastLock`

###  `org.apache.batchee.hazelcast.HazelcastUnlockBatchlet`

A batchlet releasing a hazelcast lock.

Sample:

```xml
<step id="unlock" next="check-unlock">
  <batchlet ref="hazelcastUnlock">
    <properties>
      <property name="instanceName" value="batchee-test"/>
      <property name="lockName" value="batchee-lock"/>
    </properties>
  </batchlet>
</step>
```

Configuration (excepted for file see org.jsefa.flr.config.FlrConfiguration for detail):

* local: a boolean to determine if the `HazelcastInstance` is local or remote (ie if the `HazelcastInstance` is a member of the cluster or a client)
* instanceName: the instance name when `local = true`
* xmlConfiguration: the xml configuration of the instance
* lockName: the lock name to use

Shortname: `hazelcastUnlock`

###  CDI scopes

`@org.apache.batchee.cdi.scope.JobScoped` allows you to define a bean scoped to a job execution.
`@org.apache.batchee.cdi.scope.StepScoped` allows you to define a bean scoped to a step execution.

To activate these scopes you need to define 4 listeners:
* `org.apache.batchee.cdi.listener.BeforeJobScopeListener`
* `org.apache.batchee.cdi.listener.AfterJobScopeListener`
* `org.apache.batchee.cdi.listener.BeforeStepScopeListener`
* `org.apache.batchee.cdi.listener.AfterStepScopeListener`

If your implementation supports ordering on listeners use them to ensure `Before*` are executed first and
`After*` are executed last. This will let you use these scopes in your own listeners. `*JobScopeListener` are
`javax.batch.api.listener.JobListener` and `*StepScopeListener` are `javax.batch.api.listener.StepListener`.

NB: these listeners are `@Named` so you can use their CDI name to reference them (not mandatory)

If the implementation doesn't provide any ordering of the listeners be aware these scopes will only work
in steps.

For BatchEE you can add them in `batchee.properties` this way:

```
org.apache.batchee.job.listeners.before = beforeJobScopeListener
org.apache.batchee.job.listeners.after = afterJobScopeListener
org.apache.batchee.step.listeners.before = beforeStepScopeListener
org.apache.batchee.step.listeners.after = afterStepScopeListener
```

### GUI
#### JAX-RS resource

`org.apache.batchee.jaxrs.server.JBatchResourceImpl` maps more or less `javax.batch.operations.JobOperator` API
to JAXRS. It is available in `batchee-jaxrs-server` module.

To define it with CXF you can use the `CXFNonSpringServlet` in a servlet container, in a JavaEE container
you surely already have it and just need to define a custom `javax.ws.rs.core.Application` with `JBatchResource`
as class in `getClasses` and configure `org.apache.batchee.jaxrs.server.JBatchExceptionMapper` if you want
to map `javax.batch.operations.BatchRuntimeException` to status 500:

```xml
<web-app version="2.5"
         xmlns="http://java.sun.com/xml/ns/javaee"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://java.sun.com/xml/ns/javaee http://java.sun.com/xml/ns/javaee/web-app_2_5.xsd">
  <servlet>
    <servlet-name>CXFServlet</servlet-name>
    <display-name>JBatch JAXRS Servlet</display-name>
    <servlet-class>org.apache.cxf.jaxrs.servlet.CXFNonSpringJaxrsServlet</servlet-class>
    <init-param>
      <param-name>jaxrs.serviceClasses</param-name>
      <param-value>org.apache.batchee.jaxrs.server.JBatchResourceImpl</param-value>
    </init-param>
    <init-param>
      <param-name>jaxrs.providers</param-name>
      <param-value>
        com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider,
        org.apache.batchee.jaxrs.server.JBatchExceptionMapper
      </param-value>
    </init-param>
    <init-param>
      <param-name>jaxrs.extensions</param-name>
      <param-value>json=application/json</param-value>
    </init-param>
    <load-on-startup>1</load-on-startup>
  </servlet>

  <servlet-mapping>
    <servlet-name>CXFServlet</servlet-name>
    <url-pattern>/api/*</url-pattern>
  </servlet-mapping>
</web-app>
```

Here is the mapping:

* /job-names
* /job-instance/count/{name}
* /job-instances/{name}?start={start}&count={count}
* /executions/running/{name}
* /execution/parameter/{id}
* /job-instance/{id}
* /job-executions/{id}/{name}
* /job-execution/{id}
* /step-executions/{id}
* /execution/start/{name}
* /execution/restart/{id}
* /execution/stop/{id}
* /execution/abandon/{id}

Note: `batchee-jaxrs-client` provides a way to query it through the `JobOperator` API. You need to use
`org.apache.batchee.jaxrs.client.BatchEEJAXRSClientFactory.newClient(String url, Class<?> jsonProvider, API apiType)`.
API.AUTO tries to use JAXRS 2 client and if not available uses cxf 2.6 clients. In this last case you need to provide `cxf-rt-frontend-jaxrs`.

#### HTML gui

It is based on `org.apache.batchee.servlet.JBatchController` but since the jar - `batchee-servlet` - is in a webapp in a servlet 3.0 container,
it is automatically added and you don't need to define it in your `web.xml`.

The configuration through init parameters is:

* org.apache.batchee.servlet.active: boolean to deactivate it
* org.apache.batchee.servlet.mapping: mapping for the gui, default /jbatch/*
* org.apache.batchee.servlet.filter.private: boolean saying if internal jsp should be protected, it adds a filter to check URLs on each request

### Maven Plugin
#### Coordinates

```xml
<plugin>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-maven-plugin</artifactId>
  <version>${batchee.version}</version>
</plugin>
```

#### Configuration for a remote BatchEE instance (JAX-RS client)

```xml
<plugin>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-maven-plugin</artifactId>
  <version>0.0.1-SNAPSHOT</version>
  <configuration>
    <clientConfiguration>
      <baseUrl>http://localhost:8080/myapp/</baseUrl>
      <jsonProvider>com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider</jsonProvider>
      <security>
        <type>Basic</type>
        <username>foo</username>
        <password>bar</password>
      </security>
      <ssl>
        <hostnameVerifier>org.MyHostVerifier</hostnameVerifier>
        <keystorePassword>xxx</keystorePassword>
        <keystoreType>JKS</keystoreType>
        <keystorePath>/c/cert.crt</keystorePath>
        <sslContextType>TLS</sslContextType>
        <keyManagerType>SunX509</keyManagerType>
        <keyManagerPath>/....</keyManagerPath>
        <trustManagerAlgorithm>...</trustManagerAlgorithm>
        <trustManagerProvider>...</trustManagerProvider>
        <hostnameVerifier>org.MyHostVerifier</hostnameVerifier>
      </ssl>
    </clientConfiguration>
  </configuration>
</plugin>
```



#### Goals

```
batchee:abandon
  Abandon a job.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId to abandon.
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:count-instance
  Count job instance.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    jobName
      the job name to use to count job instances
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:execution
  Print job an execution.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId to query.
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:executions
  Print job instance executions.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    instanceId
      the instanceId to use to query job executions
      Required: Yes

    jobName
      the job name to use to query job executions
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:help
  Display help information on batchee-maven-plugin.
  Call mvn batchee:help -Ddetail=true -Dgoal=<goal-name> to display parameter
  details.

  Available parameters:

    detail
      If true, display all settable properties for each goal.

    goal
      The name of the goal for which to show help. If unspecified, all goals
      will be displayed.

    indentSize
      The number of spaces per indentation level, should be positive.

    lineLength
      The maximum length of a display line, should be positive.

batchee:instance
  Print JobInstance for a particular execution.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId to use to find the corresponding job instance
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:instances
  Print job instances.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    count
      the maximum number of instance to bring back

    jobName
      the job name to use to find job instances
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

    start
      the first job instance to take into account

batchee:job-names
  List all executed job names.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:parameters
  Print parameters for a particular execution.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId to query to find parameters
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:restart
  Restart a job.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId representing the execution to restart
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jobParameters
      the job parameters to use.

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

    wait
      wait or not the end of this task before exiting maven plugin execution.

batchee:running
  List running executions.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    jobName
      the job name used to query running executions
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:start
  Start a job.

  Available parameters:

    additionalClasspathEntries
      manual entries added in the execution classpath

    clientConfiguration
      when executed remotely the client configuration

    jobName
      the job name of the job to start
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jobParameters
      the job parameters to use.

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

    useProjectClasspath
      if the project (binaries + dependencies) should be added during the
      execution to the classpath

    wait
      wait or not the end of this task before exiting maven plugin execution.

batchee:step-executions
  Print step executions of a job execution.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId used to find step executions.
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:stop
  Stop a job.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId of the execution to stop
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally
```
