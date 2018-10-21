# Dedup the String deduplicating JDBC driver

A full featured JDBC wrapping driver which deduplicates String instances. 
This driver works with any existing JDBC driver/database.

Since this a JDBC driver like any other it can used with any existing application 
or persistence framework (Hibernate, JOOQ, myBatis,...)

# Features

* Reduces memory footprint of application
* No code change
* Works with any JDBC driver
* Configure the specific driver options using the driver properties
* Inspect and control the driver at runtime through JMC
  * A JMX entry is created for each unique connection string  

## Using the dedup driver
To benefit from the deduplication feature simple:

1. add the jdbc-string-deduplicator jar to the classpath of your application or application server
2. modify to JDBC connection string of the connection pool(s) or connection to use the _dedup_ driver  


Configuring the driver is simple by design. Suppose your original connection string is of the form:

`jdbc:<subprotocol>:<subname>`

To use the dedup driver you change the connection string into:

`jdbc:dedup:<subprotocol>:<subname>`

Alternatively one can specify the fully qualified name of the wrapped driver as well. The connection string is of the from:

`jdbc:dedup:fully_qualified_driver_classname:<subprotocol>:<subname>`

###Configuration examples

From

`jdbc:h2:mem:`

Into

`jdbc:dedup:h2:mem:`

Or alternatively

`jdbc:dedup:org.h2.Driver:h2:mem:` 

# More information and examples

For more information please see https://www.externalizer4j.com