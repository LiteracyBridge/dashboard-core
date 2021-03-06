dashboard-core
==============
Contains the logic of reading the various files created by the Talking Book and TB-Loader,
and populating a PostgreSQL database.


## Building and Artifacts ##

### Prerequisites ###

**Java 8**
:  This project requires Java 8.  Make sure you have it installed and the default JVM.

**Maven**
: This project currently uses Maven for building.  Make sure you have maven installed.  If you don't you can get one at http://maven.apache.org/download.cgi

### Building ###

    mvn clean install

will build the project cleanly and run all the unit tests.

### Configuring ###

Since the dashboard-core needs to talk to S3, databases, and potentially other services, it has a [built-in configuration mechanism](https://github.com/LiteracyBridge/dashboard-core/blob/master/src/main/java/org/literacybridge/dashboard/config/PropertiesConfig.java) to look up this configuration.  It will look for a properties file in the following locations:

1.  ./dashboard.properties
2.  /opt/literacybridge/dashboard.properties
3.  otherwise it will default to the values in spring/default.properties file, located in src/main/resources/spring

To get the full list of properties, look in [src/main/resources/spring/default.properties](https://github.com/LiteracyBridge/dashboard-core/blob/master/src/main/resources/spring/default.properties)

### Using from the Commandline ###

When building, there are two jars that are created in the `target` directory:

1.  The regular jar.  This will have a version number on it, and possibly a SNAPSHOT designation.
2.  A self-contained jar.  This one is named `core-with-deps.jar` and contains all the dependency libraries in it as well.

You can execute the self-contained jar to use the commandline tools by simply calling:

    java -jar target/core-with-deps.jar -?

or using the convenience script in 
    target/lb-dashboard


There will initially be a decent amount of spewed out logging information.  This is where the tool is determining that the database has the appropriate schema objects in it. **It is important to note that this commandline tool goes directly against the DB + S3, so it is a low-level tool.  You should only use this if you know what  you are doing.  Otherwise, try to use the REST APIs on the dashboard itself.**

