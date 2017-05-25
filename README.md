# NASA-API
[![Build Status](https://travis-ci.org/antw12/NASA-API.svg?branch=master)](https://travis-ci.org/antw12/NASA-API)

The NASA-API project was a learning project that I created in my spare time to become familiar with some technologies I had not had experience with. In addition to becoming familiar, it also taught me new syntax for Java (Java 8), in addition to becoming accustomed to Git, Github and subversion control. 

The project itself is a dropwizard application in which when and instance of it is instantiated, it waits for a "client" to send data/ entries from a NASA data file, which in turn it then parses and writes to a local instance of elastic search, with the documents being in such a format that allows for API's to be called to return data from queries which match a specification. 

# Technologies Used 
  * Java 8 (https://docs.oracle.com/javase/8/docs/api/)
  * Dropwizard (http://www.dropwizard.io/1.1.0/docs/getting-started.html)
  * Rabbit MQ (https://www.rabbitmq.com/)
  * Swagger (http://swagger.io/)
  * Maven (http://search.maven.org/)
  * Spring (https://spring.io/)
  * Jackson (https://github.com/FasterXML/jackson)
  * Jive (https://github.com/zackehh/jive)
  * Immutables (https://immutables.github.io/)
  * Elastic Search 5 (https://www.elastic.co/guide/en/elasticsearch/reference/5.0/index.html)
  
# How To Download

`git clone https://github.com/antw12/NASA-API.git`

# File To Parse 

http://ita.ee.lbl.gov/html/contrib/Sask-HTTP.html

Select the NASA-HTTP

# Client Side (Sending Entries)

https://github.com/antw12/Node-NASA-Client

`git clone https://github.com/antw12/Node-NASA-Client.git`

# How To Run

1. First you will need to have maven installed.

  `brew install maven`

2. Inside the directory of the NASA-API project, run `mvn clean install` to make sure all tests pass and the clone was clean.

3. Download and install Elastic Search, and run a local instance with default settings.

4. perform a curl command for health check inside your terminal for Elastic search

5. Inside the Node-NASA-Client app.js, change the file directory to the directory in which the Nasa-HTTP file you downloaded is located.

6. Find the NasaApplication file and run it, this will run the server applet.

7. Either run `node app.js` inside the Node-NASA-Client or inside your IDE run the client to send messages to rabbit

8. NASA-API will now consume these entries parse them and place them into the local instance of elastic search.

9. Using the API's provided inside the NasaResource file you can query the database for all type of data, eg top 5 users or how many requests passed, or error rate for requests.

10. Done!!!! 

# Tests

### Integrated Tests
 
 The Integrated tests, I am using travis, to spin up an instance of Elastic search exposing 9200, 9300. Then entering in the data from a file, (5000 entries) and then using the queries from the API's to then make sure that data is being inserted correctly and that the queries return what is expected.
 
### Unit Tests 

 The unit tests make sure that the incoming entries are parsed, and not only parsed, but parsed correctly, in addition to making sure that the entry objects are created correctly and that entries that don't match the requirements are ignored 



