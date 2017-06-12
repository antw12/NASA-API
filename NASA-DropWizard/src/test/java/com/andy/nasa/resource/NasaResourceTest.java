package com.andy.nasa.resource;

import com.andy.nasa.configuration.NasaConfig;
import com.andy.nasa.configuration.configs.ElasticSearchConfig;
import com.andy.nasa.event.DatabaseHandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.dropwizard.jackson.Jackson;
import model.DBEntry;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import parser.EntryParser;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.zackehh.jackson.Jive.newJsonEntry;
import static com.zackehh.jackson.Jive.newObjectNode;

/**
 * Test class for the resource class
 * This is to test the API's work on build
 * Created by awaldman on 5/3/17.
 */
public class NasaResourceTest {

    private final ObjectMapper yamlMapper = Jackson.newObjectMapper(new YAMLFactory());
    private final ObjectMapper objectMapper = Jackson.newObjectMapper();
    private NasaResource nasaResource;
    private RestClient restClient;

    /**
     * Setting up the insertion of data to test against
     * @throws Exception IO exception
     */
    @BeforeSuite
    public void setUpIntegrationSuite() throws Exception{
        // get the config settings for Elastic Search
        NasaConfig nasaConfig = yamlMapper
                .readValue(
                        getClass().getResourceAsStream("/config.yml"),
                        NasaConfig.class
                );

        ElasticSearchConfig elasticSearchConfig = nasaConfig.getElasticSearchConfig();

        //build the Elastic search rest client
        restClient = RestClient
                .builder(
                        new HttpHost(
                                elasticSearchConfig.getHost(),
                                elasticSearchConfig.getPort(),
                                "http"
                        )
                )
                .build();

        // read in the mapping settings for values being entered
        FileInputStream fileInputStream = new FileInputStream("elastic-settings.json");
        JsonNode jsonNode = objectMapper.readTree(fileInputStream);

        // perform mappings on the ES instance
         restClient.performRequest(
                 "PUT",
                 "nasa",
                 Collections.emptyMap(),
                 new NStringEntity(objectMapper.writeValueAsString(jsonNode))
         );

        //read file into stream, try-with-resources
        BufferedReader br = new BufferedReader(new FileReader("usask_access_log_3000"));

        // for insertion in bulk
        StringBuilder stringBuilder = new StringBuilder();

        Integer count = 0;
        Integer bulkRequestCount = 0;
        String line;
        //while there is still a new line (data)
        while((line = br.readLine()) != null) {

            List<DBEntry> nasaData = EntryParser.parse(line);

            for (DBEntry entry : nasaData) {
                // this sets up the first part of the ES
                //bulk api syntax
                JsonNode index = newObjectNode(
                        newJsonEntry("index", newObjectNode(
                                newJsonEntry("_id", count)
                        ))
                );
                stringBuilder.append(objectMapper.writeValueAsString(index));
                stringBuilder.append("\n");
                stringBuilder.append(objectMapper.writeValueAsString(entry));
                stringBuilder.append("\n");
                count++;
                bulkRequestCount++;
                if (bulkRequestCount == 100) {
                    // perform bulk insertion in to ES
                    restClient.performRequest(
                            "POST",
                            "/nasa/log/_bulk",
                            Collections.emptyMap(),
                            new NStringEntity(stringBuilder.toString(), ContentType.APPLICATION_JSON)
                    );
                    stringBuilder = new StringBuilder();
                    bulkRequestCount = 0;
                }

            }
        }
        br.close();
        // used to control ingestion time to make sure all docs have been inserted and are tested against
       Thread.sleep(10000);

    }

    /**
     * Remove the index from elastic search
     */
    @AfterSuite
    public void tearDownAfterTests() {
        try {
            restClient.performRequest(
                    "DELETE",
                    "/nasa"
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method will set up the rest client needed to interact with ES
     * @throws Exception language exception
     */
    @BeforeClass
    public void setUpNasaResource() throws Exception {

        DatabaseHandler databaseHandler = new DatabaseHandler(restClient);
        nasaResource = new NasaResource(restClient, databaseHandler);
    }

    /**
     * This is to test to make sure entries are entered into the DB
     * @throws Exception language exception
     */
    @Test
    public void testEntryInsertion() throws Exception{
        // dont use nasaresource to enter just query and check as insertion is being made all the time
        nasaResource.entryInsertion("202.32.92.47 - - [01/Jun/1995:00:00:59 -0600] \"GET /~scottp/publish.html\" 200 271");
        JsonNode myQuery = newObjectNode(
                newJsonEntry("query", newObjectNode(
                        newJsonEntry("constant_score", newObjectNode(
                                newJsonEntry("filter", newObjectNode(
                                        newJsonEntry("term", newObjectNode(
                                                newJsonEntry("entryID", "adfa010222f0f1fd1ccd0d9b502bf77e")
                                        ))
                                ))
                        ))
                ))
        );
        Response response = restClient.performRequest(
                "GET",
                "/nasa/log/_search",
                Collections.emptyMap(),
                new NStringEntity(objectMapper.writeValueAsString(myQuery))
        );
        InputStream in = response.getEntity().getContent();
        JsonNode jsonNode = objectMapper.readTree(in);
        jsonNode = jsonNode.path("hits").path("total");
        Assert.assertEquals(jsonNode.asInt(), 1);
    }

    /**
     * This is to make sure the top 5 user query returns the expected results
     * @throws Exception language exception
     */
    @Test
    public void testTopFiveUsers() throws Exception {
        List<String> actualTopFive = new ArrayList<>();
        actualTopFive.add("lowey");
        actualTopFive.add("macphed");
        actualTopFive.add("scottp");
        actualTopFive.add("macpherc");
        actualTopFive.add("dougallg");
        List<String> topFiverUsers = nasaResource.topFiveUsers();
        Assert.assertEquals(topFiverUsers, actualTopFive);
    }

    /**
     * This test is to make sure that when given a
     * number in the API endpoint that the method
     * will find that many top users
     * @throws Exception language exception
     */
    @Test
    public void testTopNUsers() throws Exception {
        List<String> actualTopN = new ArrayList<>();
        actualTopN.add("lowey");
        actualTopN.add("macphed");
        actualTopN.add("scottp");
        List<String> topNusers = nasaResource.topNUsers(3);
        Assert.assertEquals(topNusers.size(), 3);
        Assert.assertEquals(topNusers, actualTopN);
    }

    /**
     * This method will test to make sure the average payload
     * size is as expected
     * @throws Exception language exception
     */
    @Test
    public void testAveragePayloadSize() throws Exception {
        double actualAveragePayload = 9114.067;
        double averagePayload = nasaResource.averagePayloadSize();
        Assert.assertEquals(averagePayload, actualAveragePayload);
    }

    /**
     * This method will test to return the users who
     * have requested the most data
     * @throws Exception language exception
     */
    @Test
    public void testGetUsersMostData() throws Exception {
        ObjectNode actualUserMostData = newObjectNode(
                newJsonEntry("lowey", 2298507),
                newJsonEntry("ladd", 1569981),
                newJsonEntry("turner", 1166984),
                newJsonEntry("scottp", 1024647),
                newJsonEntry("reevesm", 732713)
        );
        ObjectNode userMostData = nasaResource.getUsersMostData();
        System.out.println(userMostData);
        Assert.assertEquals(userMostData, actualUserMostData);
    }

    /**
     * This test gets the number of unique ids and tests
     * against the actual amount
     * @throws Exception language exception
     */
    @Test
    public void testGetUniqueClients() throws Exception {
        Integer actualUniqueClients = 539;
        Integer uniqueClients = nasaResource.getUniqueClients();
        Assert.assertEquals(uniqueClients, actualUniqueClients);
    }

    /**
     * This method will test the amount of docs requested per month
     * @throws Exception language exception
     */
    @Test
    public void testRequestPerMonth() throws Exception {
        ObjectNode actualRequestPerMonth = newObjectNode(
                newJsonEntry("801964800000", 3000)
        );
        ObjectNode requestsPerMonth = nasaResource.requestsPerMonth();
        System.out.println(requestsPerMonth);
        Assert.assertEquals(requestsPerMonth, actualRequestPerMonth);
    }

    /**
     * This method will test the error rate for all docs
     * @throws Exception language exception
     */
    @Test
    public void testErrorRate() throws Exception {
        double actualErrorRate = 0.4333333333333333;
        double errorRate = nasaResource.errorRate();
        Assert.assertEquals(errorRate, actualErrorRate);

    }

    /**
     * This method will test the number of errors for each month
     * @throws Exception language exception
     */
    @Test
    public void testErrorRatePerMonth() throws Exception {
        ObjectNode actualErrorPerMonth = newObjectNode(
                newJsonEntry("801964800000", 0.4333333333333333)
        );
        ObjectNode errorRatePerMonth = nasaResource.errorRatePerMonth();
        Assert.assertEquals(errorRatePerMonth, actualErrorPerMonth);
    }

    /**
     * This test will return a list of the most popular
     * file extensions
     * @throws Exception language exception
     */
    @Test
    public void testGetPopularExtensions() throws Exception {
        ObjectNode actualPopularExtensions = newObjectNode(
                newJsonEntry("html", 1170),
                newJsonEntry("gif", 864),
                newJsonEntry("shtml",53),
                newJsonEntry("GIF", 25),
                newJsonEntry("htm", 21)
        );
        ObjectNode getPopularExtensions = nasaResource.getPopularExtensions();
        Assert.assertEquals(getPopularExtensions, actualPopularExtensions);
    }

    /**
     * This test will get the number of docs with a given extension
     * @throws Exception language exception
     */
    @Test
    public void testGetNumberExtensionRequests() throws Exception {
        Integer actualExtensionRequests = 1170;
        Integer numberExtensionRequests = nasaResource.getNumberExtensionRequest("html");
        Assert.assertEquals(numberExtensionRequests, actualExtensionRequests);
    }

    /**
     * This Test will get all the API methods
     * and how many times they were called
     * @throws Exception language exception
     */
    @Test
    public void testGetAPICalls() throws Exception {
        ObjectNode actualApiCalls = newObjectNode(
                newJsonEntry("get", 2998),
                newJsonEntry("head", 1),
                newJsonEntry("post", 1)
        );
        ObjectNode apiCalls = nasaResource.getApiCalls();
        Assert.assertEquals(apiCalls, actualApiCalls);
    }

    /**
     * This test will how many requests a given user has made
     * @throws Exception language exception
     */
    @Test
    public void testGetRequestUser() throws Exception {
        ObjectNode actualRequestUser = newObjectNode(
                newJsonEntry("scottp", 214)
        );
        ObjectNode requestUser = nasaResource.getRequestsUser("scottp");
        Assert.assertEquals(requestUser, actualRequestUser);
    }
}
