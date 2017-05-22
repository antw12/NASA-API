package com.andy.nasa.resource;

import com.andy.nasa.event.DatabaseHandler;
import com.andy.nasa.configuration.NasaConfig;
import com.andy.nasa.configuration.configs.ElasticSearchConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.dropwizard.jackson.Jackson;
import org.apache.http.HttpHost;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
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
     * This method will set up the restclient needed to interact with ES
     * @throws Exception
     */
    @BeforeClass
    public void setUpNasaResource() throws Exception {
        NasaConfig nasaConfig = yamlMapper
                .readValue(
                        getClass().getResourceAsStream("/config.yml"),
                        NasaConfig.class
                );

        ElasticSearchConfig elasticSearchConfig = nasaConfig.getElasticSearchConfig();
        restClient = RestClient
            .builder(
                    new HttpHost(
                            elasticSearchConfig.getHost(),
                            elasticSearchConfig.getPort(),
                            "http"
                    )
            )
            .build();
        DatabaseHandler databaseHandler = new DatabaseHandler(restClient);
        nasaResource = new NasaResource(restClient, databaseHandler);
    }

    /**
     * This is to test to make sure entries are entered into the DB
     * @throws Exception
     */
    @Test
    public void testEntryInsertion() throws Exception{
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
     * @throws Exception
     */
    @Test
    public void testTopFiveUsers() throws Exception {
        List<String> actualTopFive = new ArrayList<>();
        actualTopFive.add("lowey");
        actualTopFive.add("scottp");
        actualTopFive.add("macphed");
        actualTopFive.add("keele");
        actualTopFive.add("macpherc");
        List<String> topFiverUsers = nasaResource.topFiveUsers();
        Assert.assertEquals(topFiverUsers, actualTopFive);
    }

    /**
     * This test is to make sure that when given a
     * number in the API endpoint that the method
     * will find that many top users
     * @throws Exception
     */
    @Test
    public void testTopNUsers() throws Exception {
        List<String> actualTopN = new ArrayList<>();
        actualTopN.add("lowey");
        actualTopN.add("scottp");
        actualTopN.add("macphed");
        List<String> topNusers = nasaResource.topNUsers(3);
        Assert.assertEquals(topNusers.size(), 3);
        Assert.assertEquals(topNusers, actualTopN);
    }

    /**
     * This method will test to make sure the average payload
     * size is as expected
     * @throws Exception
     */
    @Test
    public void testAveragePayloadSize() throws Exception {
        double actualAveragePayload = 5375.269638156825;
        double averagePayload = nasaResource.averagePayloadSize();
        Assert.assertEquals(averagePayload, actualAveragePayload);
    }

    /**
     * This method will test to return the users who
     * have requested the most data
     * @throws Exception
     */
    @Test
    public void testGetUsersMostData() throws Exception {
        ObjectNode actualUserMostData = newObjectNode(
                newJsonEntry("scottp", 2147483647),
                newJsonEntry("turner", 1221686450),
                newJsonEntry("lowey", 1002439963),
                newJsonEntry("keele", 719100178),
                newJsonEntry("ladd", 571747085)
        );
        ObjectNode userMostData = nasaResource.getUsersMostData();
        System.out.println(userMostData);
        Assert.assertEquals(userMostData, actualUserMostData);
    }

    /**
     * This test gets the number of unique ids and tests
     * against the actual amount
     * @throws Exception
     */
    @Test
    public void testGetUniqueClients() throws Exception {
        Integer actualUniqueClients = 137640;
        Integer uniqueClients = nasaResource.getUniqueClients();
        Assert.assertEquals(uniqueClients, actualUniqueClients);
    }

    /**
     * This method will test the amount of docs requested per month
     * @throws Exception
     */
    @Test
    public void testRequestPerMonth() throws Exception {
        ObjectNode actualRequestPerMonth = newObjectNode(
                newJsonEntry("801964800000", 214568),
                newJsonEntry("804556800000", 226501),
                newJsonEntry("807235200000", 269049),
                newJsonEntry("809913600000", 356571),
                newJsonEntry("812505600000", 458821),
                newJsonEntry("815184000000", 463477),
                newJsonEntry("817776000000", 407733),
                newJsonEntry("820454400000", 2054)
        );
        ObjectNode requestsPerMonth = nasaResource.requestsPerMonth();
        System.out.println(requestsPerMonth);
        Assert.assertEquals(requestsPerMonth, actualRequestPerMonth);
    }

    /**
     * This method will test the error rate for all docs
     * @throws Exception
     */
    @Test
    public void testErrorRate() throws Exception {
        double actualErrorRate = 0.9279323521098695;
        double errorRate = nasaResource.errorRate();
        Assert.assertEquals(errorRate, actualErrorRate);

    }

    /**
     * This method will test the number of errors for each month
     * @throws Exception
     */
    @Test
    public void testErrorRatePerMonth() throws Exception {
        ObjectNode actualErrorPerMonth = newObjectNode(
                newJsonEntry("801964800000", 0.6720480220722568),
                newJsonEntry("804556800000", 0.5545229380885736),
                newJsonEntry("807235200000", 0.761199632780646),
                newJsonEntry("809913600000", 1.0258826432884334),
                newJsonEntry("812505600000", 0.9633386440463709),
                newJsonEntry("815184000000", 1.1653221195442276),
                newJsonEntry("817776000000", 0.9852035523246829),
                newJsonEntry("820454400000", 0.8276533592989289)
        );
        ObjectNode errorRatePerMonth = nasaResource.errorRatePerMonth();
        Assert.assertEquals(errorRatePerMonth, actualErrorPerMonth);
    }

    /**
     * This test will return a list of the most popular
     * file extensions
     * @throws Exception
     */
    @Test
    public void testGetPopularExtensions() throws Exception {
        ObjectNode actualPopularExtensions = newObjectNode(
                newJsonEntry("html", 938876),
                newJsonEntry("gif",888899),
                newJsonEntry("shtml",23899),
                newJsonEntry("here_",12720),
                newJsonEntry("search",12713),
                newJsonEntry("term",12707),
                newJsonEntry("3f_cusi",10733),
                newJsonEntry("htm",9954),
                newJsonEntry("uk",9098),
                newJsonEntry("3d",8592)
        );
        ObjectNode getPopularExtensions = nasaResource.getPopularExtensions();
        Assert.assertEquals(getPopularExtensions, actualPopularExtensions);
    }

    /**
     * This test will get the number of docs with a given extension
     * @throws Exception
     */
    @Test
    public void testGetNumberExtensionRequests() throws Exception {
        Integer actualExtensionRequests = 938876;
        Integer numberExtensionRequests = nasaResource.getNumberExtensionRequest("html");
        Assert.assertEquals(numberExtensionRequests, actualExtensionRequests);
    }

    /**
     * This Test will get all the API methods
     * and how many times they were called
     * @throws Exception
     */
    @Test
    public void testGetAPICalls() throws Exception {
        ObjectNode actualApiCalls = newObjectNode(
                newJsonEntry("get", 2388274),
                newJsonEntry("head", 7171),
                newJsonEntry("post", 3305)
        );
        ObjectNode apiCalls = nasaResource.getApiCalls();
        Assert.assertEquals(apiCalls, actualApiCalls);
    }

    /**
     * This test will how many requests a given user has made
     * @throws Exception
     */
    @Test
    public void testGetRequestUser() throws Exception {
        ObjectNode actualRequestUser = newObjectNode(
                newJsonEntry("scottp", 206890)
        );
        ObjectNode requestUser = nasaResource.getRequestsUser("scottp");
        Assert.assertEquals(requestUser, actualRequestUser);
    }
}
