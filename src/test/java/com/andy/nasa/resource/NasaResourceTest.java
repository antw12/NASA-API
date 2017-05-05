package com.andy.nasa.resource;

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

        nasaResource = new NasaResource(restClient);
    }

    /**
     * This is to test to make sure entries are entered into the DB
     * @throws Exception
     */
    @Test
    public void testWriteToDB() throws Exception{
        nasaResource.writeToDB("202.32.92.47 - - [01/Jun/1995, 00, 00, 59 -0600] \"GET /~scottp/publish.html\" 200 271");
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
        actualTopFive.add("dougallg");
        actualTopFive.add("turner");
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
        double actualAveragePayload = 5542.399931965076;
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
                newJsonEntry("lowey", 12816321),
                newJsonEntry("scottp", 6893113),
                newJsonEntry("turner", 6579454),
                newJsonEntry("keele", 5782093),
                newJsonEntry("ladd", 5262472)
        );
        ObjectNode userMostData = nasaResource.getUsersMostData();
        Assert.assertEquals(userMostData, actualUserMostData);
    }

    /**
     * This test gets the number of unique ids and tests
     * against the actual amount
     * @throws Exception
     */
    @Test
    public void testGetUniqueClients() throws Exception {
        Integer actualUniqueClients = 3042;
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
                newJsonEntry("801964800000", 17637)
        );
        ObjectNode requestsPerMonth = nasaResource.requestsPerMonth();
        Assert.assertEquals(requestsPerMonth, actualRequestPerMonth);
    }

    /**
     * This method will test the error rate for all docs
     * @throws Exception
     */
    @Test
    public void testErrorRate() throws Exception {
        double actualErrorRate = 0.034017462297312624;
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
                newJsonEntry("801964800000", 0.03401939105290015)
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
                newJsonEntry("html", 6504),
                newJsonEntry("gif",5307),
                newJsonEntry("shtml",275),
                newJsonEntry("here_",249),
                newJsonEntry("search",249),
                newJsonEntry("term",249),
                newJsonEntry("3f_cusi",229),
                newJsonEntry("htm",105),
                newJsonEntry("uk",92),
                newJsonEntry("3d",74)
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
        Integer actualExtensionRequests = 6504;
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
                newJsonEntry("get", 17621),
                newJsonEntry("head",12),
                newJsonEntry("post",5)
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
                newJsonEntry("scottp", 1382)
        );
        ObjectNode requestUser = nasaResource.getRequestsUser("scottp");
        Assert.assertEquals(requestUser, actualRequestUser);
    }
}
