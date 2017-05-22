package com.andy.nasa.resource;

import com.andy.nasa.event.DatabaseHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zackehh.jackson.Jive;
import com.zackehh.jackson.stream.JiveCollectors;
import io.dropwizard.jackson.Jackson;
import io.swagger.annotations.Api;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.zackehh.jackson.Jive.newJsonEntry;
import static com.zackehh.jackson.Jive.newObjectNode;

/**
 * This resource class is the API's that can be called with the dropwizard application
 * Created by awaldman on 4/18/17.
 */
@Path("/")
@Api
@Produces(MediaType.APPLICATION_JSON)
public class NasaResource {

    // rest client for ES
    private final RestClient restClient;

    // database handler to allow writing to database
    private final DatabaseHandler databaseHandler;

    // this is to map json values
    private final ObjectMapper objectMapper = Jackson.newObjectMapper();

    // end point for index and indices
    private final String endpoint = "/nasa/log/";

    // common field in query taken as global for removal of redundant code
    private final Map.Entry<String, JsonNode> size = newJsonEntry("size", 0);

    /**
     * This creates an instances of the NasaResource passing the rest client for es5
     * this allows me to interact over http to elastic search
     * @param restClient
     */
    public NasaResource(RestClient restClient, DatabaseHandler databaseHandler) {
        this.restClient = restClient;
        this.databaseHandler = databaseHandler;
    }

    /**
     * When called this API will post data to elastic search
     * @param entryPayload
     * @throws Exception
     */
    @POST
    @Path("/entry")
    public void entryInsertion(String entryPayload) throws Exception {
        databaseHandler.writeToDB(entryPayload);
    }

    /**
     * This API queries for the top 5 users
     * @return List<String>
     * @throws Exception
     */
    @GET
    @Path("/top-five-users")
    public List<String> topFiveUsers() throws Exception {
        JsonNode myQuery = newObjectNode(
            size,
            newJsonEntry("aggs", newObjectNode(
                newJsonEntry("group_by_username", newObjectNode(
                    newJsonEntry("terms", newObjectNode(
                        newJsonEntry("field", "username"),
                        newJsonEntry("size", 5)
                    ))
                ))
            ))
        );
        // input stream returning the response in bytes form the request
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        // getting the path to the buckets from the input stream
        JsonNode buckets = getPath(in, "aggregations", "group_by_username", "buckets");
        // turning the buckets in to an json array
        ArrayNode bucketArray = (ArrayNode) buckets;
        // use a stream of each bucket returned getting the value of key and collecting it in list form
        return Jive
                .stream(bucketArray)
                .map(bucketNode -> bucketNode.path("key").asText())
                .collect(Collectors.toList());
    }

    /**
     * This API queries for the top n amount of users and returns a list
     * in descending order from most seen user
     * @param nUsers
     * @return List<String>
     * @throws Exception
     */
    @GET
    @Path("/top-n-users/{nUsers}")
    public List<String> topNUsers(@PathParam("nUsers") Integer nUsers) throws Exception {
        JsonNode myQuery = newObjectNode(
                size,
                newJsonEntry("aggs", newObjectNode(
                        newJsonEntry("group_by_username", newObjectNode(
                                newJsonEntry("terms", newObjectNode(
                                        newJsonEntry("field", "username"),
                                        newJsonEntry("size", nUsers)
                                ))
                        ))
                ))
        );
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        JsonNode buckets = getPath(in, "aggregations", "group_by_username", "buckets");
        ArrayNode bucketArray = (ArrayNode) buckets;
        // use a stream of each bucket returned getting the value of key and collecting it in list form
        return Jive
                .stream(bucketArray)
                .map(bucketNode -> bucketNode.path("key").asText())
                .collect(Collectors.toList());
    }

    /**
     * This API will query for the average payload size all of all the entries in the DB
     * @return float
     * @throws Exception
     */
    @GET
    @Path("/average-payload-size")
    public double averagePayloadSize() throws Exception {
        JsonNode myQuery = newObjectNode(
            size,
                newJsonEntry("aggs", newObjectNode(
                    newJsonEntry("average_payloadsize", newObjectNode(
                        newJsonEntry("avg", newObjectNode(
                            newJsonEntry("field", "payloadSize")
                        ))
                    ))
                ))
        );
        // content of the response from the query request
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        // get the information from the specified path from the input stream (response)
        JsonNode averagePayloadSize = getPath(in, "aggregations", "average_payloadsize");
        // returning the value of the average payload size
        return averagePayloadSize.path("value").asDouble();
    }

    /**
     * This API will return the users that request the most amount of data
     * @return ObjectNode
     * @throws Exception
     */
    @GET
    @Path("/users/data")
    public ObjectNode getUsersMostData() throws Exception {
        JsonNode myQuery = newObjectNode(
                size,
                newJsonEntry("aggs", newObjectNode(
                        newJsonEntry("by_user", newObjectNode(
                                newJsonEntry("terms", newObjectNode(
                                        newJsonEntry("field", "username"),
                                        newJsonEntry("order", newObjectNode(
                                                newJsonEntry("total_payloadSize", "desc")
                                        )),
                                        newJsonEntry("size", 5)
                                )),
                                newJsonEntry("aggs", newObjectNode(
                                        newJsonEntry("total_payloadSize" , newObjectNode(
                                                newJsonEntry("sum", newObjectNode(
                                                        newJsonEntry("field", "payloadSize")
                                                ))
                                        ))
                                ))
                        ))
                ))
        );
        // content of the response from the query request
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        // get the information from the specified path from the input stream (response)
        JsonNode buckets = getPath(in, "aggregations", "by_user", "buckets");
        // returning the value of the average payload size
        ArrayNode arrayNode = (ArrayNode) buckets;

        return Jive
                .stream(arrayNode)
                .map(bucket -> Jive.newJsonEntry(
                        bucket.path("key").asText(),
                        bucket.path("total_payloadSize").path("value").asInt()
                ))
                .collect(JiveCollectors.toObjectNode());

    }

    /**
     * This API will return the number of all the clients (non-duplicates)
     * @return Integer
     * @throws Exception
     */
    @GET
    @Path("/clients/unique")
    public Integer getUniqueClients() throws Exception {
        JsonNode myQuery = newObjectNode(
                size,
                newJsonEntry("aggs", newObjectNode(
                        newJsonEntry("distinct_clients", newObjectNode(
                                newJsonEntry("cardinality", newObjectNode(
                                        newJsonEntry("field", "client")
                                ))
                        ))
                ))
        );
        // content of the response from the query request
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        // get the information from the specified path from the input stream (response)
        JsonNode clients = getPath(in, "aggregations", "distinct_clients");
        return clients.path("value").asInt();
    }

    /**
     * This API will return how many requests were sent everymonth
     * @return ObjectNode
     * @throws Exception
     */
    @GET
    @Path("/months/requests")
    public ObjectNode requestsPerMonth() throws Exception {
        JsonNode myQuery = newObjectNode(
                size,
                newJsonEntry("aggs", newObjectNode(
                        newJsonEntry("get_months", newObjectNode(
                                newJsonEntry("date_histogram", newObjectNode(
                                        newJsonEntry("field", "datetime"),
                                        newJsonEntry("interval", "month")
                                ))
                        ))
                ))
        );
        // content of the response from the query request
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        // get the information from the specified path from the input stream (response)
        JsonNode buckets = getPath(in, "aggregations", "get_months", "buckets");
        // returning the value of the average payload size
        ArrayNode bucketArray = (ArrayNode) buckets;

        return Jive
                .stream(bucketArray)
                .map(bucket -> Jive.newJsonEntry(
                    bucket.path("key").asText(),
                    bucket.path("doc_count")
                ))
                .collect(JiveCollectors.toObjectNode());
    }

    /**
     * This API will return the error rate for a request of data from NASA
     * @return double
     * @throws Exception
     */
    @GET
    @Path("/error/rate")
    public double errorRate() throws Exception{
        Response response = restClient.performRequest(
                "GET",
                endpoint + "_count"
        );
        // content of the response from the query request
        InputStream in = response.getEntity().getContent();
        // get the information from the specified path from the input stream (response)
        Integer totalDocs = getPath(in, "count").asInt();

        JsonNode myQueryTwo = newObjectNode(
                size,
                newJsonEntry("aggs", newObjectNode(
                        newJsonEntry("filter_responseCode", newObjectNode(
                                newJsonEntry("filter", newObjectNode(
                                        newJsonEntry("range", newObjectNode(
                                                newJsonEntry("responseCode", newObjectNode(
                                                        newJsonEntry("gte", 400),
                                                        newJsonEntry("lte", 599)
                                                ))
                                        ))
                                ))
                        ))
                ))
        );
        // content of the response from the query request
        InputStream inTwo = performQueryRequest(myQueryTwo, "search").getEntity().getContent();
        // get the information from the specified path from the input stream (response)
        Double errorDocTotal = getPath(inTwo, "aggregations", "filter_responseCode", "doc_count").asDouble();
        return (errorDocTotal / totalDocs) * 100;
    }

    /**
     * This API will return the error rate for each month
     * @return ObjectNode
     * @throws Exception
     */
    @GET
    @Path("/error/rate/month")
    public ObjectNode errorRatePerMonth() throws Exception {
        JsonNode myQuery = newObjectNode(
                size,
                newJsonEntry("aggs", newObjectNode(
                        newJsonEntry("get_months", newObjectNode(
                                newJsonEntry("date_histogram", newObjectNode(
                                        newJsonEntry("field", "datetime"),
                                        newJsonEntry("interval", "month")
                                )),
                                newJsonEntry("aggs", newObjectNode(
                                        newJsonEntry("filter_responseCode", newObjectNode(
                                                newJsonEntry("filter", newObjectNode(
                                                        newJsonEntry("range", newObjectNode(
                                                                newJsonEntry("responseCode", newObjectNode(
                                                                        newJsonEntry("gte", 400),
                                                                        newJsonEntry("lte", 599)
                                                                ))
                                                        ))
                                                ))
                                        ))
                                ))
                        ))
                ))
        );
        // content of the response from the query request
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        // get the information from the specified path from the input stream (response)
        JsonNode buckets = getPath(in, "aggregations", "get_months", "buckets");
        // returning the value of the average payload size
        ArrayNode bucketArray = (ArrayNode) buckets;

        return Jive
                .stream(bucketArray)
                .map(bucketNode -> Jive.newJsonEntry(
                        bucketNode.path("key").asText(),
                        (bucketNode.path("filter_responseCode").path("doc_count").asDouble()
                                / bucketNode.path("doc_count").asDouble()) * 100
                ))
                .collect(JiveCollectors.toObjectNode());
    }

    /**
     * This API will return the most popular extensions from resources requested
     * @return
     * @throws Exception
     */
    @GET
    @Path("/extensions/popular")
    public ObjectNode getPopularExtensions() throws Exception {
        JsonNode myQuery = newObjectNode(
                size,
                newJsonEntry("aggs", newObjectNode(
                        newJsonEntry("group_file_extensions", newObjectNode(
                                newJsonEntry("terms", newObjectNode(
                                        newJsonEntry("field", "fileExtension")
                                ))
                        ))
                ))
        );
        // content of the response from the query request
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        // get the information from the specified path from the input stream (response)
        JsonNode buckets = getPath(in, "aggregations", "group_file_extensions", "buckets");
        // returning the value of the average payload size
        ArrayNode bucketArray = (ArrayNode) buckets;
        return Jive
                .stream(bucketArray)
                .map(bucketNode -> Jive.newJsonEntry(
                        bucketNode.path("key").asText(),
                        bucketNode.path("doc_count").asInt()
                ))
                .collect(JiveCollectors.toObjectNode());
    }

    /**
     * This API will return number of times an extension was requested
     * @param extension
     * @return Integer
     * @throws Exception
     */
    @GET
    @Path("/extensions/{extension}")
    public Integer getNumberExtensionRequest(@PathParam("extension") String extension) throws Exception{
        JsonNode myQuery = newObjectNode(
                newJsonEntry("query", newObjectNode(
                        newJsonEntry("constant_score", newObjectNode(
                                newJsonEntry("filter", newObjectNode(
                                        newJsonEntry("term", newObjectNode(
                                                newJsonEntry("fileExtension", extension)
                                        ))
                                ))
                        ))
                ))
        );
        // content of the response from the query request
        InputStream in = performQueryRequest(myQuery, "count").getEntity().getContent();
        return getPath(in, "count").intValue();
    }

    /**
     * This API will get all the restAPI calls and how many times each one was used
     * only three as they are the only ones that matter
     * @return Object Node
     * @throws Exception
     */
    @GET
    @Path("/api/call")
    public ObjectNode getApiCalls() throws Exception {
        JsonNode myQuery = newObjectNode(
                size,
                newJsonEntry("aggs", newObjectNode(
                        newJsonEntry("group_by_api", newObjectNode(
                                newJsonEntry("terms", newObjectNode(
                                        newJsonEntry("field", "restfulAPI"),
                                        newJsonEntry("size", 3)
                                ))
                        ))
                ))
        );
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        // get the information from the specified path from the input stream (response)
        JsonNode buckets = getPath(in, "aggregations", "group_by_api", "buckets");
        // returning the value of the average payload size
        ArrayNode bucketArray = (ArrayNode) buckets;
        return Jive
                .stream(bucketArray)
                .map(bucketNode -> Jive.newJsonEntry(
                        bucketNode.path("key").asText(),
                        bucketNode.path("doc_count").asInt()
                ))
                .collect(JiveCollectors.toObjectNode());
    }

    /**
     * This API is used to get the number of requests a given user makes to the NASA data
     * @param user
     * @return ObjectNode
     * @throws Exception
     */
    @GET
    @Path("/requests/{user}")
    public ObjectNode getRequestsUser(@PathParam("user") String user) throws Exception{
        JsonNode myQuery = newObjectNode(
                size,
                newJsonEntry("query", newObjectNode(
                        newJsonEntry("constant_score", newObjectNode(
                                newJsonEntry("filter", newObjectNode(
                                        newJsonEntry("term", newObjectNode(
                                                newJsonEntry("username", user)
                                        ))

                                ))
                        ))
                )),
                newJsonEntry("aggs", newObjectNode(
                        newJsonEntry("group_by_api", newObjectNode(
                                newJsonEntry("terms", newObjectNode(
                                        newJsonEntry("field", "username")
                                ))
                        ))
                ))
        );
        // content of the response from the query request
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        // get the information from the specified path from the input stream (response)
        JsonNode buckets = getPath(in, "aggregations", "group_by_api", "buckets");
        // returning the value of the average payload size
        ArrayNode bucketArray = (ArrayNode) buckets;
        return Jive
                .stream(bucketArray)
                .map(bucketNode -> Jive.newJsonEntry(
                        bucketNode.path("key").asText(),
                        bucketNode.path("doc_count").asInt()
                ))
                .collect(JiveCollectors.toObjectNode());
    }

    /**
     * This method is use to set up and perform the http request/ elastic search query
     * which then returns the response to that request
     * @param jsonNode
     * @param queryType
     * @return Response
     * @throws Exception
     */
    Response performQueryRequest(JsonNode jsonNode, String queryType) throws Exception {
        return  restClient.performRequest(
            "GET",
            endpoint + "_" + queryType,
            Collections.emptyMap(),
            new NStringEntity(objectMapper.writeValueAsString(jsonNode))
        );
    }

    /**
     * This method will get Json object at a certain path in the response object
     * mainly used to simplify code, keep it more maintainable and use variadic variables
     * @param in
     * @param paths
     * @return JsonNode
     * @throws Exception
     */
    JsonNode getPath(InputStream in, String... paths) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(in);
        for (String str : paths){
            jsonNode = jsonNode.path(str);
        }
        return jsonNode;
    }
}