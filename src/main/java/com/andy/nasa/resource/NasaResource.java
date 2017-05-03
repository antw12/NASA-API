package com.andy.nasa.resource;

import com.andy.nasa.model.DBEntry;
import com.andy.nasa.parser.EntryParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zackehh.jackson.Jive;
import com.zackehh.jackson.stream.JiveCollectors;
import io.dropwizard.jackson.Jackson;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.immutables.value.internal.$processor$.meta.$ValueMirrors;

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
@Produces(MediaType.APPLICATION_JSON)
public class NasaResource {

    private final RestClient restClient;
    private final ObjectMapper objectMapper = Jackson.newObjectMapper();
    private final String endpoint = "/nasa/log/";
    private final Map.Entry<String,JsonNode> size = newJsonEntry("size", 0);

    /**
     * This creates an instances of the NasaResource passing the rest client for es5
     * this allows me to interact over http to elastic search
     * @param restClient
     */
    public NasaResource(RestClient restClient) {
        this.restClient = restClient;
    }

    @POST
    @Path("/entry")
    public void writeToDB(String entryPayload) throws Exception {
        List<DBEntry> nasaData = EntryParser.parse(entryPayload);

        for (DBEntry entry: nasaData) {
            String jsonString = objectMapper.writeValueAsString(entry);
            //write to db as json
            restClient.performRequest(
                "PUT",
                endpoint + entry.entryID(),
                Collections.emptyMap(),
                new NStringEntity(jsonString)
            );
        }
    }

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
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        JsonNode buckets = getPath(in, "aggregations", "group_by_username", "buckets");
        ArrayNode bucketArray = (ArrayNode) buckets;

        return Jive
                .stream(bucketArray)
                .map(bucketNode -> bucketNode.path("key").asText())
                .collect(Collectors.toList());
    }

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

        return Jive
                .stream(bucketArray)
                .map(bucketNode -> bucketNode.path("key").asText())
                .collect(Collectors.toList());
    }

    @GET
    @Path("/average-payload-size")
    public float averagePayloadSize() throws Exception {
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
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        JsonNode averagePayloadSize = getPath(in, "aggregations", "average_payloadsize");
        return averagePayloadSize.path("value").floatValue();
    }

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
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        JsonNode buckets = getPath(in, "aggregations", "by_user", "buckets");
        ArrayNode arrayNode = (ArrayNode) buckets;

        return Jive
                .stream(arrayNode)
                .map(bucket -> Jive.newJsonEntry(
                        bucket.path("key").asText(),
                        bucket.path("total_payloadSize").path("value").asInt()
                ))
                .collect(JiveCollectors.toObjectNode());

    }

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
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        JsonNode clients = getPath(in, "aggregations", "distinct_clients");
        return clients.path("value").asInt();
    }

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
        InputStream in = performQueryRequest(myQuery,"search").getEntity().getContent();
        JsonNode buckets = getPath(in, "aggregations", "get_months", "buckets");
        ArrayNode bucketArray = (ArrayNode) buckets;

        return Jive
                .stream(bucketArray)
                .map(bucket -> Jive.newJsonEntry(
                    bucket.path("key").asText(),
                    bucket.path("doc_count")
                ))
                .collect(JiveCollectors.toObjectNode());
    }

    @GET
    @Path("/error/rate")
    public double errorRate() throws Exception{
        Response response = restClient.performRequest(
                "GET",
                endpoint + "_count"
        );
        InputStream in = response.getEntity().getContent();
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
        InputStream inTwo = performQueryRequest(myQueryTwo, "search").getEntity().getContent();
        Double errorDocTotal = getPath(inTwo, "aggregations", "filter_responseCode", "doc_count").asDouble();
        return (errorDocTotal / totalDocs) * 100;
    }

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
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        JsonNode buckets = getPath(in, "aggregations", "get_months", "buckets");
        ArrayNode bucketArray = (ArrayNode) buckets;

        return Jive
                .stream(bucketArray)
                .map(bucketNode -> Jive.newJsonEntry(
                        bucketNode.path("key").asText(),
                        (bucketNode.path("filter_responseCode").path("doc_count").asDouble() / bucketNode.path("doc_count").asDouble()) * 100
                ))
                .collect(JiveCollectors.toObjectNode());
    }

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
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        JsonNode buckets = getPath(in, "aggregations", "group_file_extensions", "buckets");
        ArrayNode bucketArray = (ArrayNode) buckets;
        return Jive
                .stream(bucketArray)
                .map(bucketNode -> Jive.newJsonEntry(
                        bucketNode.path("key").asText(),
                        bucketNode.path("doc_count").asInt()
                ))
                .collect(JiveCollectors.toObjectNode());
    }

    @GET
    @Path("/extensions/{extension}")
    public Integer getPopularExtensions(@PathParam("extension") String extension) throws Exception{
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
        InputStream in = performQueryRequest(myQuery, "count").getEntity().getContent();
        return getPath(in, "count").intValue();
    }

    @GET
    @Path("/api/call")
    public ObjectNode getApiCalls() throws Exception {
        JsonNode myQuery = newObjectNode(
                size,
                newJsonEntry("aggs", newObjectNode(
                        newJsonEntry("group_by_api", newObjectNode(
                                newJsonEntry("terms", newObjectNode(
                                        newJsonEntry("field", "restfulAPI")
                                ))
                        ))
                ))
        );
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        JsonNode buckets = getPath(in, "aggregations", "group_by_api", "buckets");
        ArrayNode bucketArray = (ArrayNode) buckets;
        return Jive
                .stream(bucketArray)
                .map(bucketNode -> Jive.newJsonEntry(
                        bucketNode.path("key").asText(),
                        bucketNode.path("doc_count").asInt()
                ))
                .collect(JiveCollectors.toObjectNode());
    }

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
        InputStream in = performQueryRequest(myQuery, "search").getEntity().getContent();
        JsonNode buckets = getPath(in, "aggregations", "group_by_api", "buckets");
        ArrayNode bucketArray = (ArrayNode) buckets;
        return Jive
                .stream(bucketArray)
                .map(bucketNode -> Jive.newJsonEntry(
                        bucketNode.path("key").asText(),
                        bucketNode.path("doc_count").asInt()
                ))
                .collect(JiveCollectors.toObjectNode());
    }

    private Response performQueryRequest(JsonNode jsonNode, String queryType) throws Exception {
        return  restClient.performRequest(
            "GET",
            endpoint + "_" + queryType,
            Collections.emptyMap(),
            new NStringEntity(objectMapper.writeValueAsString(jsonNode))
        );
    }

    private JsonNode getPath(InputStream in, String... paths) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(in);
        for (String str : paths){
            jsonNode = jsonNode.path(str);
        }
        return jsonNode;
    }
}