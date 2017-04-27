package com.andy.nasa.resource;

import com.andy.nasa.model.DBEntry;
import com.andy.nasa.parser.EntryParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.zackehh.jackson.Jive;
import io.dropwizard.jackson.Jackson;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Collections;
import java.util.List;
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
	public void writeToDB(String entryPayload) throws Exception{
		List<DBEntry> nasaData = EntryParser.parse(entryPayload);

		for (DBEntry entry: nasaData) {
			String jsonString = objectMapper.writeValueAsString(entry);
			//write to db as json
			String endpoint = "/nasa/log/" + entry.entryID();
			restClient.performRequest(
				"PUT",
				endpoint,
				Collections.emptyMap(),
				new NStringEntity(jsonString)
			);
		}
	}

	@GET
	@Path("/top-five-users")
	public List<String> topFiveUsers() throws Exception{
		String endpoint = "/nasa/log/_search";
		JsonNode myQuery = newObjectNode(
			newJsonEntry("size", 0),
			newJsonEntry("aggs", newObjectNode(
				newJsonEntry("group_by_username", newObjectNode(
					newJsonEntry("terms", newObjectNode(
						newJsonEntry("field", "username"),
						newJsonEntry("size", 5)
					))
				))
			))
		);
		String queryString = objectMapper.writeValueAsString(myQuery);

		Response response = restClient.performRequest(
			"GET",
			endpoint,
			Collections.emptyMap(),
			new NStringEntity(queryString)
		);
		JsonNode jsonNode = objectMapper.readTree(response.getEntity().getContent());
		JsonNode buckets = jsonNode
				.path("aggregations")
				.path("group_by_username")
				.path("buckets");
		ArrayNode bucketArray = (ArrayNode) buckets;

		return Jive
				.stream(bucketArray)
				.map(bucketNode -> bucketNode.path("key").asText())
				.collect(Collectors.toList());
	}
}
