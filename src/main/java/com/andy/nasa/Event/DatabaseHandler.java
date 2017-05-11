package com.andy.nasa.Event;

import com.andy.nasa.model.DBEntry;
import com.andy.nasa.parser.EntryParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.RestClient;

import java.util.Collections;
import java.util.List;

/**
 * This class will write to ES all the entries that are coming in from the client
 * Created by awaldman on 5/10/17.
 */
public class DatabaseHandler {

    private final RestClient restClient;
    private final ObjectMapper objectMapper = Jackson.newObjectMapper();
    private final String endpoint = "/nasa/log/";

    public DatabaseHandler(RestClient restClient) {
        this.restClient = restClient;
    }

    public void writeToDB(String entryPayload) throws Exception {
//        System.out.println(entryPayload);
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

}
