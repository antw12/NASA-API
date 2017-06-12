package com.andy.nasa.event;

import model.DBEntry;
import parser.EntryParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.RestClient;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.zackehh.jackson.Jive.newJsonEntry;
import static com.zackehh.jackson.Jive.newObjectNode;

/**
 * This class will write to ES all the entries that are coming in from the client
 * Created by awaldman on 5/10/17.
 */
public class DatabaseHandler {

    // ES rest client
    private final RestClient restClient;

    // json object mapper
    private final ObjectMapper objectMapper = Jackson.newObjectMapper();

    // string builder used for bulk API syntax
    private StringBuilder bulkDoc;

    // to make sure count can only be updated by one resource at a time
    private AtomicInteger count = new AtomicInteger(0);

    /**
     * Constructor for DatabaseHandler in which has a fixed scheduler
     * that will perform flush every 5 seconds
     * @param restClient storing given rest client locally
     */
    public DatabaseHandler(RestClient restClient) {
        this.restClient = restClient;
        resetBulkDoc();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        // else nothing until there's stuff there
        scheduler.scheduleAtFixedRate(this::flush, 5, 5, TimeUnit.SECONDS);

        // send mappings for the data

    }

    /**
     * This is the write to DB method which will write the data
     * to ES local instance
     * @param entryPayload the entry to be wrote to the DB
     * @throws Exception Jackson process exception
     */
    public void writeToDB(String entryPayload) throws Exception {
        List<DBEntry> nasaData = EntryParser.parse(entryPayload);

        for (DBEntry entry: nasaData) {
            // this sets up the first part of the ES
            // bulk api syntax
            JsonNode index = newObjectNode(
                newJsonEntry("index", newObjectNode(
                    newJsonEntry("_id", entry.entryID())
                ))
            );

            // string builder used to build correct bulk API syntax
            bulkDoc.append(objectMapper.writeValueAsString(index));
            bulkDoc.append("\n");
            bulkDoc.append(objectMapper.writeValueAsString(entry));
            bulkDoc.append("\n");
            count.incrementAndGet();

            // perform 1 http request per 1000 entries
            if (count.get() > 1000) {
                System.out.println("1000 entries sent");
                flush();
                System.out.println("1000 done");
            }
        }
    }

    /**
     * This is the flush method in which will perform the bulk API call
     * and write the documents, in bulk, to ES instance
     */
    private synchronized void flush() {
        //used to make sure no 404 bad requests happen due to no data being there
        if (count.get() > 0) {
            try {
                // the ES rest client for the HTTP bulk API request
                restClient.performRequest(
                        "POST",
                        "/nasa/log/_bulk",
                        Collections.emptyMap(),
                        new NStringEntity(bulkDoc.toString(), ContentType.APPLICATION_JSON)
                );
                resetBulkDoc();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //else do thing
    }

    /**
     * this will reset the variables used to check
     * and send data to ES making sure that nothing
     * is missed and all data is controlled
     */
    private void resetBulkDoc() {
        this.bulkDoc = new StringBuilder();
        this.count = new AtomicInteger(0);
    }
}
