package com.andy.nasa.event;

import com.andy.nasa.model.DBEntry;
import com.andy.nasa.parser.EntryParser;
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

    private final RestClient restClient;
    private final ObjectMapper objectMapper = Jackson.newObjectMapper();
    private StringBuilder bulkDoc;
    private AtomicInteger count = new AtomicInteger(0);


    public DatabaseHandler(RestClient restClient) {
        this.restClient = restClient;
        resetBulkDoc();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        // else nothing until there's stuff there
        scheduler.scheduleAtFixedRate(this::flush, 5, 5, TimeUnit.SECONDS);
    }

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

            bulkDoc.append(objectMapper.writeValueAsString(index));
            bulkDoc.append("\n");
            bulkDoc.append(objectMapper.writeValueAsString(entry));
            bulkDoc.append("\n");
            count.incrementAndGet();
            if (count.get() > 1000) {
                System.out.println("1000 entries sent");
                flush();
                System.out.println("1000 done");
            }
        }
    }

    private synchronized void flush() {
        if (count.get() > 0) {
            try {
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

    private void resetBulkDoc() {
        this.bulkDoc = new StringBuilder();
        this.count = new AtomicInteger(0);
    }
}
