package mapreduce;

import io.dropwizard.jackson.Jackson;
import model.DBEntry;
import org.elasticsearch.hadoop.util.BytesArray;
import parser.EntryParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * This class is the mapper class for hadoop which will map a key value pair
 * ready to be inserted into ES when the map job is run
 * Created by awaldman on 6/5/17.
 */
class EventMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private final ObjectMapper mapper = Jackson.newObjectMapper();

    EventMapper() {
    }

    protected void map(LongWritable key,
                       Text value,
                       Mapper<LongWritable,
                               Text,
                               NullWritable,
                               Text>.Context context)
            throws IOException, InterruptedException {

        // from the line of input parse out an entry
        List<DBEntry> dbEntry = EntryParser.parse(value.toString());

        // iterates once getting entry and readying it for es insertion
        for (DBEntry entry: dbEntry) {
            String output = mapper.writeValueAsString(entry);
            context.write(NullWritable.get(), new Text(output));
        }
    }
}
