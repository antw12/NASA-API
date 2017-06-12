package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.mr.EsOutputFormat;


/**
 * Used to take in nasa data to then me mapped and placed into elastic search
 * in the same format as the API would do it or the database handler
 * Created by awaldman on 5/31/17.
 */
public class NasaHadoop extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new NasaHadoop(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        conf.set("es.nodes", "localhost:9200");
        conf.set("es.resource", "nasa/log");
//        conf.set("es.input.json", "yes");

        Job job = Job.getInstance(conf);

        // configure the input and output types, and the class types
        job.setMapperClass(EventMapper.class);
        job.setInputFormatClass(TextInputFormat.class); //default
        job.setOutputKeyClass(NullWritable.class );
        job.setOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path("usask_access_log_3000"));

        // return the job status as a status code
        return job.waitForCompletion(true)?0:1;
    }
}
