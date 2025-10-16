package org.molej;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class ParkVisitStats extends Configured implements Tool {


    public enum VisitCounters {
        MAPPER_ERROR,
        COMBINER_ERROR,
        REDUCER_ERROR,
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ParkVisitStats(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "ParkVisitStats");
        job.setJarByClass(this.getClass());

        // Mapper & Reducer
        job.setMapperClass(ParkVisitsMapper.class);
        job.setReducerClass(ParkVisitsReducer.class);

        // Combiner for less data transfer between phases
        job.setCombinerClass(ParkVisitsCombiner.class);

        // Mapper io
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer io
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ParkVisitsMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final Logger LOG = LoggerFactory.getLogger(ParkVisitsMapper.class);

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            try {
                // Skip header
                if (offset.equals(new LongWritable(0))) {
                    return;
                }

                String line = lineText.toString();
                String[] fields = line.split(",");

                // 1. Extract required fields
                String parkId = fields[1];
                String entryTime = fields[3];
                String exitTime = fields[4];
                String groupSize = fields[5];

                // 2. Process dates to get unique days visit (if visit spans 2 days, create 2 entries with given group size)
                Set<String> visitDates = Stream.of(entryTime, exitTime)
                        .map(time -> time.substring(0, 10))
                        .collect(Collectors.toSet());

                // 3. Create value: "1, group_size" (visit_count, total_visitors)
                outValue.set("1," + groupSize);

                for (String visitDate : visitDates) {
                    // 4. Create composite key: "park_id,date"
                    outKey.set(parkId + "," + visitDate);

                    // 5. Emit the key-value pair
                    context.write(outKey, outValue);
                }
            }  catch (Exception e) {

                context.getCounter(VisitCounters.MAPPER_ERROR).increment(1);

                LOG.error("Failed to process line: {}", lineText.toString(), e);
            }

        }

    }

    public static class ParkVisitsCombiner extends Reducer<Text, Text, Text, Text> {

        private static final Logger LOG = LoggerFactory.getLogger(ParkVisitsCombiner.class);
        private final Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                long totalVisitorCount = 0;
                long totalVisitCount = 0;

                for (Text val : values) {
                    String[] fields = val.toString().split(",");
                    if (fields.length == 2) {
                        totalVisitCount += Long.parseLong(fields[0]);
                        totalVisitorCount += Long.parseLong(fields[1]);
                    } else {
                        throw new Exception("Invalid format");
                    }
                }

                // Emit the pre-aggregated sum in the same "visit_count,visitor_count" format
                result.set(totalVisitCount + "," + totalVisitorCount);
                context.write(key, result);
            }   catch (Exception e) {
                context.getCounter(VisitCounters.COMBINER_ERROR).increment(1);

                LOG.error("Failed to process key: {}", key.toString(), e);
            }
        }
    }

    public static class ParkVisitsReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        private static final Logger LOG = LoggerFactory.getLogger(ParkVisitsReducer.class);
        private final DoubleWritable resultValue = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            try {
                long totalVisitorCount = 0;
                long totalVisitCount = 0;

                // 1. Iterate through all values for key
                for (Text val : values) {
                    String[] fields = val.toString().split(",");
                    if (fields.length == 2) {
                        totalVisitCount += Long.parseLong(fields[0]);
                        totalVisitorCount += Long.parseLong(fields[1]);
                    } else {
                        throw new Exception("Invalid format");
                    }
                }

                // Technically impossible ?
                if (totalVisitCount == 0) {
                    return;
                }

                // 2. Calculate average group size for key
                double avgGroupSize = (double) totalVisitorCount / totalVisitCount;

                String[] key_fields = key.toString().split(",");
                String parkId = key_fields[0];
                String date = key_fields[1];

                // 3. Return result
                Text resultKeyAvg = new Text("Average group size at " + parkId + " on the day " + date + " was: ");
                resultValue.set(avgGroupSize);
                context.write(resultKeyAvg, resultValue);

                Text resultKeyTotal = new Text("Total amount of visits at " + parkId + " on the day " + date + " was: ");
                resultValue.set(totalVisitCount);
                context.write(resultKeyTotal, resultValue);

            } catch (Exception e) {
                context.getCounter(VisitCounters.REDUCER_ERROR).increment(1);

                LOG.error("Failed to process key: {}", key.toString(), e);
            }
        }
    }
}