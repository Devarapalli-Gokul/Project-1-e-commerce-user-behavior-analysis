package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EngagedUsersDriverClass {

    public static void main(String[] args) throws Exception {
        // Check that enough arguments are provided
        if (args.length != 3) {
            System.err.println("Usage: EngagedUsersDriverClass <input path> <output base path> <temp output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // ===== First Job: Count Activities per User =====
        Job job1 = Job.getInstance(conf, "Count Activities per User");
        job1.setJarByClass(EngagedUsersDriverClass.class);

        // Set Mapper and Reducer for the first job
        job1.setMapperClass(EngagedUsersMapper.class); // Use EngagedUsersMapper
        job1.setReducerClass(EngagedUsersReducer.class); // Use EngagedUsersReducer

        // Set output key and value types for the first job
        job1.setOutputKeyClass(Text.class);  // Change to Text, as emitted by the mapper
        job1.setOutputValueClass(Text.class);

        // Set input and output paths for the first job
        FileInputFormat.addInputPath(job1, new Path(args[1]));  // Input path (from args[1])
        FileOutputFormat.setOutputPath(job1, new Path(args[2] + "/tempOutput"));  // Temp output for intermediate results

        // Wait for the first job to complete
        boolean job1Success = job1.waitForCompletion(true);

        if (!job1Success) {
            System.exit(1);  // Exit if the first job fails
        }

        // ===== Second Job: Find Top 10 Engaged Users =====
        Job job2 = Job.getInstance(conf, "Top 10 Engaged Users");
        job2.setJarByClass(EngagedUsersDriverClass.class);

        // Use IdentityNullMapper for the second job
        job2.setMapperClass(IdentityNullMapper.class); // Use IdentityNullMapper here
        job2.setReducerClass(TopEngagedUserReducer.class);

        // Set output key and value types for the second job
        job2.setOutputKeyClass(NullWritable.class); // NullWritable for output key
        job2.setOutputValueClass(Text.class);       // Text for output value

        // Set input from the first job's output and final output path
        FileInputFormat.addInputPath(job2, new Path(args[2] + "/tempOutput"));  // Input path from first job's output
        FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/finalOutput"));  // Final output path (from args[2])

        // Submit second job and wait for completion
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
