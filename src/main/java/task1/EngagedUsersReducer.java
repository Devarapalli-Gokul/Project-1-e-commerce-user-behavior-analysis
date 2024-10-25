package task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class EngagedUsersReducer extends Reducer<Text, Text, IntWritable, Text> {

    private IntWritable outputKey = new IntWritable(1);
    private Text outputValue =new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int activityCount = 0;
        int maxActivityCount = 0;
        String favouriteActivity = null;
        
        // Declare and initialize activityCountMap
        HashMap<String, Integer> activityCountMap = new HashMap<>();
        
        // Count activities and find the most frequent activity
        for (Text value : values) {
            String activity = value.toString();
            activityCountMap.put(activity, activityCountMap.getOrDefault(activity, 0) + 1);
            activityCount++;

            if (activityCountMap.get(activity) > maxActivityCount) {
                favouriteActivity = activity;
                maxActivityCount = activityCountMap.get(activity);
            }
        }

        // Output result in the format "userID, activityCount, favouriteActivity"
        String result = key.toString() + "," + activityCount + "," + favouriteActivity;
        outputValue.set(result);
        // Use a Text object to write the result
        context.write(outputKey, outputValue);
    }
}
