package task1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.HashMap;
import java.util.Map;
import java.util.AbstractMap;

public class TopEngagedUserReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    private PriorityQueue<Map.Entry<String, Integer>> maxHeap;
    private HashMap<String, String> userFavouriteActivity;
    private Text outputValue =new Text();

    // Initialize resources that will be used during the execution of Reducer.
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        maxHeap = new PriorityQueue<>((entry1, entry2) -> Integer.compare(entry2.getValue(), entry1.getValue()));
        userFavouriteActivity = new HashMap<>();
    }

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] details = value.toString().split(",");  // Correct split syntax
            String userId = details[0];
            int activityCount = Integer.parseInt(details[1]);  // Convert activityCount to int
            String favouriteActivity = details[2];

            Map.Entry<String, Integer> userActivityEntry = new AbstractMap.SimpleEntry<>(userId, activityCount);
            maxHeap.add(userActivityEntry);  // Add user activity to maxHeap
            userFavouriteActivity.put(userId, favouriteActivity);
        }
    }

    // Called once at the end of the task
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int count = 0;
        while (!maxHeap.isEmpty() && count < 10) {
            Map.Entry<String, Integer> user = maxHeap.poll();
            //1   14566
            //1   19403
            String userIdInfo = user.getKey();
            String parts[] = userIdInfo.trim().split("\\s+");
            String userId = parts[parts.length-1];
            int activityCount = user.getValue();
            String frequentActivity = userFavouriteActivity.get(userIdInfo);

            String result = (count+1) + ") " + "User No: " + userId + ", Number of Activities: " + activityCount + ", Favourite Activity: " + frequentActivity;
            outputValue.set(result);
            context.write(NullWritable.get(), outputValue);  // Wrap the result in a Text object
            count++;
        }
    }
}
