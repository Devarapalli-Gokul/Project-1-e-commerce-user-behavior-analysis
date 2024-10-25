package task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class EngagedUsersMapper extends Mapper<Object, Text, Text, Text>{

    private Text userID = new Text();
    private Text activityType = new Text();
    private boolean isHeader = true;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

        String line =value.toString();
        
        if(isHeader){
            isHeader = false;
            return ;
        }

        String[] columns = line.split(",");

        if(columns.length == 5){
            String user = columns[1];
            String activity = columns[2];
            userID.set(user);
            activityType.set(activity);
            context.write(userID,activityType);
        }

    }
}
