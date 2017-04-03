package BDataP2;

/**
 * Created by lsolorzano on 4/3/2017.
 */


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by manuel on 3/6/17.
 */
public class TwitterKeyWorkReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;

        for (IntWritable value : values){
            count += value.get();
        }
        context.write(key, new IntWritable(count));

    }
}