package BDataP2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
import java.util.Scanner;

/**
 * Created by lsolorzano on 4/3/2017.
 */
public class TwitterKeyWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String tweet = status.getText().toUpperCase();


            //String myString = "This text, will be copied into clipboard when running this code! ";

            String[] Stopwords = { "a","an", "and", "are","as","at","be","by","for","from","has","he","in","is","it",
                    "its", "of", "on", "that", "the", "to", "was", "were", "will", "with"};
            Scanner sc1 = new Scanner(tweet);
            boolean Desition;

            while (sc1.hasNext() )
            {
                String str1 = sc1.next();
                Desition = true;
                for(int i=0;i < Stopwords.length;i++)
                {
                    //System.out.println(Stopwords[i]);
                    if ( Stopwords[i].equals(str1))
                    {
                        Desition = false;
                        break;
                    }

                }

                if(Desition)
                {
                    context.write(new Text(str1), new IntWritable(1));
                }

            }

        }
        catch(TwitterException e){

        }

    }
}
