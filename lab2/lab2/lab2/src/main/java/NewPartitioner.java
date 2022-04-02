import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.HashPartitioner;

public class NewPartitioner extends HashPartitioner<Text, Text> {
    /*@Override
    public int getPartition(Text key, Text value, int numReduceTasks){

    }*/
}
