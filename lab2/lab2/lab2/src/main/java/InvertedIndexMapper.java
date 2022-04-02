import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
/*
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        Text word = new Text();
        //Text word_fileName = new Text();
        Text fileName_lineOffset = new Text(fileName + "#" + key.toString());
        StringTokenizer itr = new StringTokenizer(value.toString());
        for(;itr.hasMoreTokens();){
            word.set(itr.nextToken());
            context.write(word, fileName_lineOffset);
        }
    }
}
*/
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        Text word = new Text();
        //Text word_fileName = new Text();
        Text fileName_lineOffset = new Text(fileName + "#" + key.toString());
        StringTokenizer itr = new StringTokenizer(value.toString());
        for(;itr.hasMoreTokens();){
            word.set(itr.nextToken());
            context.write(word, fileName_lineOffset);
        }
    }
}