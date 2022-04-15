import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

public class TFIDFCalculator {
    private static class TFIDFMapper  extends Mapper<Object, Text, Text, IntWritable> {
        private Text k = new Text();
        private IntWritable v = new IntWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                hashMap.put(word, hashMap.getOrDefault(word, 0) + 1);
            }
            for (String item : hashMap.keySet()) {
                k.set(fileName + "," + item);
                v.set(hashMap.get(item));
                context.write(k, v);
            }
        }
    }

    private static class TFIDFPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String word = key.toString().split(",")[1];
            return super.getPartition(new Text(word), value, numReduceTasks);
        }
    }

    private static class TFIDFReducer extends Reducer<Text, IntWritable, Text, Text> {
        private String prev = null;
        private StringBuilder stringBuilder = new StringBuilder();
        private Integer fileNumber = 0;
        private int frequency = 0;
        private double IDF;
        final private DecimalFormat decimalFormat = new DecimalFormat(".000");
        private Text k = new Text();
        private Text v = new Text();
        private Configuration conf;
        private Integer srcfileCounts;

        @Override
        protected void setup(Context context){
            conf = context.getConfiguration();
            srcfileCounts = Integer.parseInt(conf.get("srcfileCounts"));
        }
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws java.io.IOException, java.lang.InterruptedException {
            String word = key.toString().split(",")[1];
            if (prev != null && !prev.equals(word)) {
                stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
                IDF = Math.log(srcfileCounts / (fileNumber + 1));
                for(String s : TF_IDF(stringBuilder, IDF, prev)){
                    k.set(s);
                    v.set("");
                    context.write(k, v);
                }
                stringBuilder.delete(0, stringBuilder.length());
                fileNumber = 0;
            }
            prev = word;
            fileNumber ++;
            frequency = 0;
            for (IntWritable value : values)
                frequency += value.get();
            stringBuilder.append(key.toString().split(",")[0]);
            stringBuilder.append(":");
            stringBuilder.append(frequency);
            stringBuilder.append("; ");
        }

        @Override
        protected void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context) throws java.io.IOException, java.lang.InterruptedException {
            stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
            IDF = Math.log(srcfileCounts / (fileNumber + 1));
            for(String s : TF_IDF(stringBuilder, IDF, prev)){
                k.set(s);
                v.set("");
                context.write(k, v);
            }
        }

        private final List<String> TF_IDF(StringBuilder v, double IDF, String word){
            List<String> res = new LinkedList<>();
            HashMap<String, Integer> wordfile_freq_map = new HashMap<>();
            String[] values = v.toString().split("; ");
            for(String s : values){
                String fileName = s.split(":")[0];
                String frequency = s.split(":")[1];
                wordfile_freq_map.put(fileName.substring(0, fileName.indexOf('-')),
                        wordfile_freq_map.getOrDefault(fileName.substring(0, fileName.indexOf('-')), 0) + Integer.parseInt(frequency));
                //res.add(fileName + "," + word + "," + decimalFormat.format(IDF * Double.parseDouble(frequency)));
            }
            for(String key : wordfile_freq_map.keySet())
                res.add(key + "," + word + ","
                        + decimalFormat.format(IDF * wordfile_freq_map.get(key)));
            return res;
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem fileSystem = FileSystem.get(conf);
            Integer srcFileNum = fileSystem.listStatus(new Path(args[0])).length;
            conf.set("srcfileCounts", srcFileNum.toString());
            Job job = new Job(conf, "TF_IDF");
            job.setJarByClass(TFIDFCalculator.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(TFIDFMapper.class);
            job.setReducerClass(TFIDFReducer.class);
            job.setPartitionerClass(TFIDFPartitioner.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
