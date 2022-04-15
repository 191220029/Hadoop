import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.StringTokenizer;

public class SortedCounter {
    private static class SortedCountComparator extends WritableComparator {
        private SortedCountComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) throws NumberFormatException {
            Double m = Double.parseDouble(a.toString());
            Double n = Double.parseDouble(b.toString());
            return n.compareTo(m);
        }
    }

    private static class SortedCountMapper extends Mapper<Object, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws  java.io.IOException, java.lang.InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String[] s = itr.nextToken().split("\t");
                k.set(s[1]);
                v.set(s[0]);
            }
            context.write(k, v);
        }
    }

    private static class SortedCountReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws  java.io.IOException, java.lang.InterruptedException {
            for(Text value : values) {
                context.write(value, key);
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "sort count");
            job.setJarByClass(SortedCounter.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setSortComparatorClass(SortedCountComparator.class);
            job.setMapperClass(SortedCountMapper.class);
            job.setReducerClass(SortedCountReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
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
