import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Iterator;
/*
public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
        Iterator<Text> it = values.iterator();
        StringBuilder all = new StringBuilder();
        if(it.hasNext())
            all.append(it.next().toString());
        for(;it.hasNext();){
            all.append(";");
            all.append(it.next().toString());
        }
        context.write(key, new Text(all.toString()));
    }
}
*/

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        Integer srcfileCounts = Integer.parseInt(conf.get("srcfileCounts"));
        Double sum_frequency = new Double(0);

        Iterator<Text> it = values.iterator();
        StringBuilder all = new StringBuilder();
        StringBuilder curFileName = new StringBuilder();
        Integer frequency = new Integer(0);
        if(it.hasNext()) {
            //inc_file_frequency(file_frequency, it.next().toString());
            curFileName.append(it.next().toString());
            all.append(curFileName.toString() + ':');
            frequency++;
        }
        for(;it.hasNext();){
            //inc_file_frequency(file_frequency, it.next().toString());
            StringBuilder nextFileName = new StringBuilder(it.next().toString());
            if(curFileName.toString().equals(nextFileName.toString()))
                frequency++;
            else {
                sum_frequency += frequency;
                all.append(frequency.toString() + "," + nextFileName + ':');
                curFileName = new StringBuilder(nextFileName);
                frequency = 1;
            }
        }
        sum_frequency += frequency;
        all.append(frequency.toString());
        //context.write(key, new Text(print_file_frequency(file_frequency)));

        sum_frequency /= new Double(srcfileCounts);
        DecimalFormat decimalFormat = new DecimalFormat("0.000");
        Text key_avgf = new Text(key.toString() + '\t' + decimalFormat.format(sum_frequency));
        //Text key_avgf = new Text(key.toString() + '\t' + sum_frequency.toString());
        context.write(key_avgf, new Text(all.toString()));
    }
}
