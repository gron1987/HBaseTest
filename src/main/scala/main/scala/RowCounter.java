package main.scala;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: vlogvinskiy
 * Date: 11/26/13
 * Time: 3:03 PM
 */
public class RowCounter {
    /** Name of this 'program'. */
    static final String NAME = "rowcounter";
    static int cnt = 0;

    static class RowCounterMapper extends TableMapper<Text, IntWritable> {
        /** Counter enumeration to count the actual rows. */
        public static enum Counters {ROWS}

        private final IntWritable ONE = new IntWritable(1);
        private Text text = new Text();

        @Override
        public void map(ImmutableBytesWritable row, Result values,
                        Context context)
                throws IOException, InterruptedException {
            text.set("TMP");     // we can only emit Writables...

            context.write(text, ONE);
        }
    }

    static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        public static final byte[] CF = "cf".getBytes();
        public static final byte[] COUNT = "count".getBytes();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                cnt += val.get();
            }
        }
    }

    public static Job createSubmittableJob(Configuration conf)
            throws IOException {
        String tableName = "mytable";
        Job job = new Job(conf, NAME + "_" + tableName);
        job.setJarByClass(RowCounter.class);
        // Columns are space delimited
        StringBuilder sb = new StringBuilder();
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        if (sb.length() > 0) {
            for (String columnName :sb.toString().split(" ")) {
                String [] fields = columnName.split(":");
                if(fields.length == 1) {
                    scan.addFamily(Bytes.toBytes(fields[0]));
                } else {
                    scan.addColumn(Bytes.toBytes(fields[0]), Bytes.toBytes(fields[1]));
                }
            }
        }
        // Second argument is the table name.
        job.setOutputFormatClass(NullOutputFormat.class);
        TableMapReduceUtil.initTableMapperJob(tableName, scan, RowCounterMapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(
                "mytable",        // output table
                MyTableReducer.class,    // reducer class
                job);
        job.setNumReduceTasks(10);
        return job;
    }

    public static int run(Configuration conf) throws Exception{
        Job job = createSubmittableJob(conf);
        job.waitForCompletion(true);
        return cnt;

    }
}
