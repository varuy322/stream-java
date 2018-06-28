package com.sdu.hadoop.mapreduce;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import java.io.IOException;
import java.util.HashMap;

import static groovy.json.JsonOutput.toJson;

/**
 * @author hanhan.zhang
 * */
public class HiveMapReducer extends Configured implements Tool {

    private static final String KEY = "key";
    private static final String COLUMNS = "columns";

    @Override
    public int run(String[] args) throws Exception {
        String jobName = args[0];
        int reduceNum = Integer.parseInt(args[1]);

        String db = args[2];
        String table = args[3];
        String filter = "dt >= \"" + args[4] + "\" and dt <= \"" + args[5] + "\"";

        String outputTable = args[6];
        String partitionKey = args[7];

        Configuration conf = getConf();

        Job job = Job.getInstance(conf, jobName);
        HCatInputFormat.setInput(job, db, table, filter);

        job.setInputFormatClass(HCatInputFormat.class);

        // 设置输入/输出格式
        job.setJarByClass(HiveMapReducer.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(DefaultHCatRecord.class);

        job.setNumReduceTasks(reduceNum);

        HashMap<String, String> partition = Maps.newHashMap();
        partition.put("dt", partitionKey);

        // 创建数据库
        HCatOutputFormat.setOutput(job, OutputJobInfo.create(db, outputTable, partition));
        HCatSchema schema = HCatOutputFormat.getTableSchema(job.getConfiguration());
        HCatOutputFormat.setSchema(job, schema);

        job.setOutputFormatClass(HCatOutputFormat.class);


        return job.waitForCompletion(true) ? 0 : -1;
    }


    public static class Map extends Mapper<WritableComparable, HCatRecord, Text, Text> {

        private HCatSchema schema;
        private String keyColumn;
        private String[] columns;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            schema = HCatInputFormat.getTableSchema(context.getConfiguration());
            if (schema == null) {
                throw new RuntimeException("Hive schema empty !!");
            }

            keyColumn = context.getConfiguration().get(KEY);
            if (keyColumn == null || keyColumn.isEmpty()) {
                throw new IllegalArgumentException("Extract key column empty !!");
            }

            String column = context.getConfiguration().get(COLUMNS);
            if (column == null || column.isEmpty()) {
                throw new IllegalArgumentException("Extract columns empty !!");
            }
            columns = column.split(",");
        }


        @Override
        protected void map(WritableComparable key, HCatRecord value, Context context) throws IOException, InterruptedException {
            String keyVal = value.getString(keyColumn, schema);

            HashMap<String, String> output = Maps.newHashMap();
            for (String column : columns) {
                output.put(column, value.getString(column, schema));
            }

            context.write(new Text(keyVal), new Text(toJson(output)));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    public static class Reduce extends Reducer<Text, Text, WritableComparable, HCatRecord> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            HCatRecord record = new DefaultHCatRecord(2);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new HiveMapReducer(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            System.exit(-1);
        }
    }
}
