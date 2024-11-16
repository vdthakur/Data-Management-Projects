/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SQL2MR {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, FloatWritable> {
    
    private Text outputKey = new Text();
    private FloatWritable outputValue = new FloatWritable();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	    String[] columns = value.toString().split(",");
      if (columns.length > 10) {
        int length = Integer.parseInt(columns[8]);
        if (length >= 60) {
          String rating = columns[10];
          float replacementCost = Float.parseFloat(columns[9]);
          outputKey.set(rating);
          outputValue.set(replacementCost);
          context.write(outputKey, outputValue);}}
    }
  }
  
  public static class FloatAvgReducer 
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      float sum = 0;
      int count = 0;
      for (FloatWritable val : values) {sum += val.get();count++;}
      if (count >= 160) {
        float average = sum / count;
        result.set(average);
        context.write(key, result);}
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: sql2mr <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(SQL2MR.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(FloatAvgReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

