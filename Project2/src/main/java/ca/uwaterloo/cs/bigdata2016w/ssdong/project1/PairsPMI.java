package ca.uwaterloo.cs.bigdata2016w.ssdong.project2;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfStrings;

/**
* Pointwise Mutual Information pairs implementation in MapReduce for word counting
* Formula PMI(x,y) = log(p(x,y)/p(x)p(y))
*/

public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  public PairsPMI() {}

  // Implements In-mapper combiner
  private static class PairMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private static HashMap<PairOfStrings, Integer> counter = new HashMap<PairOfStrings, Integer>();
    private static PairOfStrings PAIR = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
      StringTokenizer iterator = new StringTokenizer(value.toString());

      Set<String> uniqueWordSet = new HashSet<String>();
      while (iterator.hasMoreTokens()) {
        String tunningStr = iterator.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (tunningStr.isEmpty()) continue;
        uniqueWordSet.add(tunningStr);
      }

      String[] words = uniqueWordSet.toArray(new String[uniqueWordSet.size()]);
      for (int i = 0; i < words.length; i++) {
        for (int j = i + 1; j < words.length; j++) {
          String left = words[i], right = words[j];
          if (left.compareTo(right) < 0) {
            PAIR.set(left, right);
          } else {
            PAIR.set(right, left);
          }
          if (counter.containsKey(PAIR)) {
            counter.put(PAIR, counter.get(PAIR) + 1);
          } else {
            counter.put(new PairOfStrings(PAIR.getLeftElement(), PAIR.getRightElement()), 1);
          }
        }
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable count = new IntWritable();

      for (Map.Entry<PairOfStrings, Integer> entry : counter.entrySet()) {
        count.set(entry.getValue());
        context.write(entry.getKey(), count);
      }
    }
  }

  private static class PairReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 5;
  }

  @Override
  public int run(String[] argv) throws Exception {
    Args arguments = new Args();
    CmdLineParser parser = new CmdLineParser(arguments, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + arguments.input);
    LOG.info(" - output path: " + arguments.output);
    LOG.info(" - number of reducers: " + arguments.numReducers);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(arguments.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    Job job = Job.getInstance(getConf());
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);

    TextInputFormat.setInputPaths(job, new Path(arguments.input));
    TextOutputFormat.setOutputPath(job, new Path(arguments.output));

    job.setMapperClass(PairMapper.class);
    job.setReducerClass(PairReducer.class);

    job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputValueClass(IntWritable.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}