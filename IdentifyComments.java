//--------------------------//
// Hassan Shahzad
// 18i-0441
// PDC A3
//--------------------------//

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IdentifyComments {
	static String keyword;
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: AvgTemperature <input path> <output path>");
      System.exit(-1);
    }
    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Identify Comments");
    job.setJarByClass(IdentifyComments.class);
    job.setJobName("Identify Comments");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path	(args[1]));
    job.setMapperClass(IdentifyCommentsMapper.class);
    job.setReducerClass(IdentifyCommentsReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
