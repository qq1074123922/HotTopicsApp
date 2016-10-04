package examples.cn.crxy.offline5.mr.pattern;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HotTopicsApp extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		//该判断方法不能位于main()中，只能位于run()方法中。否则，不会加载-D参数
		if(args==null ||args.length!=2){
			System.err.println("参数必须有2个，分别是[inputPath]   [outputPath]");
			System.exit(-1);
		}
		String inputPath = args[0];
		
		Path outputDir = new Path(args[1]);
		

		Configuration conf = getConf();
		outputDir.getFileSystem(conf).delete(outputDir, true);
		
		String jobName =  HotTopicsApp.class.getSimpleName();
		Job job = Job.getInstance(conf , jobName );
		
		job.setJarByClass(HotTopicsApp.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(HotTopicsMapper.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(HotTopicsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		boolean waitForCompletion = job.waitForCompletion(true);
		return waitForCompletion?0:-1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new HotTopicsApp(), args);
	}

	
	
	public static class HotTopicsMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
		TreeSet<TFIDFWord> topkSet = null;
		int k = 1;
		String type = null;
		@Override
		protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			this.k = conf.getInt("topk", 1);
			this.type = conf.get("type", "min");
			if("min".equals(this.type)){
				topkSet = new TreeSet<>();
			}else {	
				topkSet = new TreeSet<>(new Comparator<TFIDFWord>() {
					@Override
					public int compare(TFIDFWord o1, TFIDFWord o2) {
						return -o1.compareTo(o2);
					}
				});
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] splited = line.split("\t");
			topkSet.add(new TFIDFWord(Double.parseDouble(splited[2]), splited[1]));
			if(topkSet.size()>k){
				topkSet.pollLast();
			}
		}
		
		@Override
		protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			for (TFIDFWord v : topkSet) {
				NullWritable k2 = NullWritable.get();
				Text v2 = new Text(v.toString());
				context.write(k2, v2);
			}
		}
	}
	
	public static class TFIDFWord implements Comparable<TFIDFWord>{
		public double itfidf;
		public String word;
		
		public TFIDFWord(String line) {
			String[] splited = line.split("\t");
			this.itfidf = Double.parseDouble(splited[0]);
			this.word = splited[1];
		}

		public TFIDFWord(double itfidf, String word) {
			super();
			this.itfidf = itfidf;
			this.word = word;
		}
		@Override
		public int compareTo(TFIDFWord o) {
			double val = itfidf-o.itfidf;
			if(val<0)return -1;
			if(val>0)return 1;
			return 0;
		}
		@Override
		public String toString() {
			return itfidf + "\t" + word;
		}
		
	}
	
	
	public static class HotTopicsReducer extends Reducer<NullWritable, Text, Text, NullWritable>{
		TreeSet<TFIDFWord> topkSet = null;
		int k = 1;
		String type = null;
		
		@Override
		protected void setup(
				Reducer<NullWritable, Text, org.apache.hadoop.io.Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			this.k = conf.getInt("topk", 1);
			this.type = conf.get("type", "min");
			if("min".equals(this.type)){
				topkSet = new TreeSet<>();
			}else {	
				topkSet = new TreeSet<>(new Comparator<TFIDFWord>() {
					@Override
					public int compare(TFIDFWord o1, TFIDFWord o2) {
						return -o1.compareTo(o2);
					}
				});
			}
		}
		
		
		Text k3 = new Text();
		@Override
		protected void reduce(NullWritable k2, Iterable<Text> v2s,
				Reducer<NullWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (Text v2 : v2s) {
				String line = v2.toString();
				topkSet.add(new TFIDFWord(line));
				if(topkSet.size()>k){
					topkSet.pollLast();
				}
			}
			
			for (TFIDFWord v : topkSet) {
				k3.set(v.toString());
				context.write(k3, NullWritable.get());
			}
		}
	}
}
