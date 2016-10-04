package examples.cn.crxy.offline5.mr.pattern;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

/**
 * 输入数据全部使用SequenceFile格式，key是文件名、value是文件内容
 * @author Think
 * 
 * //以后切分的时候，使用该方法，可以按照所有的空白字符切分
   //StringTokenizer stringTokenizer = new StringTokenizer(content);
 *
 */
public class CnTFIDFApp extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CnTFIDFApp(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		if(args==null ||args.length!=3){
			System.err.println("参数必须有3个，分别是[inputPath] [tempPath]  [outputPath]");
			System.exit(-1);
		}
		String inputPath = args[0];
		String tempDir = args[1];
		Path outputDir = new Path(args[2]);
		
		runJob1(inputPath, tempDir);
		return runJob2(tempDir, outputDir);
	}
	public int runJob1(String inputPath, String outputDi) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = getConf();
		
		Path outputDir = new Path(outputDi);
		outputDir.getFileSystem(conf).delete(outputDir, true);
		
		String jobName =  CnTFIDFApp.class.getSimpleName()+"_1";
		Job job = Job.getInstance(conf , jobName );
		
		job.setJarByClass(CnTFIDFApp.class);
		
		job.addArchiveToClassPath(new Path("/jars/ik.jar"));
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(Job1Mapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(Job1Partitioner.class);
		job.setNumReduceTasks(2);
		
		job.setReducerClass(Job1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		return job.waitForCompletion(true)?0:1;
	}
	
	/**
	 * 读取SequenceFile格式，所以k1、v1的类型都是Text。此处，k1表示文件名，v1表示文件内容
	 * @author Think
	 *
	 */
	public static class Job1Mapper extends Mapper<Text, Text, Text, Text>{
		Text k2 = new Text();
		Text v2 = new Text();
		
		int docTimes = 0;
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			docTimes++;
			
			String fileName = key.toString();
			String content = value.toString();
			
	        StringReader sr=new StringReader(content);  
	        IKSegmenter ik=new IKSegmenter(sr, true);  
	        Lexeme lex=null;  
			int allWordsCount = 0;
			Map<String, Integer> wordTimesMap = new HashMap<>();
			while ((lex=ik.next())!=null) {
				allWordsCount++;
				String word = lex.getLexemeText();
				Integer times = wordTimesMap.get(word);
				if(times==null){
					wordTimesMap.put(word, 1);
				}else{
					wordTimesMap.put(word, times+1);
				}
			}

			
			for(Map.Entry<String, Integer> entry : wordTimesMap.entrySet()){
				String wordKey = entry.getKey();
				Integer timesValue = entry.getValue();
				
				System.out.print(wordKey+"\t"+timesValue+"\t"+allWordsCount);
				
				double tf = (timesValue*1.0)/allWordsCount;
				
				k2.set(wordKey);
				v2.set(fileName+":"+tf);
				
				context.write(k2, v2);
			}
		}
		
		
		@Override
		protected void cleanup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			k2.set("$$$$"+docTimes);
			v2.set("");
			context.write(k2, v2);
		}
	}
	public static class Job1Partitioner extends Partitioner<Text, Text>{
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			int v2length = value.toString().length();
			if(v2length==0){
				return 0;
			}
			return 1;
		}
	}
	public static class Job1Reducer extends Reducer<Text, Text, Text, NullWritable>{
		Text k3 = new  Text();
		NullWritable v3 = NullWritable.get();

		@Override
		protected void reduce(Text k2, Iterable<Text> v2s, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			if(k2.toString().startsWith("$$$$")){
				String docCount = k2.toString().substring(4);
				k3.set(docCount);
				context.write(k3, v3);
			}else{
				StringBuilder v2string = new StringBuilder();
				for (Text v2 : v2s) {
					v2string.append(v2.toString()).append(",");
				}
				v2string.deleteCharAt(v2string.length()-1);
				//
				k3.set(k2.toString()+"\t"+v2string.toString());
				context.write(k3, v3);
			}
		}
	}


	public int runJob2(String inputDir, Path outputDir) throws Exception{
		Configuration conf = getConf();
		outputDir.getFileSystem(conf).delete(outputDir, true);
		
		String jobName =  CnTFIDFApp.class.getSimpleName()+"_2";
		Job job = Job.getInstance(conf , jobName );
		
		job.setJarByClass(CnTFIDFApp.class);

		job.addCacheFile(new URI(inputDir+"/part-r-00000#docCount"));
		
		FileInputFormat.setInputPaths(job, inputDir+"/part-r-00001");
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(Job2Mapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(0);
		
		return job.waitForCompletion(true)?0:1;
	}
	public static class Job2Mapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		int docCount = 0;
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String readFileToString = FileUtils.readFileToString(new File("./docCount"));
			this.docCount = Integer.parseInt(readFileToString.trim());
		}
		
		Text k2 = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] splited = line.split("\t");

			String word = splited[0];
			String[] fileTFs = splited[1].split(",");
			int hasFilesCount = fileTFs.length;
		
			double idf = Math.log((docCount+1.0)/hasFilesCount);
			
			for (String fileTF : fileTFs) {
				String[] fileTFArray = fileTF.split(":");
				String fileName = fileTFArray[0];
				double tf = Double.parseDouble(fileTFArray[1]);
				
				double tfidf = idf*tf;
				k2.set(fileName+"\t"+word+"\t"+tfidf);
				
				
				context.write(k2, NullWritable.get());
			}
		}
	}
}
