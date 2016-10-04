package examples.cn.crxy.offline5.mr.sequencefile;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;

public class SequenceFileWriterApp {

	public static void main(String[] args) throws Exception {
		if(args.length!=3){
			System.err.println("参数必须是3个，分别是 inputDir  outputPath   compressionType");
			System.exit(-1);
		}
		
		String inputDir = args[0];
		Path outputPath = new Path(args[1]);
		String compType = args[2];	//压缩类型，如果值为1表示None，值为2表示Block，值为3表示Record
		
		Configuration conf = new Configuration();
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		Option[] opts = new Option[4];
		opts[0] = Writer.file(outputPath);
		opts[1] = Writer.keyClass(Text.class);
		opts[2] = Writer.valueClass(Text.class);
		CompressionType compressionType = getCompressionType(compType);
		CompressionCodec codec = new GzipCodec();
		opts[3] = Writer.compression(compressionType, codec);
		
		Writer writer = SequenceFile.createWriter(conf , opts);
		
		Text key = new Text();
		Text val = new Text();

		File[] listFiles = new File(inputDir).listFiles();
		for (File file : listFiles) {
			String name = file.getName();
			key.set(name);
			String content = FileUtils.readFileToString(file);
			val.set(content);
			writer.append(key, val);
		}
		writer.close();
	}

	public static CompressionType getCompressionType(String compType) {
		if("2".equals(compType)){
			return CompressionType.BLOCK;
		}else if("3".equals(compType)){
			return CompressionType.RECORD;
		}else{
			return CompressionType.NONE;
		}
		
	}

}
