中文分词器使用：
maven依赖是
		<dependency>
			<groupId>com.janeluo</groupId>
			<artifactId>ikanalyzer</artifactId>
			<version>2012_u6</version>
		</dependency>
示例代码如下：
import java.io.IOException;
import java.io.StringReader;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class IKAnalyzerUtil {
	public static void main(String[] args) throws IOException {
        
		String text="基于java语言开发的轻量级的中文分词工具包";  
        StringReader sr=new StringReader(text);  
        IKSegmenter ik=new IKSegmenter(sr, true);  
        Lexeme lex=null;  
        while((lex=ik.next())!=null){  
            System.out.print(lex.getLexemeText()+"|");  
        }  
	}
}

配置文件IKAnalyzer.cfg.xml和stopwords-cn.dic一定放到maven的resources目录下。

yarn jar jar/sequenceFileWriterApp.jar babu/ /members.seq 0  
yarn jar jar/cntfidf.jar /members.seq /out /out1
yarn jar jar/hotTopicsApp.jar /out1 /out2 为升序
或者yarn jar jar/hotTopicsApp.jar -Dtopk=10 -Dtype=max /out1/out2 为取前十个热词
