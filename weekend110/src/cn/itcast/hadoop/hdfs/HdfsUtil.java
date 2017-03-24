package cn.itcast.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsUtil {
	
	FileSystem fs = null;

	
	/**
	 * 这个init函数是做一些的输出花的操作。必须放在这个类当中的所有的函数的最前面去使用
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception{
		
		//读取classpath下的xxx-site.xml 配置文件，并解析其内容，封装到conf对象中
		Configuration conf = new Configuration();
		
		fs=FileSystem.get(conf);
		
		//也可以在代码中对conf中的配置信息进行手动设置，会覆盖掉配置文件中的读取的值
//		conf.set("fs.defaultFS", "hdfs://hadoop01:9000/");
//		
//		//根据配置信息，去获取一个具体文件系统的客户端操作实例对象
//		//Get a filesystem instance based on the uri, the passed configuration and the user
//		fs = FileSystem.get(new URI("hdfs://weekend110:9000/"),conf,"hadoop");
		
		
	}
	
	
	
	/**
	 * 上传文件，比较底层的写法。
	 * 这种方法是老师不调用HDFS的API，自己实现的方法
	 * @throws Exception
	 */
	@Test
	public void upload() throws Exception {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://weekend110:9000/");
		
		//rq：先得到一个文件系统的所有的父类的对象。之后使用这个conf函数，根据提取到的HDFS的文件系统对象，得到文件系统的一个对象
		FileSystem fs = FileSystem.get(conf);
		
		Path dst = new Path("hdfs://weekend110:9000/aa/qingshu.txt"); //读取到的数据要传到hdfs上面
		
		FSDataOutputStream os = fs.create(dst);//创建这个文件输出流的文件的输出的位置
		
		FileInputStream is = new FileInputStream("c:/qingshu.txt"); //定位到读取文件的数据流的位置。这个程序是在Windows的系统下写的，所以有c盘目录
		
		IOUtils.copy(is, os); //进行输出流和输入流的互导
		

	}

	/**
	 * 上传文件，封装好的写法
	 * @throws Exception
	 * @throws IOException
	 */
	@Test
	public void upload2() throws Exception, IOException{
		
		fs.copyFromLocalFile(new Path("c:/qingshu.txt"), new Path("hdfs://weekend110:9000/aaa/bbb/ccc/qingshu2.txt"));
		
	}
	
	
	/**
	 * 下载文件
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void download() throws Exception {
		
		fs.copyToLocalFile(new Path("hdfs://weekend110:9000/aa/qingshu2.txt"), new Path("c:/qingshu2.txt"));

	}

	
	
	/**
	 * 查看文件信息
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	 * 
	 */
	@Test
	public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {

		// listFiles列出的是文件信息，而且提供递归遍历
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);//将
		
		while(files.hasNext()){//定位到这个数组的下一个的位置
			
			//LocatedFileStatus 这个类当中记录HDFS当中的一个文件，在哪些块儿当中。备份是多少，之类的
			LocatedFileStatus file = files.next();
			
			//这个path当中隐含着文件的路径的一系列的信息
			Path filePath = file.getPath(); // hdfs://hadoop01:9000/input/0A0_05084327_0908071334_7741.ps.raw.txt
			String fileName = filePath.getName();// 0A0_05084327_0908071334_7741.ps.raw.txt
			System.out.println(fileName);
			
		}
		
		System.out.println("---------------------------------");
		
		//listStatus 可以列出文件和文件夹的信息，但是不提供自带的递归遍历
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for(FileStatus status: listStatus){
			
			String name = status.getPath().getName();//Path.getName()
			System.out.println(name + (status.isDirectory()?" is dir":" is file"));
			
		}
		
	}

	
	
	/**
	 * 创建文件夹
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void mkdir() throws IllegalArgumentException, Exception {

		fs.mkdirs(new Path("/aaa/bbb/ccc"));
		
		
	}

	/**
	 * 删除文件或文件夹
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void rm() throws IllegalArgumentException, IOException {

		fs.delete(new Path("/aa"), true);
		
	}

	
	public static void main(String[] args) throws Exception {

		//---------------------这是将文件从HDFS上面下载到本地的系统上面的实例-------------------------
//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://weekend110:9000/");
//		
//		FileSystem fs = FileSystem.get(conf);
//		
//		FSDataInputStream is = fs.open(new Path("/jdk-7u65-linux-i586.tar.gz"));
//		
//		FileOutputStream os = new FileOutputStream("c:/jdk7.tgz");
		
//		IOUtils.copy(is, os);
		/*------------------------------------------------------------------------------------------*/
		HdfsUtil str=new HdfsUtil();
		str.init();
		str.listFiles();
	}
	
	
	
}