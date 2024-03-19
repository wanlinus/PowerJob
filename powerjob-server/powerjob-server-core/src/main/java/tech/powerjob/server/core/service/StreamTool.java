package tech.powerjob.server.core.service;

import java.io.*;

/**
 * <p>Title: StreamTool.java</p>
 * <p>Description: SM3相关流处理工具类</p>
 * <p>Copyright: Copyright (c) 2015-2020
 * <p>Company: 深圳云天励飞技术有限公司</p>
 * @author Peng Cheng
 * @version 1.0 创建时间：2021年4月6日 下午4:59:03
 */
public class StreamTool {
	public static byte[] readInputStream2ByteArray(InputStream inStream) throws IOException{
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int len=0;
		while ((len=inStream.read(buffer))!=-1){
			outStream.write(buffer,0,len);
		}
		return outStream.toByteArray();
	}
	public static String readInputStream2String(InputStream inStream) throws IOException{
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int len=0;
		while ((len=inStream.read(buffer))!=-1){
			outStream.write(buffer,0,len);
		}
		return outStream.toString();
	}
	public static File readInputStream2File(InputStream inStream ,File file) throws IOException{
		@SuppressWarnings("resource")
		FileOutputStream outStream = new FileOutputStream(file);
		byte[] buffer = new byte[1024];
		int len=0;
		while ((len=inStream.read(buffer))!=-1){
			outStream.write(buffer,0,len);
		}
		return file;
	}
	public static File readInputStream2File(InputStream inStream ,String filepath , String key) throws IOException{
		File file = File.createTempFile(filepath, key);
		return readInputStream2File(inStream ,file);
		
	}
}
