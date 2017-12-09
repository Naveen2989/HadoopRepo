package com.ibm.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class ExcelMapper extends Mapper<LongWritable, Text, Text, Text>{

		
	/**
	* Excel Spreadsheet is supplied in string form to the mapper.
	* We are simply emitting them for viewing on HDFS.
	*/
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		//System.out.println(line);
		
		String outerText = new String();		
		String[] wordsArray = line.split("\t");	
		
		for(int i = 0; i <= wordsArray.length -1; i++){			
			
			char[] charArray = wordsArray[i].toCharArray();
			int count = 0;
			
			for(int j=0; j <= charArray.length -1; j++){  // Alternate: for(char c : charArray)
				System.out.println(charArray[j]);
				String innerText = new String();
						
				switch(charArray[j]){
				case 'a':
				case 'e':
		        case 'i':
		        case 'o':
		        case 'u':
		        case 'A':
				case 'E':
		        case 'I':
		        case 'O':
		        case 'U':
		        count++;
		         break;
		         default:
				}				
			}
			outerText += wordsArray[i].toString() + " " + count + "\n";
					
		}
		
		System.out.println(outerText.toString());
		context.write(new Text(outerText.toString()), null);		
	}
	
}
