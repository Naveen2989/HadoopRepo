package com.ibm.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ExcelParser {

		
	private StringBuilder currentString = new StringBuilder();
	private long bytesRead = 0;
	
	public String parseExcellData(InputStream is) throws ClassNotFoundException{
		
		try{
			//Create Workbook instance of .xlsx file
			System.out.print("Printting is : " + is);
			XSSFWorkbook workbook = new XSSFWorkbook(is);
			//HSSFWorkbook workbook = new HSSFWorkbook(is);
			//Get first sheet from the workbook
			XSSFSheet sheet = workbook.getSheetAt(0);
			//HSSFSheet sheet = workbook.getSheetAt(0);
			
			//Iterate through each rows one by one
			Iterator<Row> rowIterator = sheet.iterator();
			
			while(rowIterator.hasNext()){
				
				// For each row, iterate through all the columns
				Iterator<Cell> cellIterator = rowIterator.next().cellIterator();
				
				while(cellIterator.hasNext()){
					Cell cell = cellIterator.next();
					
					
					//Check the cell type and format accordingly
					switch(cell.getCellType()){
					
					case Cell.CELL_TYPE_BOOLEAN:
						bytesRead++;
						currentString.append(cell.getBooleanCellValue() +  "\t");
						break;
					case Cell.CELL_TYPE_NUMERIC:
						bytesRead++;
						currentString.append(cell.getNumericCellValue() + "\t");
						break;
						
					case Cell.CELL_TYPE_STRING:
						bytesRead++;
						currentString.append(cell.getStringCellValue() + "\t");
						break;					
					}
				} 
				currentString.append("\n");				
			} 
			
			is.close();			
		}catch(IOException ie){
			System.out.println("IOException - FileNotFound" + ie);
		}
		System.out.println("curretString \n" + currentString);
		return currentString.toString();		
		
	}
	
	public long bytesRead(){
		return bytesRead;
	}
	
	
}
