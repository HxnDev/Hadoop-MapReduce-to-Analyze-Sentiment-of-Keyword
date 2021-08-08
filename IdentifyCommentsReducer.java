//--------------------------//
// Hassan Shahzad
// 18i-0441
// PDC A3
//--------------------------//

import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;  
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IdentifyCommentsReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	int row_id = 0;				// Will be used for break condition
	String keyword; 		// The keyword entered
	int overall = 0;		// If overall > 0 then positive , else negative 
	
	
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		Text key = new Text();
		if (overall > 0){
		  String stre = keyword + " is used positively overall";
		  FileWriter myWriter3 = new FileWriter("/home/hxn/hadoopMR/input/result.txt", true);
		  myWriter3.write(stre);
		  myWriter3.close();
		  key.set(keyword + " is used positively overall");
		  context.write(key, new IntWritable(overall));
		  }
	  else if (overall < 0){
		  String stre = keyword + " is used negatively overall";
		  FileWriter myWriter3 = new FileWriter("/home/hxn/hadoopMR/input/result.txt", true);
		  myWriter3.write(stre);
		  myWriter3.close();
		  key.set(keyword + " is used negatively overall");
		  context.write(key, new IntWritable(overall));
		  }
	  else if (overall == 0 ){
		  String stre = keyword + " is used neutrally overall";
		  FileWriter myWriter3 = new FileWriter("/home/hxn/hadoopMR/input/result.txt", true);
		  myWriter3.write(stre);
		  myWriter3.close();
		  key.set(keyword + " is used neutrally overall");
		  context.write(key, new IntWritable(overall));
		  }
  }
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	  row_id ++;
	  try {
		  File kw = new File ("/home/hxn/hadoopMR/input/keyword.txt");		// Getting the keyword from the file
		  Scanner myReader = new Scanner(kw);
		  while (myReader.hasNextLine()) {
			  keyword = myReader.nextLine();
		  }
		  myReader.close();
	  }catch(FileNotFoundException e) {		// If file is not found
		  System.out.println("File not found");
		  e.printStackTrace();
	  }
	  
	  String str = key.toString();			// Converting the comment into string
	  
	  if (str.contains(keyword)) {			// Checking if the comment contains that keyword
		int positive = 0;					// Number of positive words
		int negative = 0;					// Number of negative words
    	String[] words = str.split("\\W+");	// Splitting the string into array of words
    	String file = "";
		String file1 = "";
    	
		try {
			  File pos = new File ("/home/hxn/hadoopMR/input/positive.txt");		// Checking the positive file
			  Scanner myReader = new Scanner(pos);
			  while (myReader.hasNextLine()) {
				  file += myReader.nextLine() + "~";								// Splitting the positive file
			  }
			  myReader.close();
		  }catch(FileNotFoundException e) {		// If file is not found
			  System.out.println("File not found");
			  e.printStackTrace();
		  }
		
		try {
			  File neg = new File ("/home/hxn/hadoopMR/input/negative.txt");		// Checking the negative file
			  Scanner myReader = new Scanner(neg);
			  while (myReader.hasNextLine()) {
				  file1 += myReader.nextLine() + "~";								// Splitting the negative file
			  }
			  myReader.close();
		  }catch(FileNotFoundException e) {		// If file is not found
			  System.out.println("File not found");
			  e.printStackTrace();
		  }
		
		String[] files = file.split("~");		// Splitting the positive string into array of words
		String[] files1 = file1.split("~");		// Splitting the negative string into array of words
		
		
		// Checking the instances of the comment in positive file
    	for (int i=0; i< words.length; i++) {
    		for (int j=0; j<files.length; j++) {
    			if (words[i].equals(files[j])) {
        			positive++;					// Incrementing the positive instances
        		}}}
    	
    	for (int i=0; i< words.length; i++) {
    		for (int j=0; j<files1.length; j++) {
    			if (words[i].equals(files1[j])) {
    				negative++;					// Incrementing the negative instances
        		}}}
    	
    	FileWriter myWriter = new FileWriter("/home/hxn/hadoopMR/input/result.txt", true);		// Creating a resultant file
    	myWriter.write("Comment = " + str + "\n");		// Writing the comment in the file
    	myWriter.close();
    	key.set("Comment = " + str + "\nOverall = ");
		context.write(key, new IntWritable(overall));
    	if (positive > negative){
    		String str1 = "Comment Sentiment = The file is POSITIVE as number of positive words are = "+ positive;
    		FileWriter myWriter1 = new FileWriter("/home/hxn/hadoopMR/input/result.txt", true);
        	myWriter1.write(str1 + "\n\n\n");
        	myWriter1.close();
        	key.set(str1 + "\nOverall = ");
    		context.write(key, new IntWritable(overall));
    		overall += 1;
    	}
    	else if (negative > positive){
    		String str1 = "Comment Sentiment = The comment is NEGATIVE as number of negative words are = "+ negative;
    		FileWriter myWriter1 = new FileWriter("/home/hxn/hadoopMR/input/result.txt", true);
        	myWriter1.write(str1 + "\n\n\n");
        	myWriter1.close();
        	key.set(str1 + "\nOverall = ");
    		context.write(key, new IntWritable(overall));
    		overall-=1;
    	}
    	else if (negative == positive){
    		String str1 = "Comment Sentiment = The comment is NEUTRAL as number of negative and positive words are the same = "+ negative;
    		FileWriter myWriter1 = new FileWriter("/home/hxn/hadoopMR/input/result.txt", true);
        	myWriter1.write(str1+"\n\n\n");
        	myWriter1.close();
        	key.set(str1 + "\nOverall = ");
    		context.write(key, new IntWritable(overall));
        	overall+=0;
    	}}
	  
	  }}
