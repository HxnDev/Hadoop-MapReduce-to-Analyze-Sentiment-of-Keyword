//--------------------------//
// Hassan Shahzad
// 18i-0441
// PDC A3
//--------------------------//

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IdentifyCommentsMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {
@Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	
    String line = value.toString();		// Reads a complete line
    int r_index = 0;					// Stores index of the character "=" of the word "Row id ="
    String id = "";						// Stores the id 
    int c_index = 0;					// Stores index of the character "T" of the word "Text"
    String comment = "";				// Stores the comment 

    for (int i=0; i< line.length(); i++){	// Getting the starting index for row id
    	char c = line.charAt(i);
    	if (c == '='){
    		r_index = i+2;
    		break;
    	}
    }
    for (int i = r_index; i< line.length(); i++){	// Getting the ending index for row id
    	char c = line.charAt(i);
    	if (c == '"')
    		break;
    	else
    		id = id + "" + c;	// Adding the id
    }
    
    for (int i=0; i< line.length(); i++){	// Getting the starting index for comment
    	char c = line.charAt(i);
    	if (c == 'T'){
    		c_index = i+6;
    		break;
    	}
    }
    for (int i = c_index; i< line.length(); i++){	// Getting the ending index for comment
    	char c = line.charAt(i);
    	if (c == '"')
    		break;
    	else
    		comment = comment + c ;								// Adding the comment
    }
    int id1 = Integer.parseInt(id);		// Converting the row id into integer

    
    context.write(new Text(comment), new IntWritable(id1));

} }
