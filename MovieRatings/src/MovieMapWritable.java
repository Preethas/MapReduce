import java.util.Iterator;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;


public class MovieMapWritable extends MapWritable {
  
 MovieMapWritable(){
		super();
 }
 
  public String toString(){
	  Iterator<Writable> it = this.keySet().iterator();
	   StringBuilder strBuilder = new StringBuilder();
	    while (it.hasNext()) {
	        Writable w = it.next();
	        Writable val = this.get(w);
	        strBuilder.append(w.toString() + " = " + val.toString());
	    }
	   return strBuilder.toString(); 
  }
}
