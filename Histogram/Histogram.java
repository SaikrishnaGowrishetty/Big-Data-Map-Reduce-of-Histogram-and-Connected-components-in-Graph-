import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


/* single color intensity */
class Color implements WritableComparable {
    public short type;       /* red=1, green=2, blue=3 */
    public short intensity;  /* between 0 and 255 */
    
    Color(){}
    
    Color(short type,short intensity)
    {
    	this.type = type;
		this.intensity = intensity;
    }
	public void write ( DataOutput out ) throws IOException {
        out.writeShort(type);
        out.writeShort(intensity);
    }

    public void readFields ( DataInput in ) throws IOException {
        type = in.readShort();
        intensity = in.readShort();
    }
	
	public String toString () { return type+" "+intensity; }

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub

	       Color other = (Color) o;
	       Short t1 = new Short(type);
	       Short t2 = new Short(other.type);
	       Short i1 = new Short(intensity);
	       Short i2 = new Short(other.intensity);
	       int cmp = t1.compareTo(t2);
	       if(cmp!=0)
	    	   return cmp;
	       return i1.compareTo(i2);
	}
	
    /* need class constructors, toString, write, readFields, and compareTo methods */
}


public class Histogram {
    public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            /* write your mapper code */
			short red,green,blue;
        	Scanner s = new Scanner(value.toString()).useDelimiter(",");
        	red = s.nextShort();
        	green = s.nextShort();
        	blue = s.nextShort();
        	Color r = new Color((short) 1,red);
        	Color g = new Color((short) 2,green);
        	Color b = new Color((short) 3,blue);
			context.write(r,new IntWritable(1));
			context.write(g,new IntWritable(1));
			context.write(b,new IntWritable(1));
        	s.close();
        }
    }

    public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> {
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            /* write your reducer code */
			int sum = 0;
			for(IntWritable v: values)
			{
				sum =sum+v.get();
					
			}
			context.write(key,new LongWritable(sum));
        }
    }

    public static void main ( String[] args ) throws Exception {
        /* write your main program code */
		Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Histogram.class);
        job.setOutputKeyClass(Color.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Color.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(HistogramMapper.class);
        job.setReducerClass(HistogramReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
   }
}
