import java.lang.*;
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
	public int hashCode() {
		Short i = new Short(this.type);
		int z = i.hashCode();	
		return z;
	}
	
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

	public static class HistogramCombiner extends Reducer<Color,IntWritable,Color,IntWritable> {
		public void reduce ( Color key, Iterable<IntWritable> values, Context context )
		throws IOException, InterruptedException {
			
			int sum = 0;
			for(IntWritable v: values)
			{
				sum =sum+v.get();
					
			}
			context.write(key,new IntWritable(sum));
			
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


	public static class HistogramInMapper extends Mapper<Object,Text,Color,IntWritable> {
		
		public Hashtable<Color,Integer> H;
		
		@Override
		protected void setup ( Context context ) throws IOException,InterruptedException {
			H = new Hashtable<Color,Integer>();
		}
		
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
			if (H.get(r) == null)
				H.put(r,1);
			else H.put(r,(int)H.get(r)+1);
        	Color g = new Color((short) 2,green);
			if (H.get(g) == null)
				H.put(g,1);
			else H.put(g,(int)H.get(g)+1);
        	Color b = new Color((short) 3,blue);
			if (H.get(b) == null)
				H.put(b,1);
			else H.put(b,(int)H.get(b)+1);
        	s.close();
        }
		
		@Override
		protected void cleanup ( Context context ) throws IOException,InterruptedException {
			Set<Color> keys = H.keySet();
			for (Color key: keys)
				context.write(key,new IntWritable((int)H.get(key)));
		}
		
		
    }

    public static class HistogramInReducer extends Reducer<Color,IntWritable,Color,LongWritable> {
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
        job.setJobName("MyJob1");
        job.setJarByClass(Histogram.class);
        job.setOutputKeyClass(Color.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Color.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(HistogramMapper.class);
		job.setCombinerClass(HistogramCombiner.class);
        job.setReducerClass(HistogramReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
		job = Job.getInstance();
        job.setJobName("MyJob2");
        job.setJarByClass(Histogram.class);
        job.setOutputKeyClass(Color.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Color.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(HistogramInMapper.class);
        job.setReducerClass(HistogramInReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"2"));
		job.waitForCompletion(true);
   }
}
