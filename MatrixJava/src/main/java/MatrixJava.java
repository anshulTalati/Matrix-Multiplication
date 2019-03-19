"""
@author: anshulTalati
"""
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Element implements Writable {
    public int tag;
	public int index_r;
    public double value;
	
	Element () {}

    Element (int t,int r, double v ) {
        tag=t; 
        index_r = r;
        value = v;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(tag);
		out.writeInt(index_r);
        out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag= in.readInt();
		index_r = in.readInt();
        value = in.readDouble();
    }
    public String toString () { return Double.toString(value); }
    
}

class Result implements Writable {
	public double matrix_value;

    Result (Double mid_val) {
        matrix_value = mid_val;
    }
	Result(){}

    public void write ( DataOutput out ) throws IOException {
		out.writeDouble(matrix_value);
    }

    public void readFields ( DataInput in ) throws IOException {
      matrix_value = in.readDouble();
    }

    public String toString () { return Double.toString(matrix_value); }
}

class Pair implements WritableComparable<Pair>{
	public int i;
	public int j;

    Pair (int m,int n) {
        i = m;  j = n;
    }
	Pair(){}

    public void write ( DataOutput out ) throws IOException {
		out.writeInt(i);
		out.writeInt(j);
    }

    public void readFields ( DataInput in ) throws IOException {
        i = in.readInt();
		j = in.readInt();
    }

    public String toString () { return i+" "+j; }
	@Override
	public int compareTo(Pair p)
	{
		if (i==p.i)
		{
			if (j == p.j)
				return 0;
			else if(j < p.j)
				return -1;
			else
				return 1;
		}
		else if(i<p.i)
			return -1;
		else 
			return 1;
	}
}

public class MatrixJava{
	public static class ElementM extends Mapper<Object,Text,IntWritable,Element > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int row_m = s.nextInt();
			int col_m = s.nextInt();
			double value_M = s.nextDouble();
            Element Element_M = new Element(0,row_m,value_M);
			String keys = Integer.toString(col_m);
            context.write(new IntWritable(col_m),Element_M);
            s.close();
        }
    }

    public static class ElementN extends Mapper<Object,Text,IntWritable,Element > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int row_n = s.nextInt();
			int col_n = s.nextInt();
			double value_n = s.nextDouble();
            Element Element_N = new Element(1,col_n,value_n);
			String keys = Integer.toString(row_n);
            context.write(new IntWritable(row_n),Element_N);
            s.close();
        }
    }

    public static class FirstReducer extends Reducer<IntWritable,Element,Pair,Result> {
        static Vector<Element> M_matrix = new Vector<Element>();
        static Vector<Element> N_matrix = new Vector<Element>();
        @Override
        public void reduce ( IntWritable key, Iterable<Element> values, Context context )
                           throws IOException, InterruptedException {
        	M_matrix.clear();
        	N_matrix.clear();
            for (Element v: values)
                if (v.tag == 0){
					Element m = new Element(0,v.index_r,v.value);
					M_matrix.add(m);
				}
                    
                else {
					Element n = new Element(1,v.index_r,v.value);
					N_matrix.add(n);
				}
				
		 for (Element m:M_matrix)
		{
		 for( Element n : N_matrix)
		{	Pair p=new Pair(m.index_r,n.index_r);			
			context.write(p,new Result(m.value*n.value));					}
	}				
        }
    }
	
	public static class InterMapper extends Mapper<Object,Text,Pair,Result > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
        	StringTokenizer s = new StringTokenizer(value.toString());
        	while (s.hasMoreTokens()) {
        		int rown = Integer.parseInt(s.nextToken());
    			int coln = Integer.parseInt(s.nextToken());
    			double prodValue = Double.parseDouble(s.nextToken());
                Pair mn = new Pair(rown,coln);
                Result res = new Result(prodValue);
    			context.write(mn,res);
        	}
			        }
    }

    public static class SecondReducer extends Reducer<Pair,Result,Text,DoubleWritable> {
       
        public void reduce ( Pair key, Iterable<Result> values, Context context )
                           throws IOException, InterruptedException {
            
			double sum= 0.0;
            for (Result v: values){
					sum= sum + v.matrix_value;
			}
			String final_key= Integer.toString(key.i)+","+Integer.toString(key.j);
			context.write(new Text(final_key),new DoubleWritable(sum));
		}
    }
	
        public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("Matrix Multiplication");
        
        //setting the main class
        job.setJarByClass(MatrixJava.class);
        
        //setting Mapper output
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Element.class);
        
        //setting the inputs
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,ElementM.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,ElementN.class);
        
        //setting the 1st reducer
        job.setReducerClass(FirstReducer.class);        
        
        //setting the key, values class for output
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(Result.class);       
        
        //setting the output 
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
		
        //it only returns when the job is over
		job.waitForCompletion(true);
        
		//setting the job 2
		Job job2 = Job.getInstance();
		job2.setJobName("Final Values");
		//setting the main class
		job2.setJarByClass(MatrixJava.class);
		
		//setting the key and value class for output
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		//setting the Mapper
		job2.setMapperClass(InterMapper.class);
		
		//setting the key and value class for Mapper output
		job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(Result.class);
		
        //setting the Reducer
        job2.setReducerClass(SecondReducer.class);
		
        //setting the Text format
        job2.setOutputFormatClass(TextOutputFormat.class);
		
        //input to 2nd reducer
        FileInputFormat.addInputPath(job2, new Path(args[2]));
		
        //output to 2nd reducer
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
		
        //it only returns when the job is over
		job2.waitForCompletion(true);
    }
}
