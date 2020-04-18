import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    /* ... */
	Vertex(short tag,long group,long VID,Vector<Long> adjacent) 
	{
	this.tag=tag;
	this.group=group;
	this.VID=VID;
	this.adjacent=adjacent;
	}
	Vertex(tag,group)
	{
	this.tag=tag;
	this.group=group;
	}
}

public class Graph {

    /* ... */
	public static class Mapper1 extends Mapper<Object,Text,LongWritable,Vertex> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            String s = value.toString();
			String[] sArray=s.split(",");
            //int x = s.nextInt();
            //double y = s.nextDouble();
			long mVID=Long.parseLong(sArray[0]);
			Vector<Long> madjacent;
			for(int i=1;i<sArray.length;i++) 
			{
    		Long madjacentVertex=Long.parseLong(sArray[1]);
    		madjacent.addElement(madjacentVertex);
			}
            context.write(new LongWritable(mVID),new Vertex(0,mVID,mVID,madjacent));
            s.close();
        }
    }
	
	public static class Reducer1 extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                           throws IOException, InterruptedException {
            for (Vertex v: values) {
			context.write(key,v);
			}
            
        }
    }
	
     public static class Mapper2 extends Mapper<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
            context.write(value.VID,value);
			for(LongWritable n : value.adjacent )
			{
            context.write(new LongWritable(n), new Vertex(1,value.group));
            }
        }
    }

	public static class Reducer2 extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                           throws IOException, InterruptedException {
			long m=Long.MAX_VALUE;
			Vector<Long> adj;
            for (Vertex v: values) {
			if(v.tag==0)
			{
			adj=v.adjacent.clone();
			 m = min(m,v.group);
			}
			context.write(m,new Vertex(0,m,key,adj)	);
			}
            
        }
    }
	
	public static class Mapper3 extends Mapper<LongWritable,Vertex,LongWritable,IntWritable> {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
            context.write(key,new IntWritable(1));
        }
    }
	
	public static class Reducer3 extends Reducer<LongWritable,IntWritable,LongWritable,IntWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
			int m=0;
            for (IntWritable v: values) {
			m=m+v;
	        } 
			context.write(key,new IntWritable(m));
			
            
        }
    }
	
    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        /* ... First Map-Reduce job to read the graph */
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
	    job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/f0"));
        job.waitForCompletion(true);
        for ( short i = 0; i < 5; i++ ) {
            job = Job.getInstance();
            /* ... Second Map-Reduce job to propagate the group number */
			job.setJobName("MyJob1");
			job.setJarByClass(Graph.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);
            job.setMapperClass(Mapper2.class);
            job.setReducerClass(Reducer2.class);
	        job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job,new Path( args[1]+"/f"+i));
            FileOutputFormat.setOutputPath(job,new Path(args[1]+"/f"+(i+1)));
            job.waitForCompletion(true);
        }
        job = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the connected component sizes */
	    job.setJobName("MyJob2");
		job.setJarByClass(Graph.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(Mapper3.class);
        job.setReducerClass(Reducer3.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path( args[1]+"/f5"));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}
