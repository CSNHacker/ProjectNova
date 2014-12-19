package finalproject.main;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class mySequenceWriter {
	/*
	//private static final String[] DATA={"His palms all sweaty","His weak arms are heavy","vomit on his sweater already"};
	public static List<Vector> getPoints(double[][] raw) 
	{
		List<Vector> points = new ArrayList<Vector>();
		for (int i = 0; i < raw.length; i++) {
			double[] fr = raw[i];
		//	myPrint(raw);
			Vector vec = new RandomAccessSparseVector(fr.length);
			vec.assign(fr);
			points.add(vec);
		}
	    return points;
	}
	public static void writetoFile(List<Vector> points,String url) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url),conf);
	    Path path=new Path(url);
	    SequenceFile.Writer SeqWriter = SequenceFile.createWriter(fs, conf,path, LongWritable.class, VectorWritable.class);
           long recNum = 0;
           VectorWritable vec = new VectorWritable();
           for (Vector tmpPoint : points) {
               vec.set(tmpPoint);
               SeqWriter.append(new LongWritable(recNum++), vec);
           }
           SeqWriter.close();
	}
	public static void myPrint(List<Vector> x)
	{
		for (int i=0;i<x.size();i++)
		{
			for (int j=0;j<(x.get(i)).size();j++)
			{
				System.out.print(x.get(i).get(j));
				System.out.print(" , ");
			}	
			System.out.println(" ");
		}
	}
	public static void RunKmeans(String seqFile_path,String work_dir) throws Exception
	{
		boolean runClustering=true;
		boolean runSequential=true;
		double convergenceDelta=1.0;
        int itt =10;
		double clusterClassificationThreshold=1.0;
		
		Configuration conf = new Configuration();
		String measureClass = SquaredEuclideanDistanceMeasure.class.getName();
		DistanceMeasure measure = ClassUtils.instantiateAs(measureClass, DistanceMeasure.class);
	    
		// Creates New Folder called Intial-Cluster which will store the Intial cluster inside the work dir.
		Path clusters = new Path(work_dir, "Intial-Cluster");   
		
	    // In the /work_dir/Intial-Cluster/ it creates a first cluster randomly using RandomSeedGenerator.
		clusters = RandomSeedGenerator.buildRandom(conf, new Path(seqFile_path),clusters,2,measure); 
	   
		// To Run KMeans we pass the path of the sequenceFile, Path of the Intial Clusters and the Path of the Work Directory 
		KMeansDriver.run(conf,new Path(seqFile_path) , clusters, new Path(work_dir), convergenceDelta,itt, true, 0.0, false);
	    
		// Finally to Dump Our Cluster we get hold of the path for the final cluster
	    Path outGlob = new Path(work_dir, "clusters-*-final");
	     //And the Centroids
	    Path clusteredPoints = new Path(work_dir,"clusteredPoints");
	    
	    //Dump Using Cluster Dumper
	    ClusterDumper dumper = new ClusterDumper(outGlob, clusteredPoints);
	    String[] input_print= null;
	    dumper.printClusters(input_print);
	    
	    
	}    
	public static void main(String[] args) throws Exception
	{
		//double[][] data = { {1, 1}, {2, 1}, {1, 2},{2, 2}, {3, 3}, {8, 8},{9, 8}, {8, 9}, {9, 9}};
		double[][] data = { {1, 1,1}, {2, 1,3}, {1, 2,4},{0, 2,0}, {3, 3,4}, {8, 8,10},{9, 8,12}, {8, 9,10}, {9, 9,10}};
		String sequenceFile ="file:/home/hduser/Desktop/finalProject/FirstRun/seqFile.seq";
		String workDirectory="file:/home/hduser/Desktop/finalProject/FirstRun/";
		
		List<Vector> temp=null;
		temp=getPoints(data);
	    myPrint(temp);
	    writetoFile(temp,sequenceFile);
	    
	   RunKmeans(sequenceFile,workDirectory);
	}*/

}
