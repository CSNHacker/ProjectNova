package finalproject.main;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.text.SequenceFilesFromDirectory;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

import com.google.common.base.Charsets;




public class App 
{
	//Working Perfectly for the following parameters for clustering drinks and countries data
/*	
    static String numItterations ="10";
    static String convergenceDelta="0.01";
    static String numClusters="2";
*/
    
	//PARAMETERS : KMEANS	
    static String numItterations ="10";
    static String convergenceDelta="0.01";
    static String numClusters="10";
    static String disMeasure="org.apache.mahout.common.distance.CosineDistanceMeasure";
    static String mdisMeasure="org.apache.mahout.common.distance.ManhattanDistanceMeasure";
    static String t1="3.1";
    static String t2="2.1";
 /*   //PARAMETERS : DUMP
    static String numWordsPrint="100";
    static String samplePoints="1000";
*/
    //PARAMETERS FOR CONSOLE OUTPUT
    static int docs=165;
    
    //DIRECTORIES
    static String parent_dir="file:/home/guangoku/Study/BDProject/textClassification";
	static String rawInputDir ="file:/home/guangoku/Study/BDProject/rawInput";
	/*
	
	static void dump_output(String kmeans_results,String vectors_dir) throws Exception
	{
		String dumpInput=kmeans_results+"/clusters-*-final";
        String dumpOutput=parent_dir+"/dump";
        String dumpPoints=kmeans_results+"/clusteredPoints";
        String dumpDictionary=vectors_dir+"/dictionary.file-0";
		ClusterDumper.main(new String[] {"--input",dumpInput.toString(), "--output", dumpOutput.toString(),"--numWords",numWordsPrint,"--pointsDir",dumpPoints.toString(),"--distanceMeasure",disMeasure,"--samplePoints",samplePoints.toString(),"--dictionary",dumpDictionary.toString(),"--dictionaryType","sequencefile","--evaluate"});
	}
	*/
	public static void main(String[] args) throws Exception{
		String rawToSeqDir = parent_dir+"/rawToSequence";
		String seqToVecDir = parent_dir+"/seqToVectors";
		String kmeansResultsDir =parent_dir+"/kmeansResults";
		String canopyResultsDir =parent_dir+"/canopyResults";
		String canopyOut = parent_dir+"/canopyResults/clusters-0";
		
		//Since main is static method of class SeqFileFromDir therefore we need to call it like class.property and dont need to create the object
		SequenceFilesFromDirectory.main(new String[] {"--input",rawInputDir.toString(), "--output", rawToSeqDir.toString(), "--chunkSize","64", "--charset",Charsets.UTF_8.name(),"--overwrite"});
		SparseVectorsFromSequenceFiles.main(new String[] {"--input",rawToSeqDir.toString(),"--output",seqToVecDir.toString(),"--maxNGramSize","3","--namedVector", "--overwrite"});

		String tfidfVecDir = seqToVecDir+"/tfidf-vectors";
	//	KMeansDriver.main(new String[] {"--input",tfidfVecDir, "--output", kmeansResultsDir, "--convergenceDelta",convergenceDelta,"--maxIter",numItterations,"--distanceMeasure",disMeasure,"--numClusters",numClusters,"--clusters",kmeansResultsDir,"--clustering","--overwrite"});
		//StreamingKMeansDriver.main(new String[] {"--input",tfidfVecDir, "--output", kmeansResultsDir, "--convergenceDelta",convergenceDelta,"--maxIter",numItterations,"--distanceMeasure",disMeasure,"--numClusters",numClusters,"--clusters",kmeansResultsDir,"--clustering","--overwrite"});
		
		

		  
		Path outPath = new Path(canopyResultsDir);
		Path inPath = new Path(tfidfVecDir);
		CanopyDriver.main(new String[] {"--input",tfidfVecDir, "--output", canopyResultsDir, "--distanceMeasure",mdisMeasure,"--t1",t1,"--t2",t2, "--overwrite"});
		KMeansDriver.main(new String[] {"--input",tfidfVecDir, "--output", canopyOut, "--convergenceDelta",convergenceDelta,"--maxIter",numItterations,"--distanceMeasure",disMeasure,"--clusters",kmeansResultsDir,"--clustering","--overwrite"});


		
		//DUMPING IN TEXTFILE (WE DONT NEED THIS)
		//dump_output(kmeansResultsDir,seqToVecDir);
	    
		
		String[] vecNames=new String[docs];
		int[] cluster=new int[docs];
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(rawToSeqDir),conf);
    	SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(kmeansResultsDir+"/clusteredPoints/part-m-00000"), conf);
		IntWritable key = new IntWritable();
		WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
		int k = 0; //NUMBER OF INPUTS OR DOCUMENTS
		while (reader.next(key, value)) {
			NamedVector vector = (NamedVector) value.getVector();
			String vectorName = vector.getName();
		    vecNames[k]=vectorName;
		    cluster[k]=(Integer.parseInt(key.toString()));
			//System.out.println(vectorName + " belongs to cluster "+key.toString());
			k++;
			if (k >=docs) {
		        break;
		    }
		}
		System.out.println("NUMBER OF FILES: "+k);
		reader.close();
		clusterPrinter myPrinter= new clusterPrinter();
		myPrinter.printConsole(cluster,vecNames,k);
	}
}


