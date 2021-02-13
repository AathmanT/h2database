package binod.suman.SparkML;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KMeansJoin {
    public static void main(String[] args) {

        KMeansExample demo = new KMeansExample();
        JavaSparkContext jsc = demo.createSparkContext();

        // Create new dataset
        List<Double> double_list = new ArrayList<Double>();
        for (int i=0;i<9;i++){
            double_list.add((double) i);
        }

        // Convert List to RDD
        JavaRDD<Vector> rdd_dataset = convertListToRDD(double_list);


        // Cluster the data into two classes using KMeans
        int numClusters = 2;
        int numIterations = 20;
        KMeansModel kMeansModel = KMeans.train(rdd_dataset.rdd(), numClusters, numIterations);


        System.out.println("Cluster centers:");
        for (Vector center: kMeansModel.clusterCenters()) {
            System.out.println(" " + center);
        }

        // Get all cluster labels of all elements
        List<Integer> all_clusters_list = getAllClusterList(kMeansModel, rdd_dataset);


        // Get prediction from the model
        int pred = kMeansModel.predict(Vectors.dense(20));
        System.out.println("The pred: "+pred);
        int search_key = pred;

        // Get all elements beloging to a specific cluster
        List<Double> cluster_elements = getSpecificClusterElements(search_key, all_clusters_list)


//        // Save and load model
//        clusters.save(jsc.sc(), "target/org/apache/spark/JavaKMeansExample/KMeansModel");
//        KMeansModel sameModel = KMeansModel.load(jsc.sc(),
//                "target/org/apache/spark/JavaKMeansExample/KMeansModel");
//        // $example off$

        jsc.stop();
    }

    public JavaSparkContext createSparkContext() {
        SparkConf conf = new SparkConf().setAppName("Main")
                .setMaster("local[2]")
                .set("spark.executor.memory", "3g")
                .set("spark.driver.memory", "3g");

        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

    public JavaRDD<Vector> convertListToRDD(List<Double> double_list){

        // Convert dataset to Vector format
        List<Vector> vector_list = new ArrayList<Vector>();

        for (int i=0;i<9;i++){
            vector_list.add(Vectors.dense(double_list.get(i)));
        }

        // Print Vector list
        System.out.println(Arrays.toString(vector_list.toArray()));

        // Convert Vector list to Vector RDD
        JavaRDD<Vector> rdd_dataset = jsc.parallelize(vector_list);

        return rdd_dataset;
    }

    public List<Integer> getAllClusterList(KMeansModel kmeans_model, JavaRDD<Vector> rdd_dataset){

        // Get cluster labels of each elements
        JavaRDD<Integer> all_clusters_rdd = kMeansModel.predict(jsc.parallelize(rdd_dataset.collect()));

        List<Integer> all_clusters_list = all_clusters_rdd.collect();

        // Print cluster labels of each elements
        System.out.println(Arrays.toString(all_clusters_list.toArray()));

        return all_clusters_list;
    }

    public List<Double> getSpecificClusterElements(int search_key, List<Integer> all_clusters_list){
        int search_key = 0;

        // Collect matches
        List<Integer> matchingIndices = new ArrayList<>();
        for (int i = 0; i < all_clusters_list.size(); i++) {
            int element = all_clusters_list.get(i);

            if (search_key == element) {
                matchingIndices.add(i);
            }
        }

        // Print matching indices
        System.out.println(Arrays.toString(matchingIndices.toArray()));

        // Get values corresponding to indices
        List<Double> cluster_elements = matchingIndices.stream()
                .map(double_list::get)
                .collect(Collectors.toList());

        // Print cluster elements
        System.out.println(Arrays.toString(cluster_elements.toArray()));

        return cluster_elements;
    }
}
