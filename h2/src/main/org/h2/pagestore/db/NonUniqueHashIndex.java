/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.pagestore.db;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.SessionLocal;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexCondition;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.DataType;
import org.h2.value.Value;

import java.util.*;

/**
 * A non-unique index based on an in-memory hash map.
 *
 * @author Sergi Vladykin
 */
public class NonUniqueHashIndex extends Index {

    /**
     * The index of the indexed column.
     */
    private final int indexColumn;
    private final boolean totalOrdering;
    private Map<Value, ArrayList<Long>> rows;
    private KMeansModel kMeansModel;
    List<Double> doubleList;
//    ArrayList<Integer> int_key_list;
    ArrayList<Long> all_positions;
    JavaRDD<Vector> rdd_dataset;
    private final PageStoreTable tableData;
    private long rowCount;
    JavaSparkContext jsc;

    public NonUniqueHashIndex(PageStoreTable table, int id, String indexName,
            IndexColumn[] columns, IndexType indexType, JavaSparkContext jsc) {
        super(table, id, indexName, columns, indexType);
        Column column = columns[0].column;
        indexColumn = column.getColumnId();
        totalOrdering = DataType.hasTotalOrdering(column.getType().getValueType());
        tableData = table;
        doubleList = new ArrayList<>();
        all_positions = new ArrayList<>();
//        int_key_list = new ArrayList<>();
        this.jsc = jsc;
        reset();
    }

    private void reset() {
        rows = totalOrdering ? new HashMap<>() : new TreeMap<>(database.getCompareMode());
        rowCount = 0;
    }

    @Override
    public void truncate(SessionLocal session) {
        reset();
    }

    @Override
    public void add(SessionLocal session, Row row) {
        System.out.println("+++++++ Adding key to Nonunique hash index +++++++");
        Value key = row.getValue(indexColumn);
        Double k = key.getDouble();
        doubleList.add(k);
//        ArrayList<Long> positions = rows.get(key);
//        if (positions == null) {
//            positions = Utils.newSmallArrayList();
//            rows.put(key, positions);
//        }
        all_positions.add(row.getKey());
        rowCount++;
    }

    public void addToKmeans(SessionLocal session ){

        this.rdd_dataset = convertListToRDD(doubleList);
        // Cluster the data into two classes using KMeans
        int numClusters = 2;
        int numIterations = 20;
        this.kMeansModel = KMeans.train(rdd_dataset.rdd(), numClusters, numIterations);


        System.out.println("Cluster centers:");
        for (Vector center: kMeansModel.clusterCenters()) {
            System.out.println(" " + center);
        }
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        if (rowCount == 1) {
            // last row in table
            reset();
        } else {
            Value key = row.getValue(indexColumn);
            ArrayList<Long> positions = rows.get(key);
            if (positions.size() == 1) {
                // last row with such key
                rows.remove(key);
            } else {
                positions.remove(row.getKey());
            }
            rowCount--;
        }
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last) {
        if (first == null || last == null) {
            throw DbException.getInternalError(first + " " + last);
        }
        if (first != last) {
            if (TreeIndex.compareKeys(first, last) != 0) {
                throw DbException.getInternalError();
            }
        }
        Value v = first.getValue(indexColumn);
        /*
         * Sometimes the incoming search is a similar, but not the same type
         * e.g. the search value is INT, but the index column is LONG. In which
         * case we need to convert, otherwise the HashMap will not find the
         * result.
         */
        v = v.convertTo(tableData.getColumn(indexColumn).getType(), session);
        long t3 = System.currentTimeMillis();

        ArrayList<Long> positions = rows.get(v);
        long t4 = System.currentTimeMillis();

        System.out.println("===============================  t4-t3="+(t4-t3)+"  ================================");

        return new NonUniqueHashCursor(session, tableData, positions);
    }

    public Cursor findByKMeans(SessionLocal session, SearchRow first, SearchRow last){
        if (first == null || last == null) {
            throw DbException.getInternalError(first + " " + last);
        }
        if (first != last) {
            if (TreeIndex.compareKeys(first, last) != 0) {
                throw DbException.getInternalError();
            }
        }

        Value v = first.getValue(indexColumn);
        /*
         * Sometimes the incoming search is a similar, but not the same type
         * e.g. the search value is INT, but the index column is LONG. In which
         * case we need to convert, otherwise the HashMap will not find the
         * result.
         */
        v = v.convertTo(tableData.getColumn(indexColumn).getType(), session);


        // Get all cluster labels of all elements
        System.out.println("Printing double_list_size: "+this.doubleList.size());
        ArrayList<Integer> all_clusters_list = getAllClusterList(kMeansModel, convertListToRDD(this.doubleList));

        long t1 = System.currentTimeMillis();
        // Get prediction from the model
        int pred = kMeansModel.predict(Vectors.dense(v.getDouble()));


        System.out.println("The pred: "+pred);
        int search_key = pred;

        // Get all elements beloging to a specific cluster
        ArrayList<Long> positions = getSpecificClusterElements(search_key, all_clusters_list, all_positions);

        long t2 = System.currentTimeMillis();
        System.out.println("===============================  t2-t1="+(t2-t1)+"  ================================");


//        ArrayList<Long> positions = rows.get(v);
        return new NonUniqueHashCursor(session, tableData, positions);


    }

    @Override
    public long getRowCount(SessionLocal session) {
        return rowCount;
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return rowCount;
    }

    @Override
    public void close(SessionLocal session) {
        // nothing to do
    }

    @Override
    public void remove(SessionLocal session) {
        // nothing to do
    }

    @Override
    public double getCost(SessionLocal session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        for (Column column : columns) {
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexCondition.EQUALITY) != IndexCondition.EQUALITY) {
                return Long.MAX_VALUE;
            }
        }
        return 2;
    }

    @Override
    public boolean needRebuild() {
        return true;
    }

    @Override
    public boolean canScan() {
        return false;
    }

    public JavaRDD<Vector> convertListToRDD(List<Double> double_list){

        // Convert dataset to Vector format
        List<Vector> vector_list = new ArrayList<Vector>();

        for (int i=0;i<double_list.size();i++){
            vector_list.add(Vectors.dense(double_list.get(i)));
        }

        // Print Vector list
        System.out.println(Arrays.toString(vector_list.toArray()));

        // Convert Vector list to Vector RDD
        JavaRDD<Vector> rdd_dataset = jsc.parallelize(vector_list);

        return rdd_dataset;
    }

    public ArrayList<Integer> getAllClusterList(KMeansModel kmeans_model, JavaRDD<Vector> rdd_dataset){

        // Get cluster labels of each elements
        JavaRDD<Integer> all_clusters_rdd = kMeansModel.predict(jsc.parallelize(rdd_dataset.collect()));

        List<Integer> all_clusters_list =  all_clusters_rdd.collect();
        ArrayList<Integer> all_clusters_list1 = new ArrayList<Integer>();
        all_clusters_list1.addAll(all_clusters_list);
        // Print cluster labels of each elements
        System.out.println("Printing labels for the whole dataset");
        System.out.println(Arrays.toString(all_clusters_list.toArray()));

        return all_clusters_list1;
    }

    public ArrayList<Long> getSpecificClusterElements(int search_key, ArrayList<Integer> all_clusters_list, ArrayList<Long> all_positions){

        // Collect matches
        ArrayList<Integer> matchingIndices = new ArrayList<>();
        for (int i = 0; i < all_clusters_list.size(); i++) {
            int element = all_clusters_list.get(i);

            if (search_key == element) {
                matchingIndices.add(i);
            }
        }

        // Print matching indices
        System.out.println("Printing matching indexes");
        System.out.println(Arrays.toString(matchingIndices.toArray()));

        // Get values corresponding to indices
//        ArrayList<Long> cluster_elements = matchingIndices.stream()
//                .map(all_positions::get)
//                .collect(Collectors.toList());

        ArrayList<Long> cluster_elements = new ArrayList<Long>();
        for (int index : matchingIndices){
            cluster_elements.add(all_positions.get(index));
        }


        // Print cluster elements
        System.out.println("Printing row_id of the selected cluster elements");
        System.out.println(Arrays.toString(cluster_elements.toArray()));

        return cluster_elements;
    }

}
