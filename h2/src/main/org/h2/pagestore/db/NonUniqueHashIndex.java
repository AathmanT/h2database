/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.pagestore.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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
import org.h2.util.Utils;
import org.h2.value.DataType;
import org.h2.value.Value;

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
    private List<Double> doubleList;
    private final PageStoreTable tableData;
    private long rowCount;

    public NonUniqueHashIndex(PageStoreTable table, int id, String indexName,
            IndexColumn[] columns, IndexType indexType) {
        super(table, id, indexName, columns, indexType);
        Column column = columns[0].column;
        indexColumn = column.getColumnId();
        totalOrdering = DataType.hasTotalOrdering(column.getType().getValueType());
        tableData = table;
        doubleList = new ArrayList<Double>();
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
        Value key = row.getValue(indexColumn);
        ArrayList<Long> positions = rows.get(key);
        if (positions == null) {
            positions = Utils.newSmallArrayList();
            rows.put(key, positions);
        }
        positions.add(row.getKey());
        rowCount++;
    }
    public void addToKmeans(SessionLocal session, Row row, JavaRDD<Vector> rdd_dataset, long rdd_row_count){

        // Cluster the data into two classes using KMeans
        int numClusters = 2;
        int numIterations = 20;
        kMeansModel = KMeans.train(rdd_dataset.rdd(), numClusters, numIterations);


        System.out.println("Cluster centers:");
        for (Vector center: kMeansModel.clusterCenters()) {
            System.out.println(" " + center);
        }
        rowCount = doubleList.size();
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
        ArrayList<Long> positions = rows.get(v);
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
        List<Integer> all_clusters_list = getAllClusterList(kMeansModel, rdd_dataset);

        // Get prediction from the model
        int pred = kMeansModel.predict(Vectors.dense(v));
        System.out.println("The pred: "+pred);
        int search_key = pred;

        // Get all elements beloging to a specific cluster
        List<Double> cluster_elements = getSpecificClusterElements(search_key, all_clusters_list);




        ArrayList<Long> positions = rows.get(v);
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

}
