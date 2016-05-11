package eu.semagrow.cassandra.connector;

import eu.semagrow.cassandra.utils.Utils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by antonis on 5/4/2016.
 */
public class CassandraSchema {

    private String address;
    private int port;
    private String keyspace;
    private String base;

    private Set<String> tables = new HashSet<>();
    private Map<String, Set<String>> partitionColumns = new HashMap<>();
    private Map<String, List<String>> clusteringColumns = new HashMap<>();
    private Map<String, Set<String>> regularColumns = new HashMap<>();
    private Map<String, Set<String>> indexedColumns = new HashMap<>();

    /* notice: the clustering order must be preserved in the list */

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void addTable(String table) {
        this.tables.add(table);
        this.partitionColumns.put(table, new HashSet<>());
        this.clusteringColumns.put(table, new ArrayList<>());
        this.regularColumns.put(table, new HashSet<>());
        this.indexedColumns.put(table, new HashSet<>());
    }

    public void addPartitionColumn(String table, String column) {
        this.partitionColumns.get(table).add(column);
    }

    public void addRegularColumn(String table, String column) {
        this.regularColumns.get(table).add(column);
    }

    public void addClusteringColumn(String table, String column, int position) {
        int size = this.clusteringColumns.get(table).size();
        if (position >= size) {
            for (int i=size; i<=position; i++) {
                this.clusteringColumns.get(table).add("");
            }
        }
        this.clusteringColumns.get(table).set(position, column);
    }

    public void addIndex(String table, String column) {
        this.indexedColumns.get(table).add(column);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public String getBase() {
        return this.base;
    }

    public String getAddress() {
        return this.address;
    }

    public int getPort() {
        return this.port;
    }

    public String getKeyspace() {
        return this.keyspace;
    }

    public Set<String> getTables() {
        return this.tables;
    }

    public Set<String> getPartitionColumns(String table) {
        return partitionColumns.get(table);
    }

    public List<String> getClusteringColumns(String table) {
        return clusteringColumns.get(table);
    }

    public Set<String> getRegularColumns(String table) {
        return regularColumns.get(table);
    }

    public boolean hasIndex(String table, String column) {
        return indexedColumns.get(table).contains(column);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void setCredentials(String address, int port, String keyspace) {
        this.address = address;
        this.port = port;
        this.keyspace = keyspace;
    }

    public void setBase(String base) {
        this.base = base;
    }

    public boolean canRestrictColumns(Set<String> restrictedColumns, String table) {
        if (restrictedColumns.isEmpty()) {
            return true;
        }

        Set<String> restrictedPartitionColumns = Utils.intersection(partitionColumns.get(table), restrictedColumns);
        Set<String> restricedClusteringColumns = Utils.intersection(clusteringColumns.get(table), restrictedColumns);
        Set<String> restrictedRegularColumns = Utils.intersection(regularColumns.get(table), restrictedColumns);

        return (canRestrictPartitionColumns(table, restrictedPartitionColumns) &&
                canRestrictClusteringColumns(table, restricedClusteringColumns) &&
                canRestrictRegularColumns(table, restrictedRegularColumns));
    }

    public Set<String> getNonRestrictableColumns(Set<String> restrictedColumns, String table) {
        Set<String> nonRestrictableColumns = new HashSet<>();

        Set<String> restrictedPartitionColumns = Utils.intersection(partitionColumns.get(table), restrictedColumns);
        Set<String> restricedClusteringColumns = Utils.intersection(clusteringColumns.get(table), restrictedColumns);
        Set<String> restrictedRegularColumns = Utils.intersection(regularColumns.get(table), restrictedColumns);

        if (!canRestrictPartitionColumns(table, restrictedPartitionColumns)) {
            for (String column: restrictedPartitionColumns) {
                if (!hasIndex(table,column)) {
                    nonRestrictableColumns.add(column);
                }
            }
        }
        if (!canRestrictClusteringColumns(table, restricedClusteringColumns)) {
            for (String column: restricedClusteringColumns) {
                if (!hasIndex(table,column)) {
                    nonRestrictableColumns.add(column);
                }
            }
        }
        if (!canRestrictRegularColumns(table, restrictedRegularColumns)) {
            for (String column: restrictedRegularColumns) {
                if (!hasIndex(table,column)) {
                    nonRestrictableColumns.add(column);
                }
            }
        }
        return nonRestrictableColumns;
    }

    private boolean canRestrictPartitionColumns(String table, Set<String> restrictedPartitionColumns) {

        /* check partition key colums: one of the two condition must be hold:
         * 1) either all partition key columns must be bound or
         * 2) none of the partition key columns can be bound (unless the column has a secondary index) */

        Set<String> partitionColumns = this.getPartitionColumns(table);

        return (restrictedPartitionColumns.isEmpty() ||
                restrictedPartitionColumns.equals(partitionColumns) ||
                restrictedPartitionColumns.stream()
                        .allMatch(column -> this.hasIndex(table, column))
        );
    }

    private boolean canRestrictClusteringColumns(String table, Set<String> restrictedClusteringColumns) {

        /* check clustering columns: if a clustering column is restricted, all preceding clustering columns
         * w.r.t the clustering order must be restricted too. if not, all columns must have an index */

        List<String> clusteringColumns = this.getClusteringColumns(table);
        List<Boolean> restrictions = clusteringColumns.stream()
                .map(restrictedClusteringColumns::contains)
                .collect(Collectors.toList());

        return (restrictedClusteringColumns.isEmpty() ||
                restrictions.indexOf(false) == -1 ||
                restrictions.lastIndexOf(true) < restrictions.indexOf(false) ||
                clusteringColumns.stream()
                        .allMatch(column -> this.hasIndex(table, column))
        );
    }

    private boolean canRestrictRegularColumns(String table, Set<String> restrictedRegularColumns) {

        /* check regular columns: no regular column must be restricted (unless it has a secondary intex). */

        return (restrictedRegularColumns.isEmpty() ||
                restrictedRegularColumns.stream()
                        .allMatch(column -> this.hasIndex(table, column))
        );
    }
}