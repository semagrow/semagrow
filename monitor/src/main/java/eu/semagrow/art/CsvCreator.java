package eu.semagrow.art;

import java.sql.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by antonis on 18/11/2015.
 */
public class CsvCreator {

    private String databaseUrl;
    private String databaseUsername;
    private String databasePassword;

    private String allQueries  = "SELECT * FROM query";
    private String totalTimes  = "SELECT * FROM evaluation";
    private String decompTimes = "SELECT * FROM decomposition";
    private String sourceTimes = "SELECT * FROM source_query";


    public CsvCreator(String databaseUrl, String databaseUsername, String databasePassword) {
        this.databaseUrl = databaseUrl;
        this.databaseUsername = databaseUsername;
        this.databasePassword = databasePassword;
    }

    public List<String> getCsv() {

        List<String> list = new LinkedList<>();
        Map<String, String> queryId = new HashMap<>();
        Map<String, String> queryString = new HashMap<>();

        list.add("\"Query\";\"QueryString\";\"Endpoint\";\"Time\"");

        Connection connection = null;
        Statement stmt = null;

        try {
            Class.forName("org.postgresql.Driver");

            connection = DriverManager.getConnection(databaseUrl, databaseUsername, databasePassword);

            stmt = connection.createStatement();

            /* All queries */

            ResultSet rs = stmt.executeQuery(allQueries);

            int i = 1;

            while (rs.next()) {
                String query_id = rs.getString("query_id");
                String query_string = rs.getString("query_string");

                queryId.put(query_id, "Q" + i);
                queryString.put(query_id, query_string);
                i++;
            }

            /* Execution Time */

            rs = stmt.executeQuery(totalTimes);

            while (rs.next()) {
                String query_id = rs.getString("query_id");
                String evaluation_time = rs.getString("evaluation_time");

                String line =   "\"" + queryId.get(query_id) + "\";" +
                                "\"" + queryString.get(query_id).replace("\"","\"\"") + "\";" +
                                "\"Total\";" +
                                "\"" + evaluation_time + "\"" ;

                list.add(line);
            }

            /* Decomposition Time */

            rs = stmt.executeQuery(decompTimes);

            while (rs.next()) {
                String query_id = rs.getString("query_id");
                String decomposition_time = rs.getString("decomposition_time");

                String line =   "\"" + queryId.get(query_id) + "\";" +
                        "\"" + queryString.get(query_id).replace("\"","\"\"") + "\";" +
                        "\"Decomposition\";" +
                        "\"" + decomposition_time + "\"" ;

                list.add(line);
            }

            /* Source Queries Time */

            rs = stmt.executeQuery(sourceTimes);

            while (rs.next()) {
                String query_id = rs.getString("query_id");
                String time = rs.getString("sum");
                String endpoint = rs.getString("endpoint");

                String line =   "\"" + queryId.get(query_id) + "\";" +
                        "\"" + queryString.get(query_id).replace("\"","\"\"") + "\";" +
                        "\"" + endpoint + "\";" +
                        "\"" + time + "\"" ;

                list.add(line);
            }


        } catch(SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();

                if (connection != null)
                    connection.close();
            } catch (SQLException e) { }
        }

        return list;
    }

    // TODO BEAUTIFY
}
