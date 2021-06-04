package org.semagrow.connector.postgis.execution;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.jooq.ConnectionProvider;
import org.jooq.exception.DataAccessException;

public class PostGISConnectionProvider implements ConnectionProvider {

	private Connection database = null;
    String url;
    String username;
    String password;

    public PostGISConnectionProvider(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }
	
	@Override
	public Connection acquire() throws DataAccessException {
		try {
            database = DriverManager.getConnection(url, username, password);
            return database;
        }
        catch (java.sql.SQLException ex) {
            throw new DataAccessException("Error getting connection from data source", ex);
        }
	}

	@Override
	public void release(Connection connection) throws DataAccessException {
        try {
            connection.close();
            connection = null;
        }
        catch (SQLException e) {
            throw new DataAccessException("Error closing connection " + connection, e);
        }
	}

}
