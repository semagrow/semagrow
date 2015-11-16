package eu.semagrow.art;

import ch.qos.logback.classic.db.DBHelper;
import ch.qos.logback.classic.db.names.ColumnName;
import ch.qos.logback.classic.db.names.DBNameResolver;
import ch.qos.logback.classic.db.names.TableName;
import ch.qos.logback.classic.spi.ILoggingEvent;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by angel on 11/11/2015.
 */
public class DbAppender extends ch.qos.logback.classic.db.DBAppender {

    static final int TIMESTMP_INDEX = 1;
    static final int  FORMATTED_MESSAGE_INDEX  = 2;
    static final int  LOGGER_NAME_INDEX = 3;
    static final int  LEVEL_STRING_INDEX = 4;
    static final int  THREAD_NAME_INDEX = 5;
    static final int  REFERENCE_FLAG_INDEX = 6;
    static final int  ARG0_INDEX = 7;
    static final int  ARG1_INDEX = 8;
    static final int  ARG2_INDEX = 9;
    static final int  ARG3_INDEX = 10;
    static final int  CALLER_FILENAME_INDEX = 11;
    static final int  CALLER_CLASS_INDEX = 12;
    static final int  CALLER_METHOD_INDEX = 13;
    static final int  CALLER_LINE_INDEX = 14;
    static final int  EVENT_ID_INDEX  = 15;

    static final int  QUERY_ID_INDEX = 11;
    static final int  NESTING_INDEX = 12;

    @Override
    protected String getInsertSQL() {
        return insertSQL;
    }

    @Override
    public void start() {
        insertSQL = buildInsertSQL();
        super.start();
        insertSQL = buildInsertSQL();

    }

    @Override
    protected void subAppend(ILoggingEvent event, Connection connection, PreparedStatement insertStatement) throws Throwable {

        bindLoggingEventWithInsertStatement(insertStatement, event);
        bindDiagnosticContext(insertStatement, event);

        // bindCallerDataWithPreparedStatement(insertStatement, event.getCallerData());

        int updateCount = insertStatement.executeUpdate();
        if (updateCount != 1) {
            addWarn("Failed to insert loggingEvent");
        }
    }

    @Override
    protected void secondarySubAppend(ILoggingEvent iLoggingEvent, Connection connection, long eventId) throws Throwable {

    }

    void bindLoggingEventWithInsertStatement(PreparedStatement stmt,
                                             ILoggingEvent event) throws SQLException
    {
        stmt.setLong(TIMESTMP_INDEX, event.getTimeStamp());
        stmt.setString(FORMATTED_MESSAGE_INDEX, event.getFormattedMessage());
        stmt.setString(LOGGER_NAME_INDEX, event.getLoggerName());
        stmt.setString(LEVEL_STRING_INDEX, event.getLevel().toString());
        stmt.setString(THREAD_NAME_INDEX, event.getThreadName());
        stmt.setShort(REFERENCE_FLAG_INDEX, DBHelper.computeReferenceMask(event));

        bindLoggingEventArgumentsWithPreparedStatement(stmt, event.getArgumentArray());
    }


    void bindLoggingEventArgumentsWithPreparedStatement(PreparedStatement stmt,
                                                        Object[] argArray) throws SQLException {

        int arrayLen = argArray != null ? argArray.length : 0;

        for(int i = 0; i < arrayLen && i < 4; i++) {
            stmt.setString(ARG0_INDEX+i, asStringTruncatedTo254(argArray[i]));
        }
        if(arrayLen < 4) {
            for(int i = arrayLen; i < 4; i++) {
                stmt.setString(ARG0_INDEX+i, null);
            }
        }
    }

    String asStringTruncatedTo254(Object o) {
        String s = null;
        if(o != null) {
            s= o.toString();
        }

        if(s == null) {
            return null;
        }
        if(s.length() <= 254) {
            return s;
        } else {
            return s.substring(0, 254);
        }
    }


    void bindDiagnosticContext(PreparedStatement stmt,
                               ILoggingEvent event) throws SQLException
    {
        Map<String,String> properties = mergePropertyMaps(event);

        stmt.setString(QUERY_ID_INDEX, properties.getOrDefault("uuid", null));

        stmt.setString(NESTING_INDEX, properties.getOrDefault("nestingLevel", null));
    }



    static String buildInsertSQL() {
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
        sqlBuilder.append("logging_event").append(" (");
        sqlBuilder.append("timestmp").append(", ");
        sqlBuilder.append("formatted_message").append(", ");
        sqlBuilder.append("logger_name").append(", ");
        sqlBuilder.append("level_string").append(", ");
        sqlBuilder.append("thread_name").append(", ");
        sqlBuilder.append("reference_flag").append(", ");
        sqlBuilder.append("arg0").append(", ");
        sqlBuilder.append("arg1").append(", ");
        sqlBuilder.append("arg2").append(", ");
        sqlBuilder.append("arg3").append(", ");
        sqlBuilder.append("query_id").append(", ");
        sqlBuilder.append("nesting").append(") ");
        sqlBuilder.append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        return sqlBuilder.toString();
    }


    Map<String, String> mergePropertyMaps(ILoggingEvent event) {
        Map<String, String> mergedMap = new HashMap<String, String>();
        // we add the context properties first, then the event properties, since
        // we consider that event-specific properties should have priority over
        // context-wide properties.
        Map<String, String> loggerContextMap = event.getLoggerContextVO()
                .getPropertyMap();
        Map<String, String> mdcMap = event.getMDCPropertyMap();
        if (loggerContextMap != null) {
            mergedMap.putAll(loggerContextMap);
        }
        if (mdcMap != null) {
            mergedMap.putAll(mdcMap);
        }

        return mergedMap;
    }


}
