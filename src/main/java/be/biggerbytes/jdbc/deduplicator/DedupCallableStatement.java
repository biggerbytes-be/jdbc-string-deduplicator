/*
 * Copyright 2013-2018 Biggerbytes.be
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package be.biggerbytes.jdbc.deduplicator;

import be.biggerbytes.jdbc.deduplicator.util.DedupContext;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public class DedupCallableStatement implements CallableStatement {
    protected final CallableStatement callableStatement;
    private final DedupContext settings;

    private DedupCallableStatement(CallableStatement callableStatement, DedupContext settings) {
        this.callableStatement = callableStatement;
        this.settings = settings;
    }

    public static DedupCallableStatement wrap(CallableStatement callableStatement, DedupContext context) {
        return new DedupCallableStatement(callableStatement, context);
    }

    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        callableStatement.registerOutParameter(parameterIndex, sqlType);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        callableStatement.registerOutParameter(parameterIndex, sqlType, scale);
    }

    public boolean wasNull() throws SQLException {
        return callableStatement.wasNull();
    }

    public String getString(int parameterIndex) throws SQLException {
        return callableStatement.getString(parameterIndex);
    }

    public boolean getBoolean(int parameterIndex) throws SQLException {
        return callableStatement.getBoolean(parameterIndex);
    }

    public byte getByte(int parameterIndex) throws SQLException {
        return callableStatement.getByte(parameterIndex);
    }

    public short getShort(int parameterIndex) throws SQLException {
        return callableStatement.getShort(parameterIndex);
    }

    public int getInt(int parameterIndex) throws SQLException {
        return callableStatement.getInt(parameterIndex);
    }

    public long getLong(int parameterIndex) throws SQLException {
        return callableStatement.getLong(parameterIndex);
    }

    public float getFloat(int parameterIndex) throws SQLException {
        return callableStatement.getFloat(parameterIndex);
    }

    public double getDouble(int parameterIndex) throws SQLException {
        return callableStatement.getDouble(parameterIndex);
    }

    @Deprecated
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return callableStatement.getBigDecimal(parameterIndex, scale);
    }

    public byte[] getBytes(int parameterIndex) throws SQLException {
        return callableStatement.getBytes(parameterIndex);
    }

    public Date getDate(int parameterIndex) throws SQLException {
        return callableStatement.getDate(parameterIndex);
    }

    public Time getTime(int parameterIndex) throws SQLException {
        return callableStatement.getTime(parameterIndex);
    }

    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return callableStatement.getTimestamp(parameterIndex);
    }

    public Object getObject(int parameterIndex) throws SQLException {
        return callableStatement.getObject(parameterIndex);
    }

    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return callableStatement.getBigDecimal(parameterIndex);
    }

    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return callableStatement.getObject(parameterIndex, map);
    }

    public Ref getRef(int parameterIndex) throws SQLException {
        return callableStatement.getRef(parameterIndex);
    }

    public Blob getBlob(int parameterIndex) throws SQLException {
        return callableStatement.getBlob(parameterIndex);
    }

    public Clob getClob(int parameterIndex) throws SQLException {
        return callableStatement.getClob(parameterIndex);
    }

    public Array getArray(int parameterIndex) throws SQLException {
        return callableStatement.getArray(parameterIndex);
    }

    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return callableStatement.getDate(parameterIndex, cal);
    }

    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return callableStatement.getTime(parameterIndex, cal);
    }

    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return callableStatement.getTimestamp(parameterIndex, cal);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        callableStatement.registerOutParameter(parameterIndex, sqlType, typeName);
    }

    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        callableStatement.registerOutParameter(parameterName, sqlType);
    }

    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        callableStatement.registerOutParameter(parameterName, sqlType, scale);
    }

    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        callableStatement.registerOutParameter(parameterName, sqlType, typeName);
    }

    public URL getURL(int parameterIndex) throws SQLException {
        return callableStatement.getURL(parameterIndex);
    }

    public void setURL(String parameterName, URL val) throws SQLException {
        callableStatement.setURL(parameterName, val);
    }

    public void setNull(String parameterName, int sqlType) throws SQLException {
        callableStatement.setNull(parameterName, sqlType);
    }

    public void setBoolean(String parameterName, boolean x) throws SQLException {
        callableStatement.setBoolean(parameterName, x);
    }

    public void setByte(String parameterName, byte x) throws SQLException {
        callableStatement.setByte(parameterName, x);
    }

    public void setShort(String parameterName, short x) throws SQLException {
        callableStatement.setShort(parameterName, x);
    }

    public void setInt(String parameterName, int x) throws SQLException {
        callableStatement.setInt(parameterName, x);
    }

    public void setLong(String parameterName, long x) throws SQLException {
        callableStatement.setLong(parameterName, x);
    }

    public void setFloat(String parameterName, float x) throws SQLException {
        callableStatement.setFloat(parameterName, x);
    }

    public void setDouble(String parameterName, double x) throws SQLException {
        callableStatement.setDouble(parameterName, x);
    }

    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        callableStatement.setBigDecimal(parameterName, x);
    }

    public void setString(String parameterName, String x) throws SQLException {
        callableStatement.setString(parameterName, x);
    }

    public void setBytes(String parameterName, byte[] x) throws SQLException {
        callableStatement.setBytes(parameterName, x);
    }

    public void setDate(String parameterName, Date x) throws SQLException {
        callableStatement.setDate(parameterName, x);
    }

    public void setTime(String parameterName, Time x) throws SQLException {
        callableStatement.setTime(parameterName, x);
    }

    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        callableStatement.setTimestamp(parameterName, x);
    }

    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        callableStatement.setAsciiStream(parameterName, x, length);
    }

    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        callableStatement.setBinaryStream(parameterName, x, length);
    }

    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        callableStatement.setObject(parameterName, x, targetSqlType, scale);
    }

    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        callableStatement.setObject(parameterName, x, targetSqlType);
    }

    public void setObject(String parameterName, Object x) throws SQLException {
        callableStatement.setObject(parameterName, x);
    }

    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        callableStatement.setCharacterStream(parameterName, reader, length);
    }

    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        callableStatement.setDate(parameterName, x, cal);
    }

    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        callableStatement.setTime(parameterName, x, cal);
    }

    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        callableStatement.setTimestamp(parameterName, x, cal);
    }

    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        callableStatement.setNull(parameterName, sqlType, typeName);
    }

    public String getString(String parameterName) throws SQLException {
        return callableStatement.getString(parameterName);
    }

    public boolean getBoolean(String parameterName) throws SQLException {
        return callableStatement.getBoolean(parameterName);
    }

    public byte getByte(String parameterName) throws SQLException {
        return callableStatement.getByte(parameterName);
    }

    public short getShort(String parameterName) throws SQLException {
        return callableStatement.getShort(parameterName);
    }

    public int getInt(String parameterName) throws SQLException {
        return callableStatement.getInt(parameterName);
    }

    public long getLong(String parameterName) throws SQLException {
        return callableStatement.getLong(parameterName);
    }

    public float getFloat(String parameterName) throws SQLException {
        return callableStatement.getFloat(parameterName);
    }

    public double getDouble(String parameterName) throws SQLException {
        return callableStatement.getDouble(parameterName);
    }

    public byte[] getBytes(String parameterName) throws SQLException {
        return callableStatement.getBytes(parameterName);
    }

    public Date getDate(String parameterName) throws SQLException {
        return callableStatement.getDate(parameterName);
    }

    public Time getTime(String parameterName) throws SQLException {
        return callableStatement.getTime(parameterName);
    }

    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return callableStatement.getTimestamp(parameterName);
    }

    public Object getObject(String parameterName) throws SQLException {
        return callableStatement.getObject(parameterName);
    }

    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return callableStatement.getBigDecimal(parameterName);
    }

    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return callableStatement.getObject(parameterName, map);
    }

    public Ref getRef(String parameterName) throws SQLException {
        return callableStatement.getRef(parameterName);
    }

    public Blob getBlob(String parameterName) throws SQLException {
        return callableStatement.getBlob(parameterName);
    }

    public Clob getClob(String parameterName) throws SQLException {
        return callableStatement.getClob(parameterName);
    }

    public Array getArray(String parameterName) throws SQLException {
        return callableStatement.getArray(parameterName);
    }

    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return callableStatement.getDate(parameterName, cal);
    }

    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return callableStatement.getTime(parameterName, cal);
    }

    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return callableStatement.getTimestamp(parameterName, cal);
    }

    public URL getURL(String parameterName) throws SQLException {
        return callableStatement.getURL(parameterName);
    }

    public RowId getRowId(int parameterIndex) throws SQLException {
        return callableStatement.getRowId(parameterIndex);
    }

    public RowId getRowId(String parameterName) throws SQLException {
        return callableStatement.getRowId(parameterName);
    }

    public void setRowId(String parameterName, RowId x) throws SQLException {
        callableStatement.setRowId(parameterName, x);
    }

    public void setNString(String parameterName, String value) throws SQLException {
        callableStatement.setNString(parameterName, value);
    }

    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        callableStatement.setNCharacterStream(parameterName, value, length);
    }

    public void setNClob(String parameterName, NClob value) throws SQLException {
        callableStatement.setNClob(parameterName, value);
    }

    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        callableStatement.setClob(parameterName, reader, length);
    }

    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        callableStatement.setBlob(parameterName, inputStream, length);
    }

    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        callableStatement.setNClob(parameterName, reader, length);
    }

    public NClob getNClob(int parameterIndex) throws SQLException {
        return callableStatement.getNClob(parameterIndex);
    }

    public NClob getNClob(String parameterName) throws SQLException {
        return callableStatement.getNClob(parameterName);
    }

    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        callableStatement.setSQLXML(parameterName, xmlObject);
    }

    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return callableStatement.getSQLXML(parameterIndex);
    }

    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return callableStatement.getSQLXML(parameterName);
    }

    public String getNString(int parameterIndex) throws SQLException {
        return callableStatement.getNString(parameterIndex);
    }

    public String getNString(String parameterName) throws SQLException {
        return callableStatement.getNString(parameterName);
    }

    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return callableStatement.getNCharacterStream(parameterIndex);
    }

    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return callableStatement.getNCharacterStream(parameterName);
    }

    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return callableStatement.getCharacterStream(parameterIndex);
    }

    public Reader getCharacterStream(String parameterName) throws SQLException {
        return callableStatement.getCharacterStream(parameterName);
    }

    public void setBlob(String parameterName, Blob x) throws SQLException {
        callableStatement.setBlob(parameterName, x);
    }

    public void setClob(String parameterName, Clob x) throws SQLException {
        callableStatement.setClob(parameterName, x);
    }

    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        callableStatement.setAsciiStream(parameterName, x, length);
    }

    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        callableStatement.setBinaryStream(parameterName, x, length);
    }

    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        callableStatement.setCharacterStream(parameterName, reader, length);
    }

    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        callableStatement.setAsciiStream(parameterName, x);
    }

    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        callableStatement.setBinaryStream(parameterName, x);
    }

    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        callableStatement.setCharacterStream(parameterName, reader);
    }

    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        callableStatement.setNCharacterStream(parameterName, value);
    }

    public void setClob(String parameterName, Reader reader) throws SQLException {
        callableStatement.setClob(parameterName, reader);
    }

    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        callableStatement.setBlob(parameterName, inputStream);
    }

    public void setNClob(String parameterName, Reader reader) throws SQLException {
        callableStatement.setNClob(parameterName, reader);
    }

    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        return callableStatement.getObject(parameterIndex, type);
    }

    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return callableStatement.getObject(parameterName, type);
    }

    public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        callableStatement.setObject(parameterName, x, targetSqlType, scaleOrLength);
    }

    public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
        callableStatement.setObject(parameterName, x, targetSqlType);
    }

    public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
        callableStatement.registerOutParameter(parameterIndex, sqlType);
    }

    public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale) throws SQLException {
        callableStatement.registerOutParameter(parameterIndex, sqlType, scale);
    }

    public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName) throws SQLException {
        callableStatement.registerOutParameter(parameterIndex, sqlType, typeName);
    }

    public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
        callableStatement.registerOutParameter(parameterName, sqlType);
    }

    public void registerOutParameter(String parameterName, SQLType sqlType, int scale) throws SQLException {
        callableStatement.registerOutParameter(parameterName, sqlType, scale);
    }

    public void registerOutParameter(String parameterName, SQLType sqlType, String typeName) throws SQLException {
        callableStatement.registerOutParameter(parameterName, sqlType, typeName);
    }

    public ResultSet executeQuery() throws SQLException {
        return DedupResultSet.wrap(callableStatement.executeQuery(), settings);
    }

    public int executeUpdate() throws SQLException {
        return callableStatement.executeUpdate();
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        callableStatement.setNull(parameterIndex, sqlType);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        callableStatement.setBoolean(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        callableStatement.setByte(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        callableStatement.setShort(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        callableStatement.setInt(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        callableStatement.setLong(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        callableStatement.setFloat(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        callableStatement.setDouble(parameterIndex, x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        callableStatement.setBigDecimal(parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        callableStatement.setString(parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        callableStatement.setBytes(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        callableStatement.setDate(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        callableStatement.setTime(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        callableStatement.setTimestamp(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        callableStatement.setAsciiStream(parameterIndex, x, length);
    }

    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        callableStatement.setUnicodeStream(parameterIndex, x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        callableStatement.setBinaryStream(parameterIndex, x, length);
    }

    public void clearParameters() throws SQLException {
        callableStatement.clearParameters();
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        callableStatement.setObject(parameterIndex, x, targetSqlType);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        callableStatement.setObject(parameterIndex, x);
    }

    public boolean execute() throws SQLException {
        return callableStatement.execute();
    }

    public void addBatch() throws SQLException {
        callableStatement.addBatch();
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        callableStatement.setCharacterStream(parameterIndex, reader, length);
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        callableStatement.setRef(parameterIndex, x);
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        callableStatement.setBlob(parameterIndex, x);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        callableStatement.setClob(parameterIndex, x);
    }

    public void setArray(int parameterIndex, Array x) throws SQLException {
        callableStatement.setArray(parameterIndex, x);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return callableStatement.getMetaData();
    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        callableStatement.setDate(parameterIndex, x, cal);
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        callableStatement.setTime(parameterIndex, x, cal);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        callableStatement.setTimestamp(parameterIndex, x, cal);
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        callableStatement.setNull(parameterIndex, sqlType, typeName);
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        callableStatement.setURL(parameterIndex, x);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return callableStatement.getParameterMetaData();
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        callableStatement.setRowId(parameterIndex, x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        callableStatement.setNString(parameterIndex, value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        callableStatement.setNCharacterStream(parameterIndex, value, length);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        callableStatement.setNClob(parameterIndex, value);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        callableStatement.setClob(parameterIndex, reader, length);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        callableStatement.setBlob(parameterIndex, inputStream, length);
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        callableStatement.setNClob(parameterIndex, reader, length);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        callableStatement.setSQLXML(parameterIndex, xmlObject);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        callableStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        callableStatement.setAsciiStream(parameterIndex, x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        callableStatement.setBinaryStream(parameterIndex, x, length);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        callableStatement.setCharacterStream(parameterIndex, reader, length);
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        callableStatement.setAsciiStream(parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        callableStatement.setBinaryStream(parameterIndex, x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        callableStatement.setCharacterStream(parameterIndex, reader);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        callableStatement.setNCharacterStream(parameterIndex, value);
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        callableStatement.setClob(parameterIndex, reader);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        callableStatement.setBlob(parameterIndex, inputStream);
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        callableStatement.setNClob(parameterIndex, reader);
    }

    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        callableStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        callableStatement.setObject(parameterIndex, x, targetSqlType);
    }

    public long executeLargeUpdate() throws SQLException {
        return callableStatement.executeLargeUpdate();
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        return DedupResultSet.wrap(callableStatement.executeQuery(sql), settings);
    }

    public int executeUpdate(String sql) throws SQLException {
        return callableStatement.executeUpdate(sql);
    }

    public void close() throws SQLException {
        callableStatement.close();
    }

    public int getMaxFieldSize() throws SQLException {
        return callableStatement.getMaxFieldSize();
    }

    public void setMaxFieldSize(int max) throws SQLException {
        callableStatement.setMaxFieldSize(max);
    }

    public int getMaxRows() throws SQLException {
        return callableStatement.getMaxRows();
    }

    public void setMaxRows(int max) throws SQLException {
        callableStatement.setMaxRows(max);
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        callableStatement.setEscapeProcessing(enable);
    }

    public int getQueryTimeout() throws SQLException {
        return callableStatement.getQueryTimeout();
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        callableStatement.setQueryTimeout(seconds);
    }

    public void cancel() throws SQLException {
        callableStatement.cancel();
    }

    public SQLWarning getWarnings() throws SQLException {
        return callableStatement.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        callableStatement.clearWarnings();
    }

    public void setCursorName(String name) throws SQLException {
        callableStatement.setCursorName(name);
    }

    public boolean execute(String sql) throws SQLException {
        return callableStatement.execute(sql);
    }

    public ResultSet getResultSet() throws SQLException {
        return callableStatement.getResultSet();
    }

    public int getUpdateCount() throws SQLException {
        return callableStatement.getUpdateCount();
    }

    public boolean getMoreResults() throws SQLException {
        return callableStatement.getMoreResults();
    }

    public void setFetchDirection(int direction) throws SQLException {
        callableStatement.setFetchDirection(direction);
    }

    public int getFetchDirection() throws SQLException {
        return callableStatement.getFetchDirection();
    }

    public void setFetchSize(int rows) throws SQLException {
        callableStatement.setFetchSize(rows);
    }

    public int getFetchSize() throws SQLException {
        return callableStatement.getFetchSize();
    }

    public int getResultSetConcurrency() throws SQLException {
        return callableStatement.getResultSetConcurrency();
    }

    public int getResultSetType() throws SQLException {
        return callableStatement.getResultSetType();
    }

    public void addBatch(String sql) throws SQLException {
        callableStatement.addBatch(sql);
    }

    public void clearBatch() throws SQLException {
        callableStatement.clearBatch();
    }

    public int[] executeBatch() throws SQLException {
        return callableStatement.executeBatch();
    }

    public Connection getConnection() throws SQLException {
        return callableStatement.getConnection();
    }

    public boolean getMoreResults(int current) throws SQLException {
        return callableStatement.getMoreResults(current);
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return callableStatement.getGeneratedKeys();
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return callableStatement.executeUpdate(sql, autoGeneratedKeys);
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return callableStatement.executeUpdate(sql, columnIndexes);
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return callableStatement.executeUpdate(sql, columnNames);
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return callableStatement.execute(sql, autoGeneratedKeys);
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return callableStatement.execute(sql, columnIndexes);
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return callableStatement.execute(sql, columnNames);
    }

    public int getResultSetHoldability() throws SQLException {
        return callableStatement.getResultSetHoldability();
    }

    public boolean isClosed() throws SQLException {
        return callableStatement.isClosed();
    }

    public void setPoolable(boolean poolable) throws SQLException {
        callableStatement.setPoolable(poolable);
    }

    public boolean isPoolable() throws SQLException {
        return callableStatement.isPoolable();
    }

    public void closeOnCompletion() throws SQLException {
        callableStatement.closeOnCompletion();
    }

    public boolean isCloseOnCompletion() throws SQLException {
        return callableStatement.isCloseOnCompletion();
    }

    public long getLargeUpdateCount() throws SQLException {
        return callableStatement.getLargeUpdateCount();
    }

    public void setLargeMaxRows(long max) throws SQLException {
        callableStatement.setLargeMaxRows(max);
    }

    public long getLargeMaxRows() throws SQLException {
        return callableStatement.getLargeMaxRows();
    }

    public long[] executeLargeBatch() throws SQLException {
        return callableStatement.executeLargeBatch();
    }

    public long executeLargeUpdate(String sql) throws SQLException {
        return callableStatement.executeLargeUpdate(sql);
    }

    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return callableStatement.executeLargeUpdate(sql, autoGeneratedKeys);
    }

    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return callableStatement.executeLargeUpdate(sql, columnIndexes);
    }

    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        return callableStatement.executeLargeUpdate(sql, columnNames);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return callableStatement.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return callableStatement.isWrapperFor(iface);
    }

}
