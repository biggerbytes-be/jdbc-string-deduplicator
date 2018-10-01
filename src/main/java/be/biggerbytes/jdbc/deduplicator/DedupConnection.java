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

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class DedupConnection implements Connection {
    protected final Connection c;
    protected final DedupContext context;

    private DedupConnection(Connection c, DedupContext context) {
        this.c = c;
        this.context = context;
    }

    public static DedupConnection wrap(Connection c, DedupContext context) {
        return new DedupConnection(c, context);
    }

    public Statement createStatement() throws SQLException {
        return DedupStatement.wrap(c.createStatement(), context);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return DedupPreparedStatement.wrap(c.prepareStatement(sql), context);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return DedupCallableStatement.wrap(c.prepareCall(sql), context);
    }

    public String nativeSQL(String sql) throws SQLException {
        return c.nativeSQL(sql);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        c.setAutoCommit(autoCommit);
    }

    public boolean getAutoCommit() throws SQLException {
        return c.getAutoCommit();
    }

    public void commit() throws SQLException {
        c.commit();
    }

    public void rollback() throws SQLException {
        c.rollback();
    }

    public void close() throws SQLException {
        c.close();
    }

    public boolean isClosed() throws SQLException {
        return c.isClosed();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return c.getMetaData();
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        c.setReadOnly(readOnly);
    }

    public boolean isReadOnly() throws SQLException {
        return c.isReadOnly();
    }

    public void setCatalog(String catalog) throws SQLException {
        c.setCatalog(catalog);
    }

    public String getCatalog() throws SQLException {
        return c.getCatalog();
    }

    public void setTransactionIsolation(int level) throws SQLException {
        c.setTransactionIsolation(level);
    }

    public int getTransactionIsolation() throws SQLException {
        return c.getTransactionIsolation();
    }

    public SQLWarning getWarnings() throws SQLException {
        return c.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        c.clearWarnings();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return DedupStatement.wrap(c.createStatement(resultSetType, resultSetConcurrency), context);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return  DedupPreparedStatement.wrap(c.prepareStatement(sql, resultSetType, resultSetConcurrency), context);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return DedupCallableStatement.wrap(c.prepareCall(sql, resultSetType, resultSetConcurrency), context);
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return c.getTypeMap();
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        c.setTypeMap(map);
    }

    public void setHoldability(int holdability) throws SQLException {
        c.setHoldability(holdability);
    }

    public int getHoldability() throws SQLException {
        return c.getHoldability();
    }

    public Savepoint setSavepoint() throws SQLException {
        return c.setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return c.setSavepoint(name);
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        c.rollback(savepoint);
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        c.releaseSavepoint(savepoint);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return DedupStatement.wrap(c.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), context);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return DedupPreparedStatement.wrap(c.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability), context);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return DedupCallableStatement.wrap(c.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability), context);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return DedupPreparedStatement.wrap(c.prepareStatement(sql, autoGeneratedKeys), context);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return DedupPreparedStatement.wrap(c.prepareStatement(sql, columnIndexes), context);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return DedupPreparedStatement.wrap(c.prepareStatement(sql, columnNames), context);
    }

    public Clob createClob() throws SQLException {
        return c.createClob();
    }

    public Blob createBlob() throws SQLException {
        return c.createBlob();
    }

    public NClob createNClob() throws SQLException {
        return c.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        return c.createSQLXML();
    }

    public boolean isValid(int timeout) throws SQLException {
        return c.isValid(timeout);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        c.setClientInfo(name, value);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        c.setClientInfo(properties);
    }

    public String getClientInfo(String name) throws SQLException {
        return c.getClientInfo(name);
    }

    public Properties getClientInfo() throws SQLException {
        return c.getClientInfo();
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return c.createArrayOf(typeName, elements);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return c.createStruct(typeName, attributes);
    }

    public void setSchema(String schema) throws SQLException {
        c.setSchema(schema);
    }

    public String getSchema() throws SQLException {
        return c.getSchema();
    }

    public void abort(Executor executor) throws SQLException {
        c.abort(executor);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        c.setNetworkTimeout(executor, milliseconds);
    }

    public int getNetworkTimeout() throws SQLException {
        return c.getNetworkTimeout();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return c.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return c.isWrapperFor(iface);
    }

}
