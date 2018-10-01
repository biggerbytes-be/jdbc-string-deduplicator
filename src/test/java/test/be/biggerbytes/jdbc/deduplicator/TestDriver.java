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

package test.be.biggerbytes.jdbc.deduplicator;

import be.biggerbytes.jdbc.deduplicator.DedupDriver;

import java.sql.*;
import java.util.Properties;
import java.util.ServiceLoader;

public class TestDriver {
    public static void main(String[] args) throws SQLException, InterruptedException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        ServiceLoader<Driver> loadedDrivers = ServiceLoader.load(Driver.class);
        for (Driver loadedDriver : loadedDrivers) {
            if (loadedDriver.acceptsURL("jdbc:dedup:"))
                System.out.println("YES");
        }
//        Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
//        use(conn);
        DedupDriver dedupDriver = new DedupDriver();
        Properties properties = new Properties();
        properties.put( "user", "sa");
        properties.put( "password", "");
//        Connection conn2 = dedupDriver.connect("jdbc:dedup:org.h2.Driver:h2:mem:", properties);
        Connection conn2 = dedupDriver.connect("jdbc:dedup:h2:mem:", properties);
        use(conn2);
//         jdbc:derby:memory:myDB;create=true
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        Connection conn3 = dedupDriver.connect("jdbc:dedup:derby:memory:test;create=true", properties);
        use(conn3);
        Thread.sleep(60*1000);
    }

    public static void use(Connection conn) throws SQLException {
        // add application code here
        conn.createStatement().execute(
                "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(256))"
//                "CREATE TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY, NAME VARCHAR);"

        );
        for (int i = 0; i < 1000; i++) {
            conn.createStatement().execute(
                    "INSERT INTO TEST VALUES("+i+", 'Hello World')"
            );
        }

        ResultSet resultSet = conn.createStatement().executeQuery("select * from TEST");
        //
        while (resultSet.next()) {
            String s = resultSet.getString("NAME");
        }
        conn.close();
    }
}
