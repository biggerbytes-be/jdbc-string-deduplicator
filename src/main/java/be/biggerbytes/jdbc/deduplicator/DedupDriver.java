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
import be.biggerbytes.jdbc.deduplicator.util.DedupInfo;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class DedupDriver implements Driver {
    Driver driver;
    private int offset = "jdbc:dedup:".length();
    private Map<String, DedupInfo> created = new ConcurrentHashMap<>();

    /**
     * format of URL:  jdbc:dedup:the_fqcn_of_your_driver:driver_specific_parameters[:....]*
     * OR
     * format of URL:  jdbc:dedup::driver_specific_parameters[:....]*
     *
     * @param url
     * @return
     */
    public Connection connect(String url, Properties info) throws SQLException {
        String[] parts = url.split(":");
        //
        String _3rd = parts[2];
        String newUrl;
        String realUrl = realPart(url);

        if (_3rd.contains(".")) {
            newUrl = "jdbc:" + urlPart(realUrl);
            driver = getDriver(realUrl);
        } else {
            newUrl = "jdbc:" + realUrl;
            driver = DriverManager.getDriver(newUrl);
        }

        DedupSettings settings = new DedupSettings(info);
        DedupInfo mBean = getMbean(newUrl, settings);
        DedupContext context = new DedupContext(settings, mBean);
        //
        return DedupConnection.wrap(driver.connect(newUrl, info), context);
    }

    /**
     *
     * @param newUrl
     * @param settings
     * @return
     */
    private DedupInfo getMbean(String newUrl, DedupSettings settings) throws SQLException {
        DedupInfo existings = created.get(newUrl);
        if (existings != null) {
            return existings;
        }

        try {
            DedupInfo mBean = new DedupInfo(settings);
            ObjectName objectName = ObjectName.getInstance(
                    "dedup",
                    "url",
                    newUrl.replace(":", "_")
                            .replace("=", "_")
                            .replace(",", "_")
            );
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            mBeanServer.registerMBean(mBean, objectName);
            //
            created.put(newUrl, mBean);
            //
            return mBean;
        } catch (MalformedObjectNameException | NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
            throw  new SQLException("Problem registering MBean for " + newUrl, e);
        }
    }

    private String realPart(String url) {
        return url.substring(offset);
    }

    private String urlPart(String url) {
        return url.substring(url.indexOf(":")+1);
    }

    /**
     * format of URL:  jdbc:dedup:the_fqcn_of_your_driver:driver_specific_parameters[:....]*
     *
     * @param url
     * @return
     */
    private Driver getDriver(String url) throws SQLException {
        String[] parts = url.split(":");
        String driverFQCN = parts[0];

        try {
            Class<Driver> driverClass = (Class<Driver>) Class.forName(driverFQCN);
            Driver d = driverClass.newInstance();
            return d;
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new SQLException("Can not instantiate driver: " + driverFQCN, e);
        }
    }

    public boolean acceptsURL(String url) throws SQLException {
        return url.toLowerCase().contains("dedup:");
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return driver.getPropertyInfo(realPart(url), info);
    }

    public int getMajorVersion() {
        return driver.getMajorVersion();
    }

    public int getMinorVersion() {
        return driver.getMinorVersion();
    }

    public boolean jdbcCompliant() {
        return driver.jdbcCompliant();
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
