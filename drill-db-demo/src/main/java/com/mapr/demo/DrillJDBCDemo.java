package com.mapr.demo;

import com.mapr.utils.Constants;
import org.apache.log4j.Logger;

import java.sql.*;

/**
 * Created by aravi on 11/25/17.
 */
public class DrillJDBCDemo {
    public static final Logger logger = Logger.getLogger(new Throwable().getStackTrace()[1].getClassName());
    private static String JDBC_DRIVER = "org.apache.drill.jdbc.Driver";
    //jdbc:drill:zk=<hostname/host-ip>:5181/drill/<cluster-name>-drillbits
    private static String DRILL_JDBC_URL = "jdbc:drill:zk=maprdemo:5181/drill/maprdemo.mapr.io-drillbits";
    //private static String DRILL_JDBC_URL = "jdbc:drill:drillbit=10.250.56.103";

    public static void main( String[] args ) {
        try {
            Class.forName(JDBC_DRIVER);
            Connection connection = DriverManager.getConnection(DRILL_JDBC_URL, "mapr", "");
            Statement st = connection.createStatement();
            final String sql = "SELECT b.`name`, b.`address`, b.`city`, b.`state`, b.`stars` " +
                    "FROM dfs.`" + Constants.TABLE_NAME + "` b " +
                    "WHERE b.`review_count` > 100 AND b.`stars` > 3.5 " +
                    "ORDER BY b.`stars` DESC";
            logger.info("Query: " + sql);
            ResultSet rs = st.executeQuery(sql);
            while(rs.next()){
                logger.info("{\"Name\": \"" + rs.getString(1) + "\", "
                        + "\"Address\": \"" + rs.getString(2) + "\", "
                        + "\"City\": \"" + rs.getString(3) + "\", "
                        + "\"State\": \"" + rs.getString(4) + "\", "
                        + "\"Stars\": " + rs.getString(5) + "}");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
