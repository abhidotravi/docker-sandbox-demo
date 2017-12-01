package com.mapr.demo;

import com.mapr.utils.Constants;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Sample JDBC based application to obtain details of all businesses that have
 * at least 100 reviews and a rating greater than 3.
 */
public class DrillJDBCDemo {
    public static final Logger logger = Logger.getLogger(new Throwable().getStackTrace()[1].getClassName());
    private static String JDBC_DRIVER = "org.apache.drill.jdbc.Driver";
    /**
     * Can specify connection URL in 2 ways.
     * 1. Connect to Zookeeper - "jdbc:drill:zk=<hostname/host-ip>:5181/drill/<cluster-name>-drillbits"
     * 2. Connect to Drillbit - "jdbc:drill:drillbit=<hostname>"
     */
    private static String DRILL_JDBC_URL = "jdbc:drill:zk=maprdemo:5181/drill/maprdemo.mapr.io-drillbits";
    //private static String DRILL_JDBC_URL = "jdbc:drill:drillbit=maprdemo";

    public static void main( String[] args ) {
        try {
            Class.forName(JDBC_DRIVER);
            //Username and password have to be provided to obtain connection.
            //Ensure that the user provided is present in the cluster / sandbox
            Connection connection = DriverManager.getConnection(DRILL_JDBC_URL, "mapr", "");

            Statement statement = connection.createStatement();

            final String sql = "SELECT b.`name`, b.`address`, b.`city`, b.`state`, b.`stars` " +
                    "FROM dfs.`" + Constants.TABLE_NAME + "` b " +
                    "WHERE b.`review_count` >= 100 AND b.`stars` > 3.5 " +
                    "ORDER BY b.`stars` DESC";
            logger.info("Query: " + sql);

            ResultSet result = statement.executeQuery(sql);

            while(result.next()){
                logger.info("{\"Name\": \"" + result.getString(1) + "\", "
                        + "\"Address\": \"" + result.getString(2) + "\", "
                        + "\"City\": \"" + result.getString(3) + "\", "
                        + "\"State\": \"" + result.getString(4) + "\", "
                        + "\"Stars\": " + result.getString(5) + "}");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
