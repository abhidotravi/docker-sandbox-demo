package com.mapr.demo;

import com.mapr.utils.Constants;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.apache.log4j.Logger;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.Driver;
import org.ojai.store.DriverManager;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.SortOrder;

/**
 * Sample OJAI application to obtain details of all restaurants that have
 * take-outs and a rating greater than 3.
 *
 * Also calculates the percentage of restaurants that offer take out, out of all restaurants
 * in Las Vegas that have take-out information available.
 */
public class OjaiDemo {

    public static final Logger logger = Logger.getLogger(new Throwable().getStackTrace()[1].getClassName());
    public static final String OJAI_CONNECTION_URL = "ojai:mapr:";

    public static void main( String[] args ) {
        try {
            Driver driver = DriverManager.getDriver(OJAI_CONNECTION_URL);

            //Get restaurants in Vegas which have take-out information
            QueryCondition condition = driver.newCondition()
                    .and()
                    .exists("attributes.RestaurantsTakeOut")
                    .is("city", QueryCondition.Op.EQUAL, "Las Vegas")
                    .close()
                    .build();
            logger.info("Businesses with take-out info: " + condition.toString());

            Query query = driver.newQuery()
                    .select("name", "address", "stars")
                    .where(condition)
                    .orderBy("stars", SortOrder.DESC)
                    .build();

            int totalCount = 0;
            try (final Connection connection = DriverManager.getConnection(OJAI_CONNECTION_URL);
                 final DocumentStore store = connection.getStore(Constants.TABLE_NAME);
                 final DocumentStream stream = store.findQuery(query)) {
                logger.info("Query Plan: " + stream.getQueryPlan().asJsonString()); //Log Query Plan for debugging
                logger.info("Restaurants in Las Vegas with take-out information: ");
                for(Document document : stream) {
                    logger.info(document.asJsonString());
                    totalCount++;
                }
            }

            /* Get restaurants that have take out and are rated greater than 3 */
            condition = driver.newCondition()
                    .and()
                    .condition(condition)
                    .is("attributes.RestaurantsTakeOut", QueryCondition.Op.EQUAL, true)
                    .is("stars", QueryCondition.Op.GREATER, 3)
                    .close()
                    .build();
            logger.info("Restaurants with take-outs: " + condition.toString());

            query = driver.newQuery()
                    .select("name", "address", "stars")
                    .where(condition)
                    .orderBy("stars", SortOrder.DESC)
                    .build();

            int takeOutCount = 0;
            try (final Connection connection = DriverManager.getConnection(OJAI_CONNECTION_URL);
                 final DocumentStore store = connection.getStore(Constants.TABLE_NAME);
                 final DocumentStream stream = store.findQuery(query)) {
                logger.info("Query Plan: " + stream.getQueryPlan().asJsonString()); //Log Query Plan for debugging
                logger.info("Restaurants in Las Vegas with take-outs: ");
                for(Document document : stream) {
                    logger.info(document.asJsonString());
                    takeOutCount++;
                }
            }

            logger.info("Percentage of restaurants with take-outs that have high rating: "
                    + ((double)takeOutCount / totalCount) * 100 + "%");

        } catch (Exception e) {
            logger.error("Application failed with " + e.getMessage());
            e.printStackTrace();
        }
    }
}
