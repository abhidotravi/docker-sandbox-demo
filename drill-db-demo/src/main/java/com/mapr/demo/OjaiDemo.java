package com.mapr.demo;

import com.mapr.utils.Constants;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.Driver;
import org.ojai.store.DriverManager;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.apache.log4j.Logger;

public class OjaiDemo {

    public static final Logger logger = Logger.getLogger(new Throwable().getStackTrace()[1].getClassName());

    public static void main( String[] args ) {
        DocumentStream stream = null;
        try {
            /**
             * Driver can be considered as an entry point to Ojai API.
             * Driver can be used to build QueryConditions / Queries / Mutations etc.
             */
            Driver driver = DriverManager.getDriver(Constants.CONNECTION_URL);

            //We first build a query condition which forms the "where" clause for a Query
            QueryCondition condition = driver.newCondition()
                    .and() //condition support prefix notation, start with join op
                    .is("stars", QueryCondition.Op.GREATER, 3) //condition 1
                    .is("state", QueryCondition.Op.EQUAL, "NV") //condition 2
                    .close() //close the predicate
                    .build(); //build immutable condition object

            logger.info("Query Condition: " + condition.toString());

            //We now build a query object by specifying the fields to be projected
            Query query = driver.newQuery()
                    .select("name", "address", "review_count") //if no select or select("*"), all fields are returned
                    .where(condition)
                    .build();
            try (final Connection connection = DriverManager.getConnection(Constants.CONNECTION_URL);
                 final DocumentStore store = connection.getStore(Constants.TABLE_NAME)) {

                stream = store.findQuery(query);

                logger.info("Query Plan: " + stream.getQueryPlan().asJsonString()); //Log Query Plan for debugging
                logger.info("Query Result:");
                for(Document document : stream) {
                    logger.info(document.asJsonString());

                }

            }
        } catch (Exception e) {
            logger.error("Application failed with " + e.getMessage());
            e.printStackTrace();
        } finally {
            if(stream != null)
                stream.close();
        }
    }
}
