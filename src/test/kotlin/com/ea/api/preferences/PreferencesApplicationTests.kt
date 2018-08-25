package com.ea.api.preferences


import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.Temporal
import java.util.*
import java.util.concurrent.TimeUnit


@DisplayName("Playing with DynamoDB and Async SDK")
class AWSAsyncUnitTesting() {

    val client: DynamoDbAsyncClient? = DynamoDbAsyncClient.builder()
            .region(Region.EU_WEST_1)
            .credentialsProvider(ProfileCredentialsProvider.builder()
                    .profileName("default")
                    .build())
            .build()

    val logger: Logger = LoggerFactory.getLogger(AWSAsyncUnitTesting::class.java)

    @BeforeEach
    fun initBeforeEach() {
        // The region and credentials provider are for demonstration purposes. Feel free to use whatever region and credentials
        // are appropriate for you, or load them from the environment (See https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/setup-credentials.html)
        logger.debug("Initilization beafore each...")
    }

    @Test
    @DisplayName("Retrieve DynamoDB tables and display them (waiting for 5 secs)")
    fun getDynamoDBTables() {
        client!!.listTables(ListTablesRequest.builder()
            .limit(5)
            .build())
            .get(5, TimeUnit.SECONDS) // This is a blocking wait !
            .tableNames()
            .forEach { logger.info("$it".toUpperCase()) }
    }

    @Test
    @DisplayName("Again another aync way to retrieve DynamoDB tables and display their number")
    fun getDynamoDBTables2() {
        val response = client!!.listTables(ListTablesRequest.builder()
                .limit(5)
                .build())

        // Map the response to another CompletableFuture containing just the table names
        val tableNames = response.thenApply { it.tableNames() }

        // When future is complete (either successfully or in error) handle the response
        tableNames.whenCompleteAsync { tables, err ->
            logger.debug("#### Someting is happening ! ")
            if (tables != null) {
                assert(tableNames.isDone && tables.size > 0)
                logger.debug("#### Number of tables : " + tables.size)
            } else {
                // Handle error
                err.printStackTrace()
            }
        }

        logger.debug("#### Now waiting for the response to come...")
        val start : LocalDateTime = LocalDateTime.now()
        var end : LocalDateTime = LocalDateTime.now()
        while (
                ( ! tableNames.isDone || ! tableNames.isCancelled )
                && ( Duration.between(start, end).toMillis() < 5000 )
            ) {
            end = LocalDateTime.now()
            if ( tableNames.isDone) {
                break
            }
        }
        logger.debug("#### Exiting...")
    }


    @Test
    @DisplayName("Let's create a table")
    fun createDynamoDBTable() {
        logger.debug("Not implemented yet")
    }

    @Test
    @DisplayName("Let's delete a table")
    fun deleteDynamoDBTable() {
        logger.debug("Not implemented yet")
    }

    @Test
    @DisplayName("Let's insert data into a table")
    fun insertDAtaFromDynamoDBTable() {
        logger.debug("Not implemented yet")
    }

    @Test
    @DisplayName("Let's delete data from a table")
    fun deleteDataFromDynamoDBTable() {
        logger.debug("Not implemented yet")
    }

}

