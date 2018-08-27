package org.vincedgy.aws.preferences

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.utils.FunctionalUtils
import java.nio.file.Paths
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

/**
 * Super snippet collection in Kotlin for AWS async operations on S3, DynamoDB etc...
 */
@DisplayName("Playing with DynamoDB and Async SDK")
class AWSAsyncUnitTesting() {

    val logger: Logger = LoggerFactory.getLogger(AWSAsyncUnitTesting::class.java)
    val BUCKET = "ea-sftp-01"
    val KEY = "test.txt"

    // Global configuration for httpClient using netty
    var httpClient = NettyNioAsyncHttpClient.builder()
            .connectionAcquisitionTimeout(Duration.ofSeconds(10))
            .connectionTimeout(Duration.ofMillis(750))
            .build()

    // DynamoDB Async client
    val dynamoDbAsyncClient: DynamoDbAsyncClient? = DynamoDbAsyncClient.builder()
            .region(Region.EU_WEST_1)
            .credentialsProvider(ProfileCredentialsProvider.builder()
                    .profileName("default")
                    .build())
            .httpClient(httpClient)
            .build()

    // S3 Async Client
    val S3Client: S3AsyncClient? = S3AsyncClient.builder()
            .region(Region.EU_WEST_3)
            .credentialsProvider(ProfileCredentialsProvider.builder()
                    .profileName("default")
                    .build())
            .httpClient(httpClient)
            .build()

    /**
     * Initialize and do things before each test
     */
    /* @BeforeEach
     fun initBeforeEach() {
         // The region and credentials provider are for demonstration purposes. Feel free to use whatever region and credentials
         // are appropriate for you, or load them from the environment (See https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/setup-credentials.html)
         logger.debug("Initilization beafore each...")
     }
 */

    /**
     * Lists dynamoDB tables with an async but blocking way
     */
    @Test
    @DisplayName("Retrieve DynamoDB tables and display them (waiting for 5 secs)")
    fun getDynamoDBTables() {
        dynamoDbAsyncClient!!.listTables(ListTablesRequest.builder()
                .limit(5)
                .build())
                .get(5, TimeUnit.SECONDS) // This is a blocking wait !
                .tableNames()
                .forEach { logger.info("$it".toUpperCase()) }
    }

    /**
     * gGets the lists of dynamoDB tables async
     */
    @Test
    @DisplayName("Again another aync way to retrieve DynamoDB tables and display their number")
    fun getDynamoDBTablesAsync() {
        val response = dynamoDbAsyncClient!!.listTables(ListTablesRequest.builder()
                .limit(5)
                .build())

        // Map the response to another CompletableFuture containing just the table names
        val tableNames = response.thenApply { it.tableNames() }

        // When future is complete (either successfully or in error) handle the response
        tableNames.whenComplete { tables, err ->
            if (tables != null) {
                assertAll("Should get a non empty tables list and the first one should have been known",
                        { assert(tables.size > 1) },
                        { assert(tables.get(0).equals("terraform_locks")) }
                )
            } else {
                assert(false)
                err.printStackTrace()
            }
        }

        logger.info("#### Now waiting for the response to come...")
        val start: LocalDateTime = LocalDateTime.now()
        var end: LocalDateTime = LocalDateTime.now()
        while (
                (!tableNames.isDone || !tableNames.isCancelled)
                && (Duration.between(start, end).toMillis() < 5000)
        ) {
            end = LocalDateTime.now()
            if (tableNames.isDone) {
                break
            }
        }
    }


    /**
     * Puts a local file within an S3 bucket
     */
    @Test
    @DisplayName("Let's put a file into an S3 Bucket Asynchonouslty")
    fun pushFileIntoS3Bucket() {
        S3Client!!.putObject(PutObjectRequest.builder()
                .bucket(BUCKET)
                .key(KEY)
                .build(), AsyncRequestBody.fromFile(Paths.get("/Users/vincent/Projects/e-attestations/_EA_API/ea-api-aws-preferences/src/test/resources/test.txt"))
        )
                .whenComplete { resp, err ->
                    try {
                        if (resp != null) {
                            logger.debug(resp.toString())
                        } else {
                            // Handle error
                            err.printStackTrace()
                        }
                    } finally {
                        // Lets the application shut down. Only close the client when you are completely done with it.
                        FunctionalUtils.invokeSafely {
                            S3Client.close()
                        }
                    }
                }
    }

    /**
     * Retrieves a file from a S3 Bucket to the local filesystem
     */
    @Test
    @DisplayName("Let's get a file content from an S3 Bucket Asynchonouslty")
    fun getFileFromS3Bucket() {
        val future = S3Client!!.getObject(GetObjectRequest.builder()
                .bucket(BUCKET)
                .key(KEY)
                .build(), AsyncResponseTransformer.toFile(Paths.get("/Users/vincent/Projects/e-attestations/_EA_API/ea-api-aws-preferences/src/test/resources/fromS3/test-fromS3.txt"))
        )
                .whenComplete { resp, err ->
                    try {
                        if (resp != null) {
                            logger.debug(resp.toString())
                        } else {
                            // Handle error
                            err.printStackTrace()
                        }
                    } finally {
                        // Lets the application shut down. Only close the client when you are completely done with it.
                        FunctionalUtils.invokeSafely {
                            S3Client.close()
                        }
                    }
                }

        logger.debug("#### Now waiting for the response to come...")
        val start: LocalDateTime = LocalDateTime.now()
        var end: LocalDateTime = LocalDateTime.now()
        while (
                (!future.isDone || !future.isCancelled)
                && (Duration.between(start, end).toMillis() < 5000)
        ) {
            end = LocalDateTime.now()
            if (future.isDone) {
                break
            }
        }
        logger.debug("#### Exiting...")

    }

    /**
     * Creates a DynamoDB table
     */
    @Test
    @DisplayName("Let's create a table")
    fun createDynamoDBTable() {
        logger.debug("Not implemented yet")
        assert(false)
    }

    /**
     * Delete a DynamoDB table
     */
    @Test
    @DisplayName("Let's delete a table")
    fun deleteDynamoDBTable() {
        logger.debug("Not implemented yet")
        assert(false)
    }

    /**
     * Inserts some data within a dynamoDB table
     */
    @Test
    @DisplayName("Let's insert data into a table")
    fun insertsDataFromDynamoDBTable() {
        logger.debug("Not implemented yet")
        assert(false)
    }

    /**
     * Updates some data within a dynamoDB table
     */
    @Test
    @DisplayName("Let's update data from a table")
    fun updatesDataFromDynamoDBTable() {
        logger.debug("Not implemented yet")
        assert(false)
    }


    /**
     * Deletes some data within a dynamoDB table
     */
    @Test
    @DisplayName("Let's delete data from a table")
    fun deleteDataFromDynamoDBTable() {
        logger.debug("Not implemented yet")
        assert(false)
    }

}

