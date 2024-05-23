/**
 * Low level stream reader using AWS SDK2
 */
package software.ddb.stream;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.dynamodb.streams.endpoints.internal.Value;
import software.ddb.stream.utils.FileStore;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DynamodbStreamReader {
    public static void main(String[] args) throws IOException {
        Map<String,String> lookup= FileStore.retrive();
        if(Objects.isNull(lookup)) lookup=new HashMap<String,String>();
        Region region = Region.AP_SOUTHEAST_2;
        DynamoDbClient dynamoDBClient = DynamoDbClient.builder()
                .region(region)
                .build();
        DynamoDbStreamsClient streamsClient=DynamoDbStreamsClient.builder().region(region).build();

        // Create a table, with a stream enabled
        String tableName = "dataengStreamTest";
        DescribeTableRequest dtr= DescribeTableRequest.builder().tableName(tableName).build();
        DescribeTableResponse describeTableResult = dynamoDBClient.describeTable(dtr);
        String streamArn=describeTableResult.table().latestStreamArn();
        System.out.println("Current stream ARN for " + tableName + ": " +
                streamArn);
        DescribeStreamRequest dsr=DescribeStreamRequest.builder().streamArn(streamArn).build();
        DescribeStreamResponse dsre=streamsClient.describeStream(dsr);
       var shards=dsre.streamDescription().shards();
       var maxItemCount=1000;
        for(var shard: shards) {
            GetShardIteratorRequest gsit=null;
            if(lookup.containsKey(shard.shardId())){
               String lastSeq= lookup.get(shard.shardId());
                 gsit= GetShardIteratorRequest.builder().streamArn(streamArn).shardId(shard.shardId())
                        .sequenceNumber(lastSeq)
                        .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER).build();
            }else {
             gsit= GetShardIteratorRequest.builder().streamArn(streamArn).shardId(shard.shardId())
                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON).build();
            }
            var currentShardIter=streamsClient.getShardIterator(gsit).shardIterator();
            int processedRecordCount = 0;
            int maxEmpty=100;
            int currentEmpty=0;
            while (currentShardIter != null && processedRecordCount < maxItemCount && currentEmpty < maxEmpty) {
                GetRecordsRequest grr=GetRecordsRequest.builder().shardIterator(currentShardIter).build();
                GetRecordsResponse getRecordsResult = streamsClient
                        .getRecords(grr);
                List<Record> records = getRecordsResult.records();
                for (Record record : records) {
                    System.out.println("        " + record.dynamodb() +" "+record.eventName());
                    lookup.put(shard.shardId(),record.dynamodb().sequenceNumber());
                }
                if(records.size() ==0){
                    currentEmpty++;
                }
                processedRecordCount += records.size();
                currentShardIter = getRecordsResult.nextShardIterator();
            }

            FileStore.store(lookup);

        }

    }
}

