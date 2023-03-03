package com.example.dynamodbclient.sample;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Component
public class GetItemSample {

    private final AmazonDynamoDB amazonDynamoDB;
    public GetItemSample(AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
    }

    @PostConstruct
    public void setup() {

        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", (new AttributeValue()).withS("1"));

        GetItemRequest getItemRequest = (new GetItemRequest())
                .withTableName("Member")
                .withKey(key);

        GetItemResult item = amazonDynamoDB.getItem(getItemRequest);
        System.out.println(item);

    }
}
