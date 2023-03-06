package com.example.dynamodbclient.sample.rangekey;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.example.dynamodbclient.sample.rangekey.entity.Member;
import org.joda.time.LocalDateTime;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;


//@Component
public class SimpleCRUDRunner {

    private final DynamoDBMapper dynamoDBMapper;
    private final AmazonDynamoDB amazonDynamoDB;

    public SimpleCRUDRunner(DynamoDBMapper dynamoDBMapper, AmazonDynamoDB amazonDynamoDB) {
        this.dynamoDBMapper = dynamoDBMapper;
        this.amazonDynamoDB = amazonDynamoDB;
    }

    @PostConstruct
    public void setup() {

        deleteTable();
        createTable();

        System.out.println("################ GET ITEM ################");
        System.out.println("Get Result : " + this.get("1", LocalDateTime.parse("1992-11-26T10:30:00.000")));
        System.out.println("################ GET ITEM ################");

        LocalDateTime birthday = LocalDateTime.now();
        Member member = new Member();
        member.setId("3");
        member.setName("김철수");
        member.setBirthday(birthday);
        System.out.println("################ PUT ITEM ################");
        this.put(member);

        System.out.println("Put Result : " + this.get("3", birthday));
        System.out.println("################ PUT ITEM ################");

        System.out.println("################ DELETE ITEM ################");
        Member deleteTarget = this.get("3", birthday);
        System.out.println("Before Delete : " + deleteTarget);
        this.delete(deleteTarget);
        System.out.println("After Delete : " + this.get("3", birthday));
        System.out.println("################ DELETE ITEM ################");

    }

    private void createTable() {
        CreateTableRequest createTableRequest = dynamoDBMapper.generateCreateTableRequest(Member.class);
        // Amazon SDK 버그로 Local DynamoDB 에 사용되지 않는 ProvisionedThroughput 값이라도 세팅해주어야 테이블 생성됨.
        // referer link : https://github.com/99x/serverless-dynamodb-local/issues/189
        createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(1000L, 1000L));
        amazonDynamoDB.createTable(createTableRequest);

        Member member1 = new Member();
        member1.setId("1");
        member1.setName("손웅배");
        member1.setBirthday(LocalDateTime.parse("1992-11-26T10:30:00.000"));
        put(member1);

        Member member2 = new Member();
        member2.setId("2");
        member2.setName("락토핏_형");
        member2.setBirthday(LocalDateTime.parse("2003-01-26T09:30:00.000"));
        put(member2);

        Member member3 = new Member();
        member3.setId("2");
        member3.setName("락토핏_동생");
        member3.setBirthday(LocalDateTime.parse("2023-01-01T01:30:00.000"));
        put(member3);

        List<Member> members = getAllItems();
        System.out.println("################ INIT TABLE ################");
        members.forEach(System.out::println);
        System.out.println("################ INIT TABLE ################");

    }

    private void deleteTable() {
        DeleteTableRequest deleteTableRequest = dynamoDBMapper.generateDeleteTableRequest(Member.class);
        amazonDynamoDB.deleteTable(deleteTableRequest);
    }

    private List<Member> getAllItems() {
        return dynamoDBMapper.scan(Member.class, new DynamoDBScanExpression());
    }

    private Member get(String id, LocalDateTime birthday) {
        return dynamoDBMapper.load(Member.class, id, birthday);
    }

    private void put(Member member) {
        dynamoDBMapper.save(member);
    }

    private void delete(Member member) {
        dynamoDBMapper.delete(member);
    }
}
