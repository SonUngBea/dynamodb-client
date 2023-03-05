package com.example.dynamodbclient.sample;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.example.dynamodbclient.entity.Member;
import org.joda.time.LocalDateTime;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

//@Component
public class RangeKeyQuerySample {

    private final DynamoDBMapper dynamoDBMapper;

    public RangeKeyQuerySample(DynamoDBMapper dynamoDBMapper) {
        this.dynamoDBMapper = dynamoDBMapper;
    }

    @PostConstruct
    public void setup() {
        List<Member> membersBetweenBirthday = getMembersBetweenBirthday(LocalDateTime.parse("1900-01-01"), LocalDateTime.parse("2005-01-01"));
        membersBetweenBirthday.forEach(System.out::println);
    }


    public List<Member> getMembersBetweenBirthday(LocalDateTime start, LocalDateTime end) {
        Condition condition = new Condition();
        condition.withComparisonOperator(ComparisonOperator.BETWEEN)
                .withAttributeValueList(new AttributeValue().withS(start.toString()))
                .withAttributeValueList(new AttributeValue().withS(end.toString()));

        Member member = new Member();
        member.setId("2");

        DynamoDBQueryExpression<Member> queryExpression =
                new DynamoDBQueryExpression<Member>()
                        .withHashKeyValues(member)
                        .withRangeKeyCondition("birthday", condition)
                        .withLimit(10);

        return dynamoDBMapper.query(Member.class, queryExpression);
    }

}
