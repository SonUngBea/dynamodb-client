package com.example.dynamodbclient.entity;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
import org.joda.time.LocalDateTime;

// With RangeKey
@DynamoDBTable(tableName = "Member")
public class Member {
    @DynamoDBHashKey(attributeName = "id")
    private String id;

    @DynamoDBRangeKey
    @DynamoDBAttribute
    @DynamoDBTypeConverted(converter = LocalDateTimeConverter.class)
    private LocalDateTime birthday;

    @DynamoDBAttribute
    private String name;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LocalDateTime getBirthday() {
        return birthday;
    }

    public void setBirthday(LocalDateTime birthday) {
        this.birthday = birthday;
    }

    @Override
    public String toString() {
        return "Member{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", birthday='" + birthday + '\'' +
                '}';
    }

    static public class LocalDateTimeConverter implements DynamoDBTypeConverter<String, LocalDateTime> {

        @Override
        public String convert( final LocalDateTime time ) {
            return time.toString();
        }

        @Override
        public LocalDateTime unconvert( final String stringValue ) {
            return LocalDateTime.parse(stringValue);
        }
    }
}
