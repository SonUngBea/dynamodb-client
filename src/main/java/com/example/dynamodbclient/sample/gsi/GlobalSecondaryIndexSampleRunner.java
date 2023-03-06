package com.example.dynamodbclient.sample.gsi;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.*;
import com.example.dynamodbclient.sample.gsi.entity.Movie;
import com.example.dynamodbclient.sample.rangekey.entity.Member;
import org.joda.time.LocalDateTime;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;

@Component
public class GlobalSecondaryIndexSampleRunner {

    private final AmazonDynamoDB amazonDynamoDB;
    private final DynamoDBMapper dynamoDBMapper;

    public GlobalSecondaryIndexSampleRunner(AmazonDynamoDB amazonDynamoDB, DynamoDBMapper dynamoDBMapper) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.dynamoDBMapper = dynamoDBMapper;
    }

    @PostConstruct
    public void setup() {
        deleteTable();
        createTable();

        System.out.println("################## Get with GSI ################");
        getMoviesWithGenre("Action").stream().map(movie -> get(movie.getId())).forEach(System.out::println);
        System.out.println("################## Get with GSI ################");

    }

    public List<Movie> getMoviesWithGenre(String genre) {

        HashMap<String, AttributeValue> eav = new HashMap<>();
        eav.put(":v_genre", new AttributeValue().withS(genre));

        DynamoDBQueryExpression<Movie> expression = new DynamoDBQueryExpression<>();
        expression.withIndexName("genre-index")
                .withKeyConditionExpression("genre = :v_genre")
                .withExpressionAttributeValues(eav)
                .withConsistentRead(false);

        return dynamoDBMapper.query(Movie.class, expression);
    }

    // Key 가 아닌 Attribute 는 조회 불가능
    public List<Movie> getMoviesWithGenreAndTitle(String genre, String title) {

        HashMap<String, AttributeValue> eav = new HashMap<>();
        eav.put(":v_genre", new AttributeValue().withS(genre));
        eav.put(":v_title", new AttributeValue().withS(title));

        DynamoDBQueryExpression<Movie> expression = new DynamoDBQueryExpression<>();
        expression.withIndexName("genre-index")
                .withKeyConditionExpression("genre = :v_genre AND title = :v_title")
                .withExpressionAttributeValues(eav)
                .withConsistentRead(false);

        return dynamoDBMapper.query(Movie.class, expression);
    }

    public void deleteTable() {
        DeleteTableRequest deleteTableRequest = dynamoDBMapper.generateDeleteTableRequest(Movie.class);
        amazonDynamoDB.deleteTable(deleteTableRequest);
    }

    public void createTable() {
        CreateTableRequest createTableRequest = dynamoDBMapper.generateCreateTableRequest(Movie.class);
        createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(1000L, 1000L));

        createTableRequest.setGlobalSecondaryIndexes(buildGSI());
        amazonDynamoDB.createTable(createTableRequest);

        Movie movie1 = new Movie();
        movie1.setDirector("Pierre Morel");
        movie1.setGenre("Action");
        movie1.setActors(new HashSet<>(Arrays.asList("Liam Neeson", "Maggie Grace")));
        movie1.setTitle("Taken");

        put(movie1);

        Movie movie2 = new Movie();
        movie2.setDirector("Antoine Fuqua");
        movie2.setGenre("Action");
        movie2.setActors(new HashSet<>(Arrays.asList("Kate Mara", "Levon Helm")));
        movie2.setTitle("Shooter");

        put(movie2);

        Movie movie3 = new Movie();
        movie3.setDirector("Paramount Pictures");
        movie3.setGenre("Super Hero");
        movie3.setActors(new HashSet<>(Arrays.asList("Robert Downey Jr.")));
        movie3.setTitle("Iron Man");

        put(movie3);

        List<Movie> movies = getAllItems();
        System.out.println("################ INIT TABLE ################");
        movies.forEach(System.out::println);
        System.out.println("################ INIT TABLE ################");

    }

    private List<Movie> getAllItems() {
        return dynamoDBMapper.scan(Movie.class, new DynamoDBScanExpression());
    }

    private Movie get(String id) {
        return dynamoDBMapper.load(Movie.class, id);
    }

    private void put(Movie movie) {
        dynamoDBMapper.save(movie);
    }


    private HashSet<GlobalSecondaryIndex> buildGSI() {
        HashSet<GlobalSecondaryIndex> globalSecondaryIndexes = new HashSet<>();
        GlobalSecondaryIndex genreGSI = new GlobalSecondaryIndex();

        genreGSI.setProvisionedThroughput(new ProvisionedThroughput(1000L, 1000L));
        genreGSI.setIndexName("genre-index");

        Projection genreIndexProjection = new Projection();
        genreIndexProjection.setProjectionType(ProjectionType.KEYS_ONLY);
        genreGSI.setProjection(genreIndexProjection);

        List<KeySchemaElement> keySchemaElements = new ArrayList<>();

        KeySchemaElement genreKeySchemaElement = new KeySchemaElement();
        genreKeySchemaElement.setKeyType(KeyType.HASH);
        genreKeySchemaElement.setAttributeName("genre");

        keySchemaElements.add(genreKeySchemaElement);
        genreGSI.setKeySchema(keySchemaElements);

        globalSecondaryIndexes.add(genreGSI);
        return globalSecondaryIndexes;
    }
}
