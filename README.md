### Download local dynamoDB
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html

### Local DynamoDB Run command
java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb





SortKey 를 추가하면 조회조건에 필수가 됨.

@DynamoDBAutoGeneratedKey 는 String 하고만 사용 가능