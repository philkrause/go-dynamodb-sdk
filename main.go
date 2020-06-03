package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type Item struct {
	MongoID string
}

// func main() {
// 	checkMongoID()
// 	// writeMongoID()
// }

func getMongoIDs() {

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create DynamoDB client
	svc := dynamodb.New(sess)

	tableName := "devops-mongo-duty"

	proj := expression.NamesList(expression.Name("MongoID"))

	expr, err := expression.NewBuilder().WithProjection(proj).Build()
	if err != nil {
		fmt.Println("Got error building expression:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	params := &dynamodb.ScanInput{
		ExpressionAttributeNames: expr.Names(),
		ProjectionExpression:     expr.Projection(),
		TableName:                aws.String(tableName),
	}

	result, err := svc.Scan(params)
	if err != nil {
		fmt.Println("Query API call failed:")
		fmt.Println((err.Error()))
		os.Exit(1)
	}

	for _, i := range result.Items {

		item := Item{}

		err = dynamodbattribute.UnmarshalMap(i, &item)

		if err != nil {
			fmt.Println("Got error unmarshalling:")
			fmt.Println(err.Error())
			os.Exit(1)
		}

		fmt.Println("MongoID:", item.MongoID)
	}

}

func AddTableItem(svc dynamodbiface.DynamoDBAPI, mongoID, table string) error {
	// snippet-start:[dynamodb.go.create_new_item.assign_struct]
	item := Item{
		MongoID: mongoID,
	}

	av, err := dynamodbattribute.MarshalMap(item)
	// snippet-end:[dynamodb.go.create_new_item.assign_struct]
	if err != nil {
		return err
	}

	_, err = svc.PutItem(&dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(table),
	})
	if err != nil {
		return err
	}

	return nil
}

func main() {

	getMongoIDs()

	mongoID := "qweqwe"
	table := "devops-mongo-duty"

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := dynamodb.New(sess)
	// snippet-end:[dynamodb.go.create_new_item.session]

	err := AddTableItem(svc, mongoID, table)
	if err != nil {
		fmt.Println("Got an error adding item to table:")
		fmt.Println(err)
		return
	}

	fmt.Println("Successfully added MongoID:"+mongoID+"to table", table)
}

func writeMongoID() {
	fmt.Println("writing to dynamodb")
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	payload := Item{
		MongoID: "tesIDt",
	}

	// Create DynamoDB client
	svc := dynamodb.New(sess)

	fmt.Println(sess)
	av, err := dynamodbattribute.MarshalMap(payload)

	if err != nil {
		fmt.Println("Got error marshalling the mongo data:")
		fmt.Println(err.Error())
		os.Exit(1)

		tableName := "devops-mongo-duty"

		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String(tableName),
		}

		_, err = svc.PutItem(input)
		if err != nil {
			fmt.Println("Got error calling PutItem:")
			fmt.Println(err.Error())
			os.Exit(1)
		}

		fmt.Println("Successfully added '" + payload.MongoID + " to table " + tableName)

	}
}
