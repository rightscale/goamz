package dynamodb_test

import (
	"fmt"
	"github.com/rightscale/goamz/dynamodb"
	"gopkg.in/check.v1"
)

type TableSuite struct {
	TableDescriptionT dynamodb.TableDescriptionT
	DynamoDBTest
}

func (s *TableSuite) SetUpSuite(c *check.C) {
	setUpAuth(c)
	s.DynamoDBTest.TableDescriptionT = s.TableDescriptionT
	s.server = dynamodb.New(dynamodb_auth, dynamodb_region)
	pk, err := s.TableDescriptionT.BuildPrimaryKey()
	if err != nil {
		c.Skip(err.Error())
	}
	s.table = s.server.NewTable(s.TableDescriptionT.TableName, pk)

	// Cleanup
	s.TearDownSuite(c)
}

var table_suite = &TableSuite{
	TableDescriptionT: dynamodb.TableDescriptionT{
		TableName: "DynamoDBTestMyTable",
		AttributeDefinitions: []dynamodb.AttributeDefinitionT{
			dynamodb.AttributeDefinitionT{"TestHashKey", "S"},
			dynamodb.AttributeDefinitionT{"TestRangeKey", "N"},
		},
		KeySchema: []dynamodb.KeySchemaT{
			dynamodb.KeySchemaT{"TestHashKey", "HASH"},
			dynamodb.KeySchemaT{"TestRangeKey", "RANGE"},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughputT{
			ReadCapacityUnits:  1,
			WriteCapacityUnits: 1,
		},
	},
}

var table_suite_gsi = &TableSuite{
	TableDescriptionT: dynamodb.TableDescriptionT{
		TableName: "DynamoDBTestMyTable2",
		AttributeDefinitions: []dynamodb.AttributeDefinitionT{
			dynamodb.AttributeDefinitionT{"UserId", "S"},
			dynamodb.AttributeDefinitionT{"OSType", "S"},
			dynamodb.AttributeDefinitionT{"IMSI", "S"},
		},
		KeySchema: []dynamodb.KeySchemaT{
			dynamodb.KeySchemaT{"UserId", "HASH"},
			dynamodb.KeySchemaT{"OSType", "RANGE"},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughputT{
			ReadCapacityUnits:  1,
			WriteCapacityUnits: 1,
		},
		GlobalSecondaryIndexes: []dynamodb.GlobalSecondaryIndexT{
			dynamodb.GlobalSecondaryIndexT{
				IndexName: "IMSIIndex",
				KeySchema: []dynamodb.KeySchemaT{
					dynamodb.KeySchemaT{"IMSI", "HASH"},
				},
				Projection: dynamodb.ProjectionT{
					ProjectionType: "KEYS_ONLY",
				},
				ProvisionedThroughput: dynamodb.ProvisionedThroughputT{
					ReadCapacityUnits:  1,
					WriteCapacityUnits: 1,
				},
			},
		},
	},
}

func (s *TableSuite) TestCreateListUpdateDescribeTableGsi(c *check.C) {
	status, err := s.server.CreateTable(s.TableDescriptionT)
	if err != nil {
		fmt.Printf("err %#v", err)
		c.Fatal(err)
	}
	if status != "ACTIVE" && status != "CREATING" {
		c.Error("Expect status to be ACTIVE or CREATING")
	}

	s.WaitUntilStatus(c, "ACTIVE")

	tables, err := s.server.ListTables()
	if err != nil {
		c.Fatal(err)
	}
	c.Check(len(tables), check.Not(check.Equals), 0)
	c.Check(findTableByName(tables, s.TableDescriptionT.TableName), check.Equals, true)

	// Update throughput
	provisionedThroughput := s.TableDescriptionT.ProvisionedThroughput
	updateTableDescriptionT := dynamodb.TableDescriptionT{
		TableName: s.TableDescriptionT.TableName,
		ProvisionedThroughput: dynamodb.ProvisionedThroughputT{
			ReadCapacityUnits:  provisionedThroughput.ReadCapacityUnits + 1,
			WriteCapacityUnits: provisionedThroughput.WriteCapacityUnits + 1,
		},
	}

	status, err = s.server.UpdateTable(updateTableDescriptionT)
	if err != nil {
		fmt.Printf("err %#v", err)
		c.Fatal(err)
	}

	if status != "ACTIVE" && status != "UPDATING" {
		c.Error("Expect status to be ACTIVE or UPDATING")
	}

	s.WaitUntilStatus(c, "ACTIVE")

	// Verify throughput got updated
	updatedTableDescriptionT, err := s.server.DescribeTable(s.TableDescriptionT.TableName)
	if err != nil {
		c.Fatal(err)
	}

	updateProvisionedThroughput := updateTableDescriptionT.ProvisionedThroughput
	updatedProvisionedThroughput := updatedTableDescriptionT.ProvisionedThroughput

	c.Check(updatedProvisionedThroughput.ReadCapacityUnits, check.Equals,
		updateProvisionedThroughput.ReadCapacityUnits)
	c.Check(updatedProvisionedThroughput.WriteCapacityUnits, check.Equals,
		updateProvisionedThroughput.WriteCapacityUnits)
}

var _ = check.Suite(table_suite)
var _ = check.Suite(table_suite_gsi)
