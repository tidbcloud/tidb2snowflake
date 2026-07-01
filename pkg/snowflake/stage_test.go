package snowflake

import (
	"context"
	"net/url"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/stretchr/testify/require"
)

func TestCreateStageUsesInternalSchemaAndSingleStage(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn := &Connector{db: db, TargetDatabase: "ODS_DB"}
	cred := &credentials.Value{
		AccessKeyID:     "ak",
		SecretAccessKey: "sk",
		SessionToken:    "token",
	}

	mock.ExpectExec(regexp.QuoteMeta(`CREATE SCHEMA IF NOT EXISTS "ODS_DB"."TIDB2SNOWFLAKE_INTERNAL";`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`CREATE OR REPLACE STAGE "ODS_DB"."TIDB2SNOWFLAKE_INTERNAL"."TIDB2SNOWFLAKE_EXTERNAL" URL = 's3://bucket/root/'`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, conn.CreateStage(context.Background(), &url.URL{Scheme: "s3", Host: "bucket", Path: "/root"}, cred))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDropStageUsesInternalSchemaAndSingleStage(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn := &Connector{db: db, TargetDatabase: "ODS_DB"}
	mock.ExpectExec(regexp.QuoteMeta(`DROP STAGE IF EXISTS "ODS_DB"."TIDB2SNOWFLAKE_INTERNAL"."TIDB2SNOWFLAKE_EXTERNAL";`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	conn.DropStage(context.Background())
	require.NoError(t, mock.ExpectationsWereMet())
}
