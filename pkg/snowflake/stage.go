package snowflake

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	// InternalSchemaName is the Snowflake schema used for tidb2snowflake-owned objects.
	// It is not mapped from any upstream TiDB/MySQL database.
	InternalSchemaName = "TIDB2SNOWFLAKE_INTERNAL"
	// ExternalStageName is the single external stage shared by snapshot and incremental loads.
	ExternalStageName = "tidb2snowflake_external"
)

func (sc *Connector) CreateStage(ctx context.Context, storageURI *url.URL, cred *credentials.Value) error {
	// A Snowflake named stage must belong to a schema. Keep it in an internal
	// schema so business schemas can stay mapped from upstream databases.
	// The full stage name is:
	// "<target_database>"."TIDB2SNOWFLAKE_INTERNAL"."TIDB2SNOWFLAKE_EXTERNAL".
	if _, err := sc.db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", quoteQualifiedIdent(sc.TargetDatabase, InternalSchemaName))); err != nil {
		return errors.Annotate(err, "Failed to create internal schema")
	}

	stageName := quoteQualifiedIdent(sc.TargetDatabase, InternalSchemaName, ExternalStageName)
	stageURL := stageRootURL(storageURI)
	sql := fmt.Sprintf(`CREATE OR REPLACE STAGE %s URL = '%s' CREDENTIALS = (AWS_KEY_ID = '%s' AWS_SECRET_KEY = '%s' AWS_TOKEN = '%s')
				FILE_FORMAT = (type = 'CSV' EMPTY_FIELD_AS_NULL = FALSE NULL_IF=('\\N') FIELD_OPTIONALLY_ENCLOSED_BY='"' ESCAPE='\\' BINARY_FORMAT = 'HEX');`,
		stageName, escapeString(stageURL), escapeString(cred.AccessKeyID), escapeString(cred.SecretAccessKey), escapeString(cred.SessionToken))
	if _, err := sc.db.ExecContext(ctx, sql); err != nil {
		log.Error("Snowflake external stage creation failed",
			zap.String("stage", stageName),
			zap.String("stageURL", stageURL),
			zap.Error(err))
		return errors.Annotate(err, "Failed to create stage")
	}
	log.Info("Snowflake external stage created",
		zap.String("stage", stageName),
		zap.String("stageURL", stageURL))
	return nil
}

func stageRootURL(storageURI *url.URL) string {
	rootPath := storageURI.EscapedPath()
	if rootPath == "" {
		rootPath = "/"
	}
	if !strings.HasSuffix(rootPath, "/") {
		rootPath += "/"
	}
	return fmt.Sprintf("%s://%s%s", storageURI.Scheme, storageURI.Host, rootPath)
}

func (sc *Connector) DropStage(ctx context.Context) {
	stageName := quoteQualifiedIdent(sc.TargetDatabase, InternalSchemaName, ExternalStageName)
	sql := fmt.Sprintf(`DROP STAGE IF EXISTS %s;`, stageName)
	if _, err := sc.db.ExecContext(ctx, sql); err != nil {
		log.Error("snowflake drop external stage failed", zap.String("stage", stageName), zap.Error(err))
		return
	}
	log.Info("snowflake external stage dropped", zap.String("stage", stageName))
}
