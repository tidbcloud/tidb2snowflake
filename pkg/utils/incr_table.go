package utils

import "github.com/pingcap/tiflow/pkg/sink/cloudstorage"

var (
	CDCFlagColumnName       = "tidb2snow_flag"
	CDCTablenameColumnName  = "tidb2snow_tablename"
	CDCSchemanameColumnName = "tidb2snow_schemaname"
	CDCCommitTsColumnName   = "tidb2snow_commit_ts"
)

func GenIncrementTableColumns(columns []cloudstorage.TableCol) []cloudstorage.TableCol {
	return append([]cloudstorage.TableCol{
		{
			Name: CDCFlagColumnName,
			Tp:   "varchar",
		},
		{
			Name: CDCTablenameColumnName,
			Tp:   "varchar",
		},
		{
			Name: CDCSchemanameColumnName,
			Tp:   "varchar",
		},
		{
			Name: CDCCommitTsColumnName,
			Tp:   "bigint",
		},
	}, columns...)
}
