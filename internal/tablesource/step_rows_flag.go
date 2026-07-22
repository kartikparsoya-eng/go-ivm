package tablesource

import "os"

// UseStepRowsShim controls whether the C step-rows shim is used instead
// of the standard database/sql Rows.Next + Scan path. Enabled by env var
// GO_IVM_STEP_ROWS_SHIM=1. When disabled, all queries use the standard
// database/sql path (the default, safe for production).
var UseStepRowsShim = os.Getenv("GO_IVM_STEP_ROWS_SHIM") == "1"
