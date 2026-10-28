# Storage cleanup actions

The cleanup apply endpoint accepts optional snapshot preconditions:

```json
{
  "expected_git_revision": "<git_revision from the audit summary>",
  "expected_syfon_revision": "<syfon_revision from the audit summary>"
}
```

Send both values from the audit that produced the selected findings on dry-run and actual apply requests. Gecko compares each supplied value with the current Git commit and Syfon record revision before planning cleanup. A mismatch returns HTTP `409 Conflict` with a message asking the caller to refresh the audit. If Gecko cannot load the current Syfon revision, it fails the request before cleanup planning or mutation.

The fields are optional for compatibility with existing API callers. When omitted, cleanup keeps its existing behavior, including its physical storage revalidation. Revision checks also apply to dry-run requests.
