# Parsl Monitoring to WFCommons Converter

This Python script parses workflow execution data collected from Parsl’s monitoring database and converts it into a WFCommons-compliant JSON format. It allows analysis, visualization, and sharing of workflow executions in a standard structure.


## Usage

Run the script from the command line:

```bash
python parsl_to_wfcommons.py -o OUTPUT_JSON [-p DB_PATH] [-i DB_FILENAME] [-r RUN_ID]
```

### Arguments

| Argument       | Description                                                                     |
| -------------- | ------------------------------------------------------------------------------- |
| `-p, --path`   | Path to the folder containing the monitoring database. Defaults to `./runinfo`. |
| `-i, --input`  | Database filename. Defaults to `monitoring.db`.                                 |
| `-r, --runid`  | Specific workflow run ID to parse and export. If omitted, all runs are parsed.  |
| `-o, --output` | Output JSON filename or base name for multiple runs.                            |

---

### Examples

**Parse and export a single workflow run:**

```bash
python parsl_to_wfcommons.py -p ./runinfo -i monitoring.db -r 123 -o workflow_123.json
```

**Parse and export all workflow runs in the database:**

```bash
python parsl_to_wfcommons.py -p ./runinfo -i monitoring.db -o workflow_all.json
```

This will create separate JSON files for each run, e.g., `workflow_all_123.json`, `workflow_all_124.json`, etc.

---
# Output Format

The resulting JSON follows the **WFCommons schema** (v1.5), containing:

- `name`: Workflow name
- `description`: Placeholder (can be updated manually)
- `createdAt`: Workflow start time
- `workflow.specification.tasks`: List of tasks with parent/child relationships
- `workflow.execution.tasks`: Task execution runtimes
- `workflow.execution.makespanInSeconds`: Total workflow runtime
- `author` and `runtimeSystem`: Metadata placeholders

---

## Notes

- Only workflows with fully parsed tasks and valid timestamps are exported.
- Tasks with invalid timestamps or negative runtimes are ignored.
- Workflows with failed tasks are skipped.
- Source (`-1`) and sink (`-2`) nodes are automatically added to the DAG to simplify analysis.

## References:

- WFCommons Schema v1.5: https://wfcommons.github.io/schema/
- WFCommons Project: https://wfcommons.github.io/

