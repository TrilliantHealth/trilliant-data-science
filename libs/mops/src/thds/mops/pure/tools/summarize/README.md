# Mops Summarize Tool

The Mops Summarize Tool is a command-line utility designed to generate summaries of pipeline run logs for
mops. It reads JSON log files from a specified run directory and produces a summary report of function
executions, including details on total calls, cache hits, and execution timestamps.

## Usage

The Mops Summarize Tool can be run using `poetry run`. The tool accepts an optional argument specifying
the run directory. If no directory is provided, the tool will automatically select the latest run
directory based on the timestamp in the directory names.

### Command

```bash
poetry run mops-summarize [run_directory] [--sort-by name|time]
```

### Arguments

- `run_directory` (optional): The path to the pipeline run directory. If not provided, the tool will use
  the latest run directory based on the timestamp.
- `--sort-by` (optional): The sorting method for the summary report. Can be either `name` (sort by
  function name) or `time` (sort by the first call time). The default is `name`.

### Example

```bash
# Run the summarizer for the latest run directory, sorted by function name (default)
poetry run mops-summarize

# Run the summarizer for the latest run directory, sorted by the first call time
poetry run mops-summarize --sort-by time

# Run the summarizer for a specific run directory, sorted by function name
poetry run mops-summarize .mops/2024-05-30T10:33:39.012334Z-12345

# Run the summarizer for a specific run directory, sorted by the first call time
poetry run mops-summarize .mops/2024-05-30T10:33:39.012334Z-12345 --sort-by time
```

### Configuration

By default, the tool looks for run directories in the `.mops` directory. You can change this directory by
setting the configuration item `thds.mops.summary_dir` to your preferred directory path.

## Output

The tool generates a summary report of the function executions in the specified run directory. The report
includes:

- Function name
- Total calls
- Cache hits
- Executed (functions that were run and not retrieved from cache)
- Timestamps of the function calls (limited to the first 3 timestamps, with an indication of additional
  entries)

### Sample Output

```
Function '__main__:find_in_file':
  Total calls: 3
  Cache hits: 3
  Executed: 0
  Timestamps: 2024-06-11T17:20:46.631862, 2024-06-11T17:20:46.539746, 2024-06-11T17:20:46.455321

Function '__main__:mul2':
  Total calls: 4
  Cache hits: 2
  Executed: 2
  Timestamps: 2024-06-11T17:20:45.366552, 2024-06-11T17:20:45.945696, 2024-06-11T17:20:45.248452, and 1 more...
```

## Implementation Details

The tool reads each JSON log file in the specified run directory, processes the entries, and aggregates
the data to generate the summary report. Each log file contains a single log entry, with the filename
including a timestamp and a short UUID for uniqueness.

To use the tool, ensure you have your environment set up with `poetry`, navigate to the `libs/mops`
directory, and run the commands as described above.
