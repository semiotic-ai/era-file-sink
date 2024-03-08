# Era File Sink

A Substream Sink that saves era files to disk.

## Usage

### Prerequisites

Get a StreamingFast API Token. Instructions can be found [here](https://substreams.streamingfast.io/documentation/consume/authentication#get-your-api-key).

### Running

```bash
SUBSTREAMS_API_TOKEN="<StreamingFast API Token>" cargo run -- <output_directory> <start_era>:<end_era>
```

This will save the era files to the output directory.