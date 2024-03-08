use anyhow::{format_err, Context, Error};
use futures03::StreamExt;
use pb::sf::substreams::rpc::v2::BlockScopedData;
use pb::sf::substreams::v1::Package;
use std::io::Write;

use crate::e2store::builder::EraBuilder;
use crate::header_accumulator::{get_epoch, EPOCH_SIZE};
use crate::pb::acme::verifiable_block::v1::VerifiableBlock;
use prost::Message;
use std::{env, process::exit, sync::Arc};
use substreams::SubstreamsEndpoint;
use substreams_stream::{BlockResponse, SubstreamsStream};

mod e2store;
mod header_accumulator;
mod pb;
mod reth_mappings;
mod rlp;
mod snap;
mod substreams;
mod substreams_stream;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = env::args();
    if args.len() < 2 || args.len() > 3 {
        println!("usage: stream <output_dir> <start_era>:<stop_era>");
        println!();
        println!("The environment variable SUBSTREAMS_API_TOKEN must also be set");
        println!("and should contain a valid Substream API token.");
        exit(1);
    }

    const ENDPOINT_URL: &str = "https://mainnet.eth.streamingfast.io:443";
    const PACKAGE_FILE: &str = "https://spkg.io/semiotic-ai/era-file-substream-v1.0.1.spkg";
    const MODULE_NAME: &str = "map_block";

    let output_dir = env::args().nth(1).expect("output_dir not provided");

    let token_env = env::var("SUBSTREAMS_API_TOKEN").expect("SUBSTREAMS_API_TOKEN not set");
    if token_env.is_empty() {
        println!("The environment variable SUBSTREAMS_API_TOKEN must be set and contain a valid Substream API token.");
        exit(1);
    }

    let token: Option<String> = Some(token_env);

    let package = read_package(&PACKAGE_FILE).await?;
    let block_range = read_block_range()?;
    let endpoint = Arc::new(SubstreamsEndpoint::new(&ENDPOINT_URL, token).await?);

    let cursor: Option<String> = load_persisted_cursor()?;

    let mut stream = SubstreamsStream::new(
        endpoint.clone(),
        cursor,
        package.modules.clone(),
        MODULE_NAME.to_string(),
        block_range.0,
        block_range.1,
    );

    let header_accumulator_values = header_accumulator::read_values();

    let mut writer = std::fs::File::create(format!(
        "{}/era-{}.era1",
        output_dir,
        get_epoch(block_range.0 as u64)
    ))?;
    let mut builder = EraBuilder::new(writer.try_clone()?);
    loop {
        match process_iteration(&mut stream, &mut builder, header_accumulator_values.clone()).await
        {
            Ok(finished_era) => {
                if finished_era {
                    writer = std::fs::File::create(format!(
                        "{}/era-{}.era1",
                        output_dir,
                        get_epoch(builder.starting_number as u64 + EPOCH_SIZE)
                    ))?;
                    builder.reset(writer.try_clone()?);
                }
            }
            Err(err) => {
                if !err.to_string().is_empty() {
                    println!("Error: {}", err);
                }

                break;
            }
        }
    }

    Ok(())
}

async fn process_iteration<W: Write>(
    stream: &mut SubstreamsStream,
    builder: &mut EraBuilder<W>,
    header_accumulator_values: Vec<String>,
) -> Result<bool, anyhow::Error> {
    match stream.next().await {
        None => {
            Err(anyhow::anyhow!(""))
        }
        Some(Ok(BlockResponse::New(data))) => {
            process_block_scoped_data(&data, builder)?;

            if builder.len() == EPOCH_SIZE as usize {
                match header_accumulator::get_value_for_block(
                    &header_accumulator_values,
                    builder.starting_number as u64,
                ) {
                    Some(value) => {
                        let header_accumulator_value = hex::decode(value)?;
                        builder.finalize(header_accumulator_value)?;

                        Ok(true)
                    }
                    None => Err(anyhow::anyhow!(
                        "Error, no header acc value found for block: {}",
                        builder.starting_number
                    )),
                }
            } else {
                Ok(false)
            }
        }
        Some(Ok(BlockResponse::Undo(_))) => {
            Err(anyhow::anyhow!("Error, undo signal not supported"))
        }
        Some(Err(err)) => Err(anyhow::anyhow!(
            "Error, stream terminated with error, {}",
            err
        )),
    }
}

fn process_block_scoped_data<W: Write>(
    data: &BlockScopedData,
    builder: &mut EraBuilder<W>,
) -> Result<(), Error> {
    let output = data.output.as_ref().unwrap().map_output.as_ref().unwrap();

    let block = VerifiableBlock::decode(output.value.as_slice())?;
    builder.add(block)?;

    Ok(())
}

fn load_persisted_cursor() -> Result<Option<String>, anyhow::Error> {
    // FIXME: Handling of the cursor is missing here. It should be loaded from
    // somewhere (local file, database, cloud storage) and then `SubstreamStream` will
    // be able correctly resume from the right block.
    Ok(None)
}

fn read_block_range() -> Result<(i64, u64), anyhow::Error> {
    let input: String = env::args().nth(2).expect("Era range not provided");
    let (prefix, suffix) = match input.split_once(':') {
        Some((prefix, suffix)) => (prefix.to_string(), suffix.to_string()),
        None => ("".to_string(), input),
    };

    let start: i64 = match prefix.as_str() {
        "" => 0,
        x => x
            .parse::<i64>()
            .context("argument <start> is not a valid integer")?,
    };

    let stop: u64 = suffix.parse::<u64>().context("argument <stop> is not a valid integer")?;

    let start = start * EPOCH_SIZE as i64;
    let stop = (stop+1) * EPOCH_SIZE;

    Ok((start, stop))
}

async fn read_package(input: &str) -> Result<Package, anyhow::Error> {
    if input.starts_with("http") {
        return read_http_package(input).await;
    }

    // Assume it's a local file

    let content =
        std::fs::read(input).context(format_err!("read package from file '{}'", input))?;
    Package::decode(content.as_ref()).context("decode command")
}

async fn read_http_package(input: &str) -> Result<Package, anyhow::Error> {
    let body = reqwest::get(input).await?.bytes().await?;

    Package::decode(body).context("decode command")
}
