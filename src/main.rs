use anyhow::{format_err, Context, Error};
use futures03::StreamExt;
use pb::sf::substreams::rpc::v2::BlockScopedData;
use pb::sf::substreams::v1::Package;
use std::io::Write;

use crate::e2store::EraBuilder;
use crate::header_accumulator::{get_epoch, EPOCH_SIZE};
use crate::pb::acme::verifiable_block::v1::VerifiableBlock;
use prost::Message;
use std::{env, process::exit, sync::Arc};
use substreams::SubstreamsEndpoint;
use substreams_stream::{BlockResponse, SubstreamsStream};

mod e2store;
mod header_accumulator;
mod pb;
mod substreams;
mod substreams_stream;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = env::args();
    if args.len() < 5 || args.len() > 6 {
        println!("usage: stream <endpoint> <spkg> <module> <output_dir> [<start>:<stop>]");
        println!();
        println!("The environment variable SUBSTREAMS_API_TOKEN must be set also");
        println!("and should contain a valid Substream API token.");
        exit(1);
    }

    let endpoint_url = env::args().nth(1).unwrap();
    let package_file = env::args().nth(2).unwrap();
    let module_name = env::args().nth(3).unwrap();
    let output_dir = env::args().nth(4).unwrap();

    let token_env = env::var("SUBSTREAMS_API_TOKEN").unwrap_or("".to_string());
    let mut token: Option<String> = None;
    if !token_env.is_empty() {
        token = Some(token_env);
    }

    let package = read_package(&package_file).await?;
    let block_range = read_block_range(&package, &module_name)?;
    let endpoint = Arc::new(SubstreamsEndpoint::new(&endpoint_url, token).await?);

    let cursor: Option<String> = load_persisted_cursor()?;

    let mut stream = SubstreamsStream::new(
        endpoint.clone(),
        cursor,
        package.modules.clone(),
        module_name.to_string(),
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
                println!("Error: {}", err);
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
            println!("Stream consumed");
            Err(anyhow::anyhow!("Stream consumed"))
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
                        println!(
                            "Finalizing era with header accumulator value: {:x?}",
                            header_accumulator_value
                        );
                        builder.finalize(header_accumulator_value)?;
                        println!("Finalized era");

                        // let writer = std::fs::File::create(format!("{}/era-{}.e2s", output_dir, get_epoch(builder.starting_number as u64 + EPOCH_SIZE))).unwrap();
                        Ok(true)
                    }
                    None => Err(anyhow::anyhow!(
                        "Error, no header acc value fond for block: {}",
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

fn read_block_range(pkg: &Package, module_name: &str) -> Result<(i64, u64), anyhow::Error> {
    let module = pkg
        .modules
        .as_ref()
        .unwrap()
        .modules
        .iter()
        .find(|m| m.name == module_name)
        .ok_or_else(|| format_err!("module '{}' not found in package", module_name))?;

    let mut input: String = "".to_string();
    if let Some(range) = env::args().nth(5) {
        input = range;
    };

    let (prefix, suffix) = match input.split_once(':') {
        Some((prefix, suffix)) => (prefix.to_string(), suffix.to_string()),
        None => ("".to_string(), input),
    };

    let start: i64 = match prefix.as_str() {
        "" => module.initial_block as i64,
        x if x.starts_with('+') => {
            let block_count = x
                .trim_start_matches('+')
                .parse::<u64>()
                .context("argument <stop> is not a valid integer")?;

            (module.initial_block + block_count) as i64
        }
        x => x
            .parse::<i64>()
            .context("argument <start> is not a valid integer")?,
    };

    let stop: u64 = match suffix.as_str() {
        "" => 0,
        "-" => 0,
        x if x.starts_with('+') => {
            let block_count = x
                .trim_start_matches('+')
                .parse::<u64>()
                .context("argument <stop> is not a valid integer")?;

            start as u64 + block_count
        }
        x => x
            .parse::<u64>()
            .context("argument <stop> is not a valid integer")?,
    };

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
