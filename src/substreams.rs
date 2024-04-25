use std::{fmt::Display, sync::Arc, time::Duration};

use anyhow::anyhow;
use http::{uri::Scheme, Uri};
use revm_primitives::HashMap;
use serde::Deserialize;

use tonic::{
    codegen::http,
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig},
};

use crate::pb::sf::substreams::rpc::v2::{stream_client::StreamClient, Request, Response};

#[derive(Deserialize)]
struct SFRes {
    token: String,
    // expires_at: u64, // Using u64 for timestamps is typical in Rust
}

#[derive(Clone, Debug)]
pub struct SubstreamsEndpoint {
    pub uri: String,
    pub token: Option<String>,
    channel: Channel,
}

impl Display for SubstreamsEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.uri.as_str(), f)
    }
}

impl SubstreamsEndpoint {
    pub async fn new<S: AsRef<str>>(
        url: S,
        api_key: Option<String>,
    ) -> Result<Self, anyhow::Error> {
        let uri = url
            .as_ref()
            .parse::<Uri>()
            .expect("the url should have been validated by now, so it is a valid Uri");

        let client = reqwest::Client::new();

        let mut map = HashMap::new();

        // Insert the api_key as a reference to a string slice (`&str`).

        let data = format!(
            r#"{{"api_key": "{}", "lifetime": {}}}"#,
            api_key.unwrap(),
            "3600"
        );

        map.insert("lifetime", "3600");
        let response = client
            .post("https://auth.streamingfast.io/v1/auth/issue")
            .header("Content-Type", "application/json") // Explicitly set the content type.
            .body(data.to_string())
            .send() // Send the request.
            .await?; // Wait for the response.

        let token;
        if response.status().is_success() {
            let sf_res: SFRes = response.json().await?;
            token = Some(sf_res.token);
        } else {
            eprintln!("Failed to get a successful response: {}", response.status());
            if let Ok(err_body) = response.text().await {
                eprintln!("Error details: {}", err_body);
            }
            return Err(anyhow!("failed to fetch token"));
        }

        let endpoint = match uri.scheme().unwrap_or(&Scheme::HTTP).as_str() {
            "http" => Channel::builder(uri),
            "https" => Channel::builder(uri)
                .tls_config(ClientTlsConfig::new())
                .expect("TLS config on this host is invalid"),
            _ => panic!("invalid uri scheme for firehose endpoint"),
        }
        .connect_timeout(Duration::from_secs(10))
        .tcp_keepalive(Some(Duration::from_secs(30)));

        let uri = endpoint.uri().to_string();
        let channel = endpoint.connect_lazy();

        Ok(SubstreamsEndpoint {
            uri,
            channel,
            token,
        })
    }

    pub async fn substreams(
        self: Arc<Self>,
        request: Request,
    ) -> Result<tonic::Streaming<Response>, anyhow::Error> {
        let token_metadata: Option<MetadataValue<tonic::metadata::Ascii>> = match self.token.clone()
        {
            Some(token) => Some(token.as_str().try_into()?),
            None => None,
        };

        let mut client = StreamClient::with_interceptor(
            self.channel.clone(),
            move |mut r: tonic::Request<()>| {
                if let Some(ref t) = token_metadata {
                    r.metadata_mut().insert("authorization", t.clone());
                }

                Ok(r)
            },
        );

        let response_stream = client.blocks(request).await?;
        let block_stream = response_stream.into_inner();

        Ok(block_stream)
    }
}
