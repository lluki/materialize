// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate core;

mod login;
mod profiles;
mod regions;
mod shell;
mod utils;

use serde::{Deserialize, Serialize};

use clap::{ArgEnum, Args, Parser, Subcommand};
use reqwest::Client;

use crate::login::{login_with_browser, login_with_console};
use crate::profiles::{authenticate_profile, get_local_profile, validate_profile};
use crate::regions::{
    delete_region, enable_region, list_cloud_providers, list_regions, warning_delete_region,
};
use crate::shell::shell;

#[derive(Debug, Clone, ArgEnum)]
#[allow(non_camel_case_types)]
enum CloudProviderRegion {
    usEast_1,
    euWest_1,
}

/// Command-line interface for Materialize.
#[derive(Debug, Parser)]
#[clap(name = "Materialize CLI")]
#[clap(about = "Command-line interface for Materialize.", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Login to Materialize
    Login(Login),
    /// Enable or delete a region
    Regions(Regions),
    /// Connect to a region using a SQL shell
    Shell,
}

#[derive(Debug, Args)]
struct Login {
    #[clap(subcommand)]
    command: Option<LoginCommands>,
}

#[derive(Debug, Subcommand)]
enum LoginCommands {
    /// Log in via the console using email and password.
    Interactive,
}

#[derive(Debug, Args)]
struct Regions {
    #[clap(subcommand)]
    command: RegionsCommands,
}

#[derive(Debug, Subcommand)]
enum RegionsCommands {
    /// Enable a new region.
    Enable {
        #[clap(arg_enum)]
        cloud_provider_region: CloudProviderRegion,
    },
    /// Delete an existing region.
    Delete {
        #[clap(arg_enum)]
        cloud_provider_region: CloudProviderRegion,
    },
    /// List all enabled regions.
    List,
}

/**
 ** Internal types, struct and enums
 **/

#[derive(Debug, Deserialize)]
struct Region {
    coordd_pgwire_address: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct CloudProvider {
    region: String,
    environment_controller_url: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FronteggAuthUser {
    access_token: String,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct FronteggAuthMachine {
    access_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FronteggAPIToken {
    client_id: String,
    secret: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrowserAPIToken {
    email: String,
    client_id: String,
    secret: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Profile {
    email: String,
    client_id: String,
    secret: String,
    default_region: Option<String>,
}

const PROFILES_DIR_NAME: &str = ".config/mz";
const PROFILES_FILE_NAME: &str = "profiles.toml";
const CLOUD_PROVIDERS_URL: &str = "https://cloud.materialize.com/api/cloud-providers";
const API_TOKEN_AUTH_URL: &str =
    "https://admin.cloud.materialize.com/identity/resources/users/api-tokens/v1";
const USER_AUTH_URL: &str =
    "https://admin.cloud.materialize.com/frontegg/identity/resources/auth/v1/user";
const MACHINE_AUTH_URL: &str =
    "https://admin.cloud.materialize.com/identity/resources/auth/v1/api-token";
const WEB_LOGIN_URL: &str = "http://www.materialize.com/account/login?redirectUrl=/access/cli";

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    match args.command {
        Commands::Login(login_cmd) => match login_cmd.command {
            Some(LoginCommands::Interactive) => match get_local_profile() {
                None => login_with_console().await.unwrap(),
                Some(profile) => println!(
                    "There is a profile available: {:?} \nPlease, remove to assign a new one.",
                    profile
                ),
            },
            _ => match get_local_profile() {
                None => login_with_browser().await.unwrap(),
                Some(profile) => println!(
                    "There is a profile available: {:?} \nPlease, remove to assign a new one.",
                    profile
                ),
            },
        },

        Commands::Regions(regions_cmd) => {
            let client = Client::new();

            match regions_cmd.command {
                RegionsCommands::Enable {
                    cloud_provider_region,
                } => match validate_profile(client.clone()).await {
                    Some(frontegg_auth_machine) => {
                        match enable_region(client, cloud_provider_region, frontegg_auth_machine)
                            .await
                        {
                            Ok(region) => println!("Region enabled: {:?}", region),
                            Err(e) => panic!("Error enabling region: {:?}", e),
                        }
                    }
                    None => {}
                },
                RegionsCommands::Delete {
                    cloud_provider_region,
                } => {
                    if warning_delete_region(cloud_provider_region.clone()) {
                        match validate_profile(client.clone()).await {
                            Some(frontegg_auth_machine) => {
                                println!(
                                    "Deleting region. The operation may take a couple of minutes."
                                );

                                match delete_region(
                                    client.clone(),
                                    cloud_provider_region,
                                    frontegg_auth_machine,
                                )
                                .await
                                {
                                    Ok(_) => println!("Region deleted."),
                                    Err(e) => panic!("Error deleting region: {:?}", e),
                                }
                            }
                            None => {}
                        }
                    }
                }
                RegionsCommands::List => match validate_profile(client.clone()).await {
                    Some(frontegg_auth_machine) => {
                        match list_cloud_providers(client.clone(), frontegg_auth_machine.clone())
                            .await
                        {
                            Ok(cloud_providers) => {
                                let regions = list_regions(
                                    cloud_providers,
                                    client.clone(),
                                    frontegg_auth_machine,
                                )
                                .await;
                                println!("Regions: {:?}", regions);
                            }
                            Err(error) => panic!("Error retrieving cloud providers: {:?}", error),
                        }
                    }
                    None => {}
                },
            }
        }

        Commands::Shell => {
            // TODO: Use local profile to retrieve default region.
            match get_local_profile() {
                Some(profile) => {
                    let client = Client::new();
                    match authenticate_profile(client.clone(), profile.clone()).await {
                        Ok(frontegg_auth_machine) => {
                            shell(client, profile, frontegg_auth_machine).await
                        }
                        Err(error) => panic!("Error authenticating profile : {:?}", error),
                    }
                }
                None => println!("Profile not found. Please, login using `mz login`."),
            };
        }
    }
}
