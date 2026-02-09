use s3s::auth::SimpleAuth;

use crate::Config;

pub(crate) fn create_auth(config: &Config) -> SimpleAuth {
    SimpleAuth::from_single(
        config.client_access_key_id.as_str(),
        config.client_secret_access_key.as_str(),
    )
}
