use chrono::{offset::LocalResult, DateTime, SubsecRound, TimeZone, Utc};
use std::{future::Future, pin::Pin};

use crate::{anyhow, bail, Result};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Returns the current time in UTC (truncate to microsecond).
pub fn now_utc() -> DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

pub fn utc_from_timestamp(micros: i64) -> Result<DateTime<Utc>> {
    if let LocalResult::Single(t) = Utc.timestamp_micros(micros) {
        Ok(t)
    } else {
        bail!(@InvalidArgument "invalid timestamp: {micros}");
    }
}

pub(crate) fn wrap_nom_from_str<R>(res: nom::IResult<&str, R>) -> Result<R> {
    match res {
        Ok((rem, res)) => {
            if !rem.is_empty() {
                bail!(@InvalidArgument "unexpected trailing characters: {rem:?}");
            }
            Ok(res)
        }
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            bail!(@InvalidArgument "parse error: {e}");
        }
        Err(nom::Err::Incomplete(_)) => {
            bail!(@InvalidArgument "incomplete input");
        }
    }
}
