use crate::{anyhow, ErrorKind, Result};

pub(crate) fn wrap_nom_from_str<R>(res: nom::IResult<&str, R>, kind: ErrorKind) -> Result<R> {
    let (rem, res) = res.map_err(|e| anyhow!("unknown error: {e}").with_kind(kind))?;
    if !rem.is_empty() {
        return Err(anyhow!("unexpected trailing characters: {rem:?}").with_kind(kind));
    }
    Ok(res)
}
