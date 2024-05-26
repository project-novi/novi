use crate::{anyhow, tag, Result};

tonic::include_proto!("novi");

impl From<Uuid> for uuid::Uuid {
    fn from(pb: Uuid) -> Self {
        Self::from_u64_pair(pb.hi, pb.lo)
    }
}

pub fn uuid_to_pb(uuid: uuid::Uuid) -> Uuid {
    let (hi, lo) = uuid.as_u64_pair();
    Uuid { hi, lo }
}

pub fn tags_into_pb(ts: tag::Tags) -> Tags {
    let mut tags = Vec::new();
    let properties = ts
        .into_iter()
        .filter_map(|(k, v)| {
            if let Some(v) = v {
                Some((k, v))
            } else {
                tags.push(k);
                None
            }
        })
        .collect();

    Tags { tags, properties }
}

pub fn tags_from_pb(pb: Tags) -> tag::Tags {
    let mut tags = tag::Tags::new();
    for tag in pb.tags {
        tags.insert(tag, None);
    }
    for (k, v) in pb.properties {
        tags.insert(k, Some(v));
    }
    tags
}

pub fn required<T>(what: Option<T>) -> Result<T> {
    what.ok_or_else(|| anyhow!(@InvalidArgument "missing required field"))
}
