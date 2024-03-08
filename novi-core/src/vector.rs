use sqlx::{
    encode::IsNull,
    error::BoxDynError,
    postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef},
    Decode, Encode, Postgres, Type,
};

pub struct Vector(pub Vec<f32>);
impl Vector {
    fn from_sql(buf: &[u8]) -> Result<Vector, BoxDynError> {
        let dim = u16::from_be_bytes(buf[0..2].try_into()?) as usize;
        let unused = u16::from_be_bytes(buf[2..4].try_into()?);
        if unused != 0 {
            return Err("expected unused to be 0".into());
        }

        let mut vec = Vec::with_capacity(dim);
        for i in 0..dim {
            let s = 4 + 4 * i;
            vec.push(f32::from_be_bytes(buf[s..s + 4].try_into()?));
        }

        Ok(Vector(vec))
    }
}

impl Type<Postgres> for Vector {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("vector")
    }
}

impl Encode<'_, Postgres> for Vector {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> IsNull {
        let dim = self.0.len();
        buf.extend(&u16::try_from(dim).unwrap().to_be_bytes());
        buf.extend(&0_u16.to_be_bytes());

        for v in &self.0 {
            buf.extend(&v.to_be_bytes());
        }

        IsNull::No
    }
}

impl Decode<'_, Postgres> for Vector {
    fn decode(value: PgValueRef<'_>) -> Result<Self, BoxDynError> {
        let buf = <&[u8] as Decode<Postgres>>::decode(value)?;
        Vector::from_sql(buf)
    }
}

impl PgHasArrayType for Vector {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_vector")
    }
}
