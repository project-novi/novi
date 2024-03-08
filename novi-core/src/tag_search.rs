use crate::{query::pg_pattern_escape, Result};
use jieba_rs::Jieba;
use once_cell::sync::Lazy;
use serde::Serialize;
use sqlx::{Pool, Postgres};
use tracing::{debug, error};

static JIEBA: Lazy<Jieba> = Lazy::new(Jieba::new);

fn to_ts_text(s: &str) -> String {
    let cut = JIEBA.cut(s, false);
    cut.join(" ")
}

async fn analyze_tags(db: &Pool<Postgres>) -> Result<()> {
    let mut tr = db.begin().await?;

    let tags = sqlx::query!(
        r#"select tag as "tag!", count(*) as "count!" from object
        cross join jsonb_object_keys(tags) as tag group by tag"#
    )
    .fetch_all(tr.as_mut())
    .await?;

    let all_tags: Vec<_> = tags.iter().map(|it| it.tag.clone()).collect();
    let missing_tags = sqlx::query_scalar!(
        r#"select tag as "tag!" from unnest($1::text[]) as tag where tag not in (select tag from tag)"#,
        &all_tags,
    )
    .fetch_all(tr.as_mut())
    .await?;

    if !missing_tags.is_empty() {
        for tag in missing_tags {
            sqlx::query!(
                "insert into tag(tag, count, search_vector) values ($1, 0, to_tsvector('simple', $2))",
                tag,
                to_ts_text(&tag),
            )
            .execute(tr.as_mut())
            .await?;
        }
    }

    sqlx::query!(
        "delete from tag where tag not in (select unnest($1::text[]))",
        &all_tags,
    )
    .execute(tr.as_mut())
    .await?;

    let counts: Vec<_> = tags.iter().map(|it| it.count as i32).collect();

    sqlx::query!(
        "update tag set count = bulk.count
        from (select unnest($1::text[]) as tag, unnest($2::int[]) as count) as bulk
        where tag.tag = bulk.tag
        ",
        &all_tags,
        &counts,
    )
    .execute(tr.as_mut())
    .await?;

    tr.commit().await?;

    Ok(())
}

pub async fn update_task(db: Pool<Postgres>, interval: u64) {
    loop {
        debug!("analyzing tags");

        if let Err(err) = analyze_tags(&db).await {
            error!(?err, "failed to analyze tags");
        }

        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
    }
}

#[derive(Serialize)]
pub struct TagSearch {
    tag: String,
    count: i64,
}
pub async fn search(db: &Pool<Postgres>, tag: &str) -> Result<Vec<TagSearch>> {
    let prefix = sqlx::query_as!(
        TagSearch,
        r#"select tag, count from tag where tag like $1 || '%' order by count desc limit 10"#,
        pg_pattern_escape(tag)
    )
    .fetch_all(db)
    .await?;

    let ts = sqlx::query_as!(
        TagSearch,
        "select tag, count
        from tag, plainto_tsquery('simple', $1) query
        where query @@ search_vector
        order by count::real * ts_rank_cd(search_vector, query, 32) desc limit 10",
        to_ts_text(tag)
    )
    .fetch_all(db)
    .await?;

    let mut tags = prefix;
    tags.extend(ts.into_iter());

    tags.sort_by(|x, y| x.tag.cmp(&y.tag));
    tags.dedup_by(|x, y| x.tag == y.tag);

    let mut sort_part = &mut tags[..];
    // Exact match
    if let Some(index) = sort_part.iter().position(|it| it.tag == tag) {
        sort_part.swap(0, index);
        sort_part = &mut sort_part[1..];
    }

    sort_part.sort_by_key(|it| -it.count);

    tags.truncate(10);

    Ok(tags)
}
