use crate::ROOT_PATH;
use chrono::{DateTime, NaiveTime, TimeZone, Timelike, Utc};
use colored::Colorize;
use once_cell::sync::Lazy;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    mem::MaybeUninit,
    path::PathBuf,
    sync::Mutex,
};
use tracing::{
    field::{FieldSet, Visit},
    Level, Metadata, Subscriber,
};
use tracing_subscriber::{prelude::*, EnvFilter, Layer};

static LOG_PATH: Lazy<PathBuf> = Lazy::new(|| ROOT_PATH.join("log"));

fn truncate_to_hours(dt: DateTime<Utc>) -> DateTime<Utc> {
    let time = NaiveTime::from_hms_nano_opt(dt.time().hour(), 0, 0, 0).unwrap();
    Utc.from_utc_datetime(&dt.date_naive().and_time(time))
}

struct Callsite;
static CALLSITE: Callsite = Callsite;
impl tracing_core::callsite::Callsite for Callsite {
    fn set_interest(&self, _interest: tracing_core::subscriber::Interest) {}
    fn metadata(&self) -> &tracing_core::Metadata<'_> {
        unimplemented!()
    }
}

pub struct Logger {
    filter: EnvFilter,
    file: Mutex<(File, DateTime<Utc>)>,
}
impl Logger {
    fn open_file(time: DateTime<Utc>) -> File {
        OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(LOG_PATH.join(format!("novi-{}.log", time.format("%Y-%m-%d-%H"))))
            .expect("failed to open log file")
    }

    pub fn new() -> Self {
        if LOG_PATH.exists() {
            if !LOG_PATH.is_dir() {
                panic!("log path is not a dir")
            }
        } else {
            std::fs::create_dir(&*LOG_PATH).unwrap();
        }
        let now = truncate_to_hours(Utc::now());
        Self {
            filter: EnvFilter::from_default_env(),
            file: Mutex::new((Self::open_file(now), now)),
        }
    }

    fn write(&self, msg: String) {
        let (file, time) = &mut *self.file.lock().unwrap();
        let now = truncate_to_hours(Utc::now());
        if *time != now {
            *file = Self::open_file(now);
            *time = now;
        }
        file.write_all(msg.as_bytes()).expect("failed to write log");
    }

    pub fn log(
        &self,
        level: Level,
        target: String,
        msg: Option<String>,
        fields: Vec<(&'static str, String)>,
    ) {
        if level > Level::DEBUG {
            return;
        }

        let mut line = format!("{:.6?} ", chrono::Utc::now())
            .bright_black()
            .to_string()
            + &match level {
                Level::TRACE => "TRACE".bright_black(),
                Level::DEBUG => "DEBUG".magenta(),
                Level::INFO => " INFO".green(),
                Level::WARN => " WARN".yellow(),
                Level::ERROR => "ERROR".red(),
            }
            .to_string()
            + " ";

        line += &target.bright_black().to_string();
        if let Some(content) = msg {
            line += ": ";
            line += &content;
        }

        if !fields.is_empty() {
            line += &"{".bold().to_string();
            let len = fields.len();
            for (idx, (name, val)) in fields.into_iter().enumerate() {
                use std::fmt::Write;
                let _ = write!(line, "{}={val}", name.italic());
                if idx + 1 != len {
                    line.push(' ');
                }
            }
            line += &"}".bold().to_string();
        }

        let meta = Metadata::new(
            "log",
            &target,
            level,
            None,
            None,
            Some(&target),
            FieldSet::new(&[], tracing_core::identify_callsite!(&CALLSITE)),
            tracing_core::Kind::EVENT,
        );
        #[allow(invalid_value)]
        if self
            .filter
            .enabled::<()>(&meta, unsafe { MaybeUninit::uninit().assume_init() })
        {
            eprintln!("{line}");
        }

        line.push('\n');
        self.write(line);
    }
}
impl<S> Layer<S> for Logger
where
    S: Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Visitor(Option<String>, Vec<(&'static str, String)>);
        impl Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                let val = format!("{value:?}");
                if field.name() == "message" {
                    self.0 = Some(val);
                } else if !field.name().starts_with("log.") {
                    self.1.push((field.name(), val));
                }
            }
        }

        let meta = event.metadata();

        let mut v = Visitor(None, Vec::new());
        event.record(&mut v);

        self.log(*meta.level(), meta.target().to_string(), v.0, v.1);
    }
}

pub static LOGGER: Lazy<Logger> = Lazy::new(Logger::new);

pub fn register() {
    tracing_subscriber::registry().with(Logger::new()).init();
}
