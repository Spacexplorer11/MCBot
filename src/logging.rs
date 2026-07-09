use chrono::Utc;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};
use reqwest::Client;
use std::env;
use tracing::Level;
use tracing_subscriber::{
    layer::SubscriberExt,
    util::SubscriberInitExt,
    {EnvFilter, Layer},
};

#[derive(serde::Serialize)]
struct LogPayload {
    timestamp: String,
    group: String,
    severity: String,
    message: String,
    hostname: String,
}

struct LogVisitor {
    message: String,
}

impl tracing::field::Visit for LogVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

struct HttpLogger {
    client: Client,
    url: String,
}

impl<S: tracing::Subscriber> Layer<S> for HttpLogger {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = LogVisitor {
            message: String::new(),
        };
        event.record(&mut visitor);

        let severity = match *event.metadata().level() {
            Level::ERROR => "error",
            Level::WARN => "warn",
            Level::INFO => "info",
            Level::DEBUG => "debug",
            Level::TRACE => "trace",
        };

        let payload = LogPayload {
            timestamp: Utc::now().to_rfc3339(),
            group: "MCBot".to_string(),
            severity: severity.to_string(),
            message: visitor.message,
            hostname: hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string()),
        };

        let client = self.client.clone();
        let url = self.url.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = client.post(url).json(&payload).send().await;
            });
        }
    }
}

pub fn initialise_logging() {
    let appsignal_api_key =
        env::var("APPSIGNAL_PUSH_API_KEY").expect("APPSIGNAL_PUSH_API_KEY must be set in .env");

    let appsignal_url = "https://m1lxp90w.eu-central.appsignal-collector.net/v1/traces";

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary) // NEVER TOUCH THIS AGAIN. ONLY BINARY WORKS - WASTED HOURS ON THIS
        .with_endpoint(appsignal_url)
        .build()
        .expect("Failed to create OpenTelemetry span exporter");

    let appsignal_environment = env::var("APPSIGNAL_ENVIRONMENT").unwrap_or_else(|_| {
        tracing::warn!("No appsignal environment env variable found, defaulting to development");
        "development".to_string()
    });

    let appsignal_revision =
        env::var("APPSIGNAL_REVISION").unwrap_or_else(|_| "unknown".to_string());

    let resource = Resource::builder()
        .with_attributes(vec![
            KeyValue::new("service.name", "MCBot"),
            KeyValue::new("appsignal.config.name", "MCBot"),
            KeyValue::new("appsignal.config.language_integration", "rust"),
            KeyValue::new("appsignal.config.environment", appsignal_environment),
            KeyValue::new("appsignal.config.push_api_key", appsignal_api_key),
            KeyValue::new("appsignal.config.revision", appsignal_revision),
        ])
        .build();

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource.clone())
        .build();

    global::set_tracer_provider(tracer_provider.clone());

    let tracer = global::tracer("mc-bot-tracer");
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,mcbot=debug,opentelemetry_sdk=off,opentelemetry-otlp=off")
    });

    let logs_url = env::var("APPSIGNAL_LOGS_URL").expect("No appsignal logs url found");

    let client = Client::new();

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry_layer)
        .with(filter)
        .with(HttpLogger {
            client: client.clone(),
            url: logs_url,
        })
        .init();
}
