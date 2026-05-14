// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(docsrs, feature(doc_cfg))]

mod metrics;
pub mod tags;
mod translator;

pub use metrics::InstrumentKind;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use metrique_writer_core::{
    Entry,
    descriptor::DescriptorId,
    sink::{EntrySink, FlushWait},
};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use smallvec::SmallVec;

use crate::{
    metrics::{FallbackCache, InstrumentBuilder},
    translator::{EntryPlan, OtelEntryWriter},
};

#[derive(Clone)]
pub struct OtelSink {
    inner: Arc<OtelSinkInner>,
}

struct OtelSinkInner {
    meter_provider: SdkMeterProvider,
    /// Builds OTel instruments at plan-build time. Holds no map and no lock;
    /// the resolved instruments live on the per-shape [`EntryPlan`].
    builder: InstrumentBuilder,
    /// Lazy cache for instruments resolved at write time rather than
    /// plan-build time — only hit by the residual fallback paths (entries
    /// with no descriptors, or `Distribution`-flagged metrics whose field
    /// name isn't in the plan).
    fallback_cache: FallbackCache,
    /// Cache of resolved entry plans, keyed by the sequence of
    /// [`DescriptorId`]s emitted by the entry. `SmallVec` keeps the
    /// 1-segment case (the common one) heap-free at lookup time, and the
    /// `HashMap`'s own hasher does the only SipHash pass over the sequence.
    plans: RwLock<HashMap<PlanKey, Arc<EntryPlan>>>,
    /// Descriptor cache-keys for which we have already emitted the
    /// "unclassified field" warning. Lets the warning fire once per shape
    /// even though `append` runs on every entry.
    warned: RwLock<HashSet<PlanKey>>,
    fallback_plan: Arc<EntryPlan>,
}

/// Identity of an entry shape for plan-cache lookup. Built by walking
/// [`Entry::descriptors`] and collecting each segment's [`DescriptorId`].
/// Inline storage for 1-segment entries (the common case); the heap kicks
/// in only for composed entries (e.g. aggregation results).
type PlanKey = SmallVec<[DescriptorId; 1]>;

impl OtelSink {
    pub fn builder() -> OtelSinkBuilder {
        OtelSinkBuilder::default()
    }

    /// Drive `force_flush` on the meter provider and resolve once it's done.
    /// Errors from `force_flush` are logged at `warn` level but not surfaced —
    /// the [`EntrySink`] trait has no way to report them.
    ///
    /// Internally this uses `tokio::task::spawn_blocking` so it must be
    /// awaited on a tokio runtime. Callers of [`with_otlp_default`] already
    /// require tokio (the OTLP/gRPC exporters use it transitively), so this
    /// is not an additional constraint in practice.
    ///
    /// [`with_otlp_default`]: Self::with_otlp_default
    pub fn flush_async(&self) -> FlushWait {
        let meter = self.inner.meter_provider.clone();
        FlushWait::from_future(async move {
            let _ = tokio::task::spawn_blocking(move || {
                if let Err(e) = meter.force_flush() {
                    tracing::warn!(error = %e, "metrique-otel: meter provider force_flush failed");
                }
            })
            .await;
        })
    }

    /// Build a sink whose meter provider is wired to an OTLP/gRPC exporter
    /// using the standard `OTEL_*` environment variables.
    pub fn with_otlp_default() -> Result<Self, OtelSinkError> {
        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .build()
            .map_err(|e| OtelSinkError::Otlp(Box::new(e)))?;
        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(metric_exporter).build();
        let meter_provider = SdkMeterProvider::builder().with_reader(reader).build();

        Ok(OtelSinkBuilder::default()
            .with_meter_provider(meter_provider)
            .build())
    }

    /// Resolve the cached plan for an entry, building one if this is the
    /// first time we've seen this shape. Returns the fallback plan if the
    /// entry emits no descriptors.
    ///
    /// Steady state (cache hit) walks `entry.descriptors()` once to build a
    /// `PlanKey` — stack-allocated for the common 1-segment case — then does
    /// a read-locked map lookup. On miss it walks `descriptors()` a second
    /// time to collect the full `DescriptorRef`s for plan construction.
    fn plan_for<E: Entry>(&self, entry: &E) -> Arc<EntryPlan> {
        let mut key: PlanKey = SmallVec::new();
        for d in entry.descriptors() {
            key.push(d.id());
        }
        if key.is_empty() {
            return Arc::clone(&self.inner.fallback_plan);
        }

        if let Some(plan) = self
            .inner
            .plans
            .read()
            .expect("plan cache read poisoned")
            .get(&key)
            .cloned()
        {
            return plan;
        }

        // Miss: rebuild segments and construct the plan. `Entry::descriptors`
        // takes `&self`, so re-invoking yields a fresh iterator.
        let segments: Vec<_> = entry.descriptors().collect();
        let plan = Arc::new(EntryPlan::from_descriptors(&segments, &self.inner.builder));

        if !plan.unclassified.is_empty() {
            let mut warned = self.inner.warned.write().expect("warned cache poisoned");
            if warned.insert(key.clone()) {
                let scope = &plan.scope;
                let fields: Vec<&str> = plan.unclassified.iter().map(String::as_str).collect();
                tracing::warn!(
                    target: "metrique_otel",
                    scope = %scope,
                    fields = ?fields,
                    "metrique-otel: fields have no instrument-kind tag; only Distribution-flagged observations will be recorded. \
                     Apply `#[metrics(field_tag(Counter|UpDownCounter|Histogram|Gauge))]`."
                );
            }
        }

        // Use the canonical Arc — if another thread raced us, `or_insert_with`
        // keeps the first-write winner and we return that, not our local copy.
        self.inner
            .plans
            .write()
            .expect("plan cache write poisoned")
            .entry(key)
            .or_insert_with(|| Arc::clone(&plan))
            .clone()
    }

    #[cfg(test)]
    fn plan_cache_len(&self) -> usize {
        self.inner
            .plans
            .read()
            .expect("plan cache read poisoned")
            .len()
    }
}

#[non_exhaustive]
#[derive(Debug)]
pub enum OtelSinkError {
    Otlp(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for OtelSinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Otlp(e) => write!(f, "failed to build OTLP exporter: {e}"),
        }
    }
}

impl std::error::Error for OtelSinkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Otlp(e) => Some(&**e),
        }
    }
}

/// Builder for [`OtelSink`].
///
/// `with_resource` only applies when the meter provider is *not* supplied
/// explicitly via `with_meter_provider` — a user-supplied provider already
/// carries its own resource.
#[derive(Default)]
pub struct OtelSinkBuilder {
    meter_provider: Option<SdkMeterProvider>,
    resource: Option<Resource>,
}

impl OtelSinkBuilder {
    pub fn with_meter_provider(mut self, provider: SdkMeterProvider) -> Self {
        self.meter_provider = Some(provider);
        self
    }

    pub fn with_resource(mut self, resource: Resource) -> Self {
        self.resource = Some(resource);
        self
    }

    pub fn build(self) -> OtelSink {
        let meter_provider = self.meter_provider.unwrap_or_else(|| {
            let mut b = SdkMeterProvider::builder();
            if let Some(r) = self.resource {
                b = b.with_resource(r);
            }
            b.build()
        });
        let builder = InstrumentBuilder::new(meter_provider.clone());
        let fallback_cache = FallbackCache::new(builder.clone());
        OtelSink {
            inner: Arc::new(OtelSinkInner {
                meter_provider,
                builder,
                fallback_cache,
                plans: RwLock::new(HashMap::new()),
                warned: RwLock::new(HashSet::new()),
                fallback_plan: Arc::new(EntryPlan::fallback()),
            }),
        }
    }
}

impl<E: Entry + Send + 'static> EntrySink<E> for OtelSink {
    fn append(&self, entry: E) {
        let plan = self.plan_for(&entry);
        let mut writer = OtelEntryWriter::new(&plan, &self.inner.fallback_cache);
        entry.write(&mut writer);
        writer.finish();
    }

    fn flush_async(&self) -> FlushWait {
        OtelSink::flush_async(self)
    }
}

// Note on lifecycle: `OtelSink` deliberately does not implement `Drop` to
// call `shutdown` on the meter provider. Users can pass an externally-owned
// provider via `OtelSinkBuilder::with_meter_provider`, and shutting it down
// when the sink drops would be surprising. If explicit shutdown is needed,
// expose an `OtelSink::shutdown(&self)` later.

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use metrique::unit::Millisecond;
    use metrique::unit_of_work::metrics;
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader};

    use super::*;
    use crate::tags::{Counter, Gauge, Histogram, UpDownCounter};

    fn in_memory_sink() -> (OtelSink, InMemoryMetricExporter, SdkMeterProvider) {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let meter_provider = SdkMeterProvider::builder().with_reader(reader).build();
        let sink = OtelSink::builder()
            .with_meter_provider(meter_provider.clone())
            .build();
        (sink, exporter, meter_provider)
    }

    #[test]
    fn builder_default_constructs_a_sink() {
        let sink = OtelSink::builder().build();
        let _cloned = sink.clone();
    }

    #[metrics(rename_all = "PascalCase")]
    struct CounterMetrics {
        #[metrics(field_tag(Counter))]
        requests: u64,
    }

    #[test]
    fn counter_observation_lands_in_exporter() {
        let (sink, exporter, meter_provider) = in_memory_sink();

        sink.append(metrique::RootEntry::new(metrique::CloseValue::close(
            CounterMetrics { requests: 7 },
        )));
        meter_provider.force_flush().expect("force_flush");

        let exported = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics");
        let names: Vec<&str> = exported
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .map(|m| m.name())
            .collect();
        assert!(
            names.iter().any(|n| *n == "Requests"),
            "expected 'Requests' metric, found {names:?}"
        );
    }

    #[metrics(rename_all = "PascalCase")]
    struct AllKindsMetrics {
        #[metrics(field_tag(Counter))]
        counter: u64,
        #[metrics(field_tag(UpDownCounter))]
        up_down: f64,
        #[metrics(field_tag(Histogram))]
        hist: f64,
        #[metrics(field_tag(Gauge))]
        gauge: f64,
    }

    #[test]
    fn all_instrument_kinds_land_in_exporter() {
        use opentelemetry_sdk::metrics::data::AggregatedMetrics;

        let (sink, exporter, meter_provider) = in_memory_sink();

        sink.append(metrique::RootEntry::new(metrique::CloseValue::close(
            AllKindsMetrics {
                counter: 3,
                up_down: -2.0,
                hist: 12.5,
                gauge: 0.42,
            },
        )));
        meter_provider.force_flush().expect("force_flush");

        let exported = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics");
        let mut by_name: Vec<(&str, &str)> = Vec::new();
        for rm in &exported {
            for sm in rm.scope_metrics() {
                for m in sm.metrics() {
                    let variant = match m.data() {
                        AggregatedMetrics::U64(_) => "u64",
                        AggregatedMetrics::I64(_) => "i64",
                        AggregatedMetrics::F64(_) => "f64",
                    };
                    by_name.push((m.name(), variant));
                }
            }
        }

        for expected in [
            ("Counter", "u64"),
            ("UpDown", "i64"),
            ("Hist", "f64"),
            ("Gauge", "f64"),
        ] {
            assert!(
                by_name.contains(&expected),
                "missing {expected:?} in exported metrics: {by_name:?}"
            );
        }
    }

    #[metrics(rename_all = "PascalCase")]
    struct MixedEntry {
        operation: String,
        #[metrics(field_tag(Counter))]
        requests: u64,
        #[metrics(unit = Millisecond, field_tag(Histogram))]
        latency: Duration,
    }

    /// String fields ride along as attributes on every metric in the same
    /// entry, even when declared after the metric — the writer buffers
    /// metric records until `finish()`.
    #[test]
    fn string_field_attaches_as_attribute_to_metrics() {
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};

        let (sink, exporter, meter_provider) = in_memory_sink();

        sink.append(metrique::RootEntry::new(metrique::CloseValue::close(
            MixedEntry {
                operation: "GET".to_owned(),
                requests: 1,
                latency: Duration::from_millis(42),
            },
        )));
        meter_provider.force_flush().expect("force_flush");

        let exported = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics");
        let mut found_attrs: Vec<(String, String)> = Vec::new();
        let mut found_unit: Option<String> = None;
        for rm in &exported {
            for sm in rm.scope_metrics() {
                for m in sm.metrics() {
                    if m.name() == "Latency" {
                        found_unit = Some(m.unit().to_owned());
                    }
                    if m.name() != "Requests" {
                        continue;
                    }
                    if let AggregatedMetrics::U64(MetricData::Sum(sum)) = m.data() {
                        for dp in sum.data_points() {
                            for kv in dp.attributes() {
                                found_attrs
                                    .push((kv.key.to_string(), kv.value.as_str().into_owned()));
                            }
                        }
                    }
                }
            }
        }

        assert_eq!(
            found_attrs,
            vec![("Operation".to_string(), "GET".to_string())],
            "expected Operation=GET to ride along as a metric attribute"
        );
        assert_eq!(
            found_unit.as_deref(),
            Some("ms"),
            "expected Latency to carry ms unit derived from #[metrics(unit = Millisecond)]"
        );
    }

    /// The plan cache is keyed by `DescriptorId` — N writes of the same
    /// shape build the plan exactly once.
    #[test]
    fn plan_cache_built_once_per_shape() {
        let (sink, _exporter, _mp) = in_memory_sink();

        for _ in 0..5 {
            sink.append(metrique::RootEntry::new(metrique::CloseValue::close(
                CounterMetrics { requests: 1 },
            )));
        }
        assert_eq!(
            sink.plan_cache_len(),
            1,
            "five writes of the same entry shape should populate one plan entry"
        );
    }

    /// Meter scope name is derived from `desc.name()`. Verified by routing
    /// two different entry types through the same sink and observing two
    /// distinct InstrumentationScope names on the exporter.
    #[test]
    fn meter_scope_uses_descriptor_name() {
        #[metrics(rename_all = "PascalCase")]
        struct ScopeA {
            #[metrics(field_tag(Counter))]
            n: u64,
        }
        #[metrics(rename_all = "PascalCase")]
        struct ScopeB {
            #[metrics(field_tag(Counter))]
            n: u64,
        }

        let (sink, exporter, meter_provider) = in_memory_sink();
        sink.append(metrique::RootEntry::new(metrique::CloseValue::close(
            ScopeA { n: 1 },
        )));
        sink.append(metrique::RootEntry::new(metrique::CloseValue::close(
            ScopeB { n: 1 },
        )));
        meter_provider.force_flush().expect("force_flush");

        let exported = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics");
        let mut scopes: Vec<String> = exported
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .map(|sm| sm.scope().name().to_string())
            .collect();
        scopes.sort();
        scopes.dedup();
        assert!(
            scopes.contains(&"metrique/ScopeA".to_string()),
            "expected scope 'metrique/ScopeA' in {scopes:?}"
        );
        assert!(
            scopes.contains(&"metrique/ScopeB".to_string()),
            "expected scope 'metrique/ScopeB' in {scopes:?}"
        );
    }

    /// `flush_async` drives `force_flush` on the meter provider end-to-end.
    #[tokio::test(flavor = "multi_thread")]
    async fn flush_async_drains_meter_provider() {
        let (sink, exporter, _mp) = in_memory_sink();

        sink.append(metrique::RootEntry::new(metrique::CloseValue::close(
            CounterMetrics { requests: 3 },
        )));
        sink.flush_async().await;

        let exported = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics");
        let names: Vec<&str> = exported
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .map(|m| m.name())
            .collect();
        assert!(
            names.contains(&"Requests"),
            "expected Requests counter via flush_async, got {names:?}"
        );
    }
}
