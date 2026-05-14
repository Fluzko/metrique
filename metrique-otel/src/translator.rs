// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{any::TypeId, borrow::Cow, collections::HashMap};

use metrique_writer_core::{
    EntryConfig, MetricFlags, Observation, Unit, ValidationError,
    descriptor::DescriptorRef,
    entry::EntryWriter,
    value::{Distribution, Value, ValueWriter},
};
use opentelemetry::KeyValue;

use crate::{
    metrics::{CachedInstrument, FallbackCache, InstrumentBuilder, InstrumentKind, record_observations},
    tags,
};

/// One field's pre-resolved OTel instrument.
///
/// Built once at plan-build time and reused across every write of the same
/// entry shape — the hot path holds a borrow into the plan and clones this
/// instrument's `Arc`-backed handle without touching any external lock.
/// The kind is encoded in the [`CachedInstrument`] variant.
#[derive(Clone)]
pub(crate) struct FieldInstrument {
    pub(crate) instrument: CachedInstrument,
}

/// Pre-resolved plan for one entry shape: how to handle each named field at
/// write time without walking the descriptor again. `fields` maps the
/// runtime field name (the same string the macro/`Value` impl passes to
/// [`ValueWriter::metric`]) to a fully resolved OTel instrument.
///
/// Field names not present in `fields` fall back to runtime classification:
/// strings become attributes; metrics with the [`Distribution`] flag map to
/// a histogram resolved via [`FallbackCache`]; anything else is dropped and
/// counted as unclassified.
///
/// `scope` is `&'static str` because the OTel `MeterProvider::meter()` API
/// requires it; we intern via `Box::leak` once per unique entry shape at
/// plan-build time, which leaks O(#entry_types) bytes for the process —
/// acceptable in exchange for keeping the plan cheap to share.
#[derive(Clone)]
pub(crate) struct EntryPlan {
    pub(crate) scope: &'static str,
    pub(crate) fields: HashMap<String, FieldInstrument>,
    /// Names of fields that arrived from a descriptor but carried no
    /// instrument-kind tag. Captured at plan-build time so we can warn once
    /// per descriptor rather than once per write.
    pub(crate) unclassified: Vec<String>,
}

impl EntryPlan {
    /// Plan for a hand-rolled `Entry` that emits no descriptors. Strings
    /// become attributes; only `Distribution`-flagged metrics are recorded,
    /// resolved through the sink's fallback cache.
    pub(crate) fn fallback() -> Self {
        Self {
            scope: "metrique-otel",
            fields: HashMap::new(),
            unclassified: Vec::new(),
        }
    }

    /// Build a plan from one or more descriptor segments emitted by a single
    /// entry. For every tagged field, the matching OTel instrument is
    /// constructed up front from the descriptor's declared unit, so the
    /// hot path never has to consult a cache. The meter scope name is taken
    /// from the first segment's canonical entry name.
    pub(crate) fn from_descriptors(
        segments: &[DescriptorRef<'_>],
        builder: &InstrumentBuilder,
    ) -> Self {
        let scope: &'static str = match segments.first() {
            Some(d) => Box::leak(format!("metrique/{}", d.name()).into_boxed_str()),
            None => "metrique-otel",
        };

        let mut fields = HashMap::new();
        let mut unclassified = Vec::new();
        for desc in segments {
            for field in desc.fields() {
                let mut full = String::new();
                for part in field.name_parts() {
                    full.push_str(part);
                }
                match resolve_kind(&field) {
                    Some(kind) => {
                        // Descriptor-declared unit wins; fields without a
                        // declared unit fall back to dimensionless. The OTel
                        // instrument's unit is fixed at construction time.
                        let unit = field.unit().unwrap_or(Unit::None);
                        let instrument = builder.build(scope, &full, kind, unit);
                        fields.insert(full, FieldInstrument { instrument });
                    }
                    None => {
                        unclassified.push(full);
                    }
                }
            }
        }

        Self {
            scope,
            fields,
            unclassified,
        }
    }
}

fn resolve_kind(field: &metrique_writer_core::descriptor::FieldView<'_>) -> Option<InstrumentKind> {
    use metrique_writer_core::descriptor::FieldTagState;

    let counter = TypeId::of::<tags::Counter>();
    let up_down = TypeId::of::<tags::UpDownCounter>();
    let histogram = TypeId::of::<tags::Histogram>();
    let gauge = TypeId::of::<tags::Gauge>();

    for tag in field.tags() {
        if tag.state() != FieldTagState::Present {
            continue;
        }
        let id = tag.tag_id();
        if id == counter {
            return Some(InstrumentKind::Counter);
        } else if id == up_down {
            return Some(InstrumentKind::UpDownCounter);
        } else if id == histogram {
            return Some(InstrumentKind::Histogram);
        } else if id == gauge {
            return Some(InstrumentKind::Gauge);
        }
    }
    None
}

/// A pending metric observation captured during `Entry::write`, replayed
/// once we have the full entry-level attribute set. Buffering is what lets
/// a string field declared *after* a metric field still ride along as an
/// attribute on that metric.
///
/// The resolved instrument is carried directly (cheap `Arc`-clone), so
/// `finish()` never needs to look anything up.
struct PendingMetric {
    instrument: CachedInstrument,
    observations: Vec<Observation>,
    per_metric_dimensions: Vec<KeyValue>,
}

pub(crate) struct OtelEntryWriter<'sink, 'plan> {
    pub(crate) plan: &'plan EntryPlan,
    /// Used only when an emitted field name isn't in `plan.fields` — i.e. the
    /// hand-rolled-entry path, or a descriptor-driven entry that emits an
    /// unexpected field with the `Distribution` flag.
    pub(crate) fallback_cache: &'sink FallbackCache,
    /// String fields collected during the walk; applied as attributes to
    /// every metric in this entry at `finish()` time.
    entry_attributes: Vec<KeyValue>,
    pending: Vec<PendingMetric>,
}

impl<'sink, 'plan> OtelEntryWriter<'sink, 'plan> {
    pub(crate) fn new(plan: &'plan EntryPlan, fallback_cache: &'sink FallbackCache) -> Self {
        Self {
            plan,
            fallback_cache,
            entry_attributes: Vec::new(),
            pending: Vec::new(),
        }
    }

    pub(crate) fn finish(self) {
        for m in self.pending {
            // Per-metric dimensions take precedence by appearing first; the
            // entry-level attributes follow. The OTEL SDK does not de-dup
            // attribute keys, so any collision is left visible — that's a
            // user-data problem, not something to paper over here.
            let mut attributes = m.per_metric_dimensions;
            attributes.extend(self.entry_attributes.iter().cloned());
            record_observations(&m.instrument, m.observations, &attributes);
        }
    }
}

impl<'a, 'sink, 'plan> EntryWriter<'a> for OtelEntryWriter<'sink, 'plan> {
    fn timestamp(&mut self, _timestamp: std::time::SystemTime) {
        // OTEL meter readers stamp measurements with their own clock; the
        // entry timestamp is informational only.
    }

    fn value(&mut self, name: impl Into<Cow<'a, str>>, value: &(impl Value + ?Sized)) {
        let name = name.into();
        let writer = OtelValueWriter { parent: self, name };
        value.write(writer);
    }

    fn config(&mut self, _config: &'a dyn EntryConfig) {
        // OTEL-specific entry config is not consumed yet.
    }
}

pub(crate) struct OtelValueWriter<'a, 'sink, 'plan> {
    pub(crate) parent: &'a mut OtelEntryWriter<'sink, 'plan>,
    pub(crate) name: Cow<'a, str>,
}

impl<'a, 'sink, 'plan> ValueWriter for OtelValueWriter<'a, 'sink, 'plan> {
    fn string(self, value: &str) {
        // String fields become entry-wide attributes attached to every
        // metric this entry produces.
        self.parent
            .entry_attributes
            .push(KeyValue::new(self.name.into_owned(), value.to_owned()));
    }

    fn metric<'b>(
        self,
        distribution: impl IntoIterator<Item = Observation>,
        unit: Unit,
        dimensions: impl IntoIterator<Item = (&'b str, &'b str)>,
        flags: MetricFlags<'_>,
    ) {
        // Resolve the instrument:
        //   1. Descriptor-tagged field → pre-built instrument from the plan
        //      (cheap Arc clone, no lock).
        //   2. `Distribution` flag on an un-tagged field → histogram looked
        //      up in the fallback cache (RwLock read-fast-path).
        //   3. Anything else is unclassified and dropped; the one-time warn
        //      is emitted at plan-build time for descriptors that contain
        //      such fields.
        let instrument = if let Some(fi) = self.parent.plan.fields.get(self.name.as_ref()) {
            fi.instrument.clone()
        } else if flags.downcast::<Distribution>().is_some() {
            self.parent.fallback_cache.get_or_build(
                self.parent.plan.scope,
                self.name.as_ref(),
                InstrumentKind::Histogram,
                unit,
            )
        } else {
            return;
        };

        let per_metric_dimensions: Vec<KeyValue> = dimensions
            .into_iter()
            .map(|(k, v)| KeyValue::new(k.to_owned(), v.to_owned()))
            .collect();
        self.parent.pending.push(PendingMetric {
            instrument,
            observations: distribution.into_iter().collect(),
            per_metric_dimensions,
        });
    }

    fn error(self, _error: ValidationError) {
        // Validation errors are silently dropped for now.
    }
}
