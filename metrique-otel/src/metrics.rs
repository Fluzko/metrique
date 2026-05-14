// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::RwLock};

use metrique_writer_core::{
    Observation, Unit,
    unit::{NegativeScale, PositiveScale},
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram, MeterProvider, UpDownCounter},
};
use opentelemetry_sdk::metrics::SdkMeterProvider;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum InstrumentKind {
    Counter,
    UpDownCounter,
    Histogram,
    Gauge,
}

/// An OTel instrument resolved for a specific `(scope, name, kind, unit)`.
/// Each variant is `Arc`-backed inside the OTel SDK, so cloning is cheap and
/// recording is internally synchronized — no external locking required.
#[derive(Clone)]
pub(crate) enum CachedInstrument {
    Counter(Counter<u64>),
    UpDownCounter(UpDownCounter<i64>),
    Histogram(Histogram<f64>),
    Gauge(Gauge<f64>),
}

/// Constructs OTel instruments from a meter provider.
///
/// Holds no internal cache: callers (notably [`EntryPlan`]) own the resolved
/// [`CachedInstrument`] for the lifetime of the plan, so the hot path never
/// touches this builder.
///
/// [`EntryPlan`]: crate::translator::EntryPlan
#[derive(Clone)]
pub(crate) struct InstrumentBuilder {
    meter_provider: SdkMeterProvider,
}

impl InstrumentBuilder {
    pub(crate) fn new(meter_provider: SdkMeterProvider) -> Self {
        Self { meter_provider }
    }

    /// Build a fresh instrument. The OTel SDK already deduplicates by
    /// `(meter, name)` internally, so calling this twice with the same
    /// arguments returns equivalent handles.
    pub(crate) fn build(
        &self,
        scope: &'static str,
        name: &str,
        kind: InstrumentKind,
        unit: Unit,
    ) -> CachedInstrument {
        let meter = self.meter_provider.meter(scope);
        let unit_str = unit_to_otel(unit);
        match kind {
            InstrumentKind::Counter => CachedInstrument::Counter(
                meter
                    .u64_counter(name.to_owned())
                    .with_unit(unit_str)
                    .build(),
            ),
            InstrumentKind::UpDownCounter => CachedInstrument::UpDownCounter(
                meter
                    .i64_up_down_counter(name.to_owned())
                    .with_unit(unit_str)
                    .build(),
            ),
            InstrumentKind::Histogram => CachedInstrument::Histogram(
                meter
                    .f64_histogram(name.to_owned())
                    .with_unit(unit_str)
                    .build(),
            ),
            InstrumentKind::Gauge => CachedInstrument::Gauge(
                meter.f64_gauge(name.to_owned()).with_unit(unit_str).build(),
            ),
        }
    }
}

/// Lazy cache for instruments resolved at write time rather than plan-build
/// time. Hit only on the residual fallback path: hand-rolled `Entry` impls
/// that emit no descriptors, or descriptor-driven entries that emit a field
/// name the descriptor didn't list. The descriptor-driven hot path bypasses
/// this entirely.
///
/// Reads use the read lock and clone the `Arc`-backed handle out, so steady
/// state is contention-free; the write lock is taken only on first sight of
/// a `(scope, name, kind)` triple.
pub(crate) struct FallbackCache {
    builder: InstrumentBuilder,
    map: RwLock<HashMap<FallbackKey, CachedInstrument>>,
}

#[derive(Hash, PartialEq, Eq, Clone)]
struct FallbackKey {
    scope: &'static str,
    name: String,
    kind: InstrumentKind,
}

impl FallbackCache {
    pub(crate) fn new(builder: InstrumentBuilder) -> Self {
        Self {
            builder,
            map: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn get_or_build(
        &self,
        scope: &'static str,
        name: &str,
        kind: InstrumentKind,
        unit: Unit,
    ) -> CachedInstrument {
        let key = FallbackKey {
            scope,
            name: name.to_owned(),
            kind,
        };
        if let Some(inst) = self
            .map
            .read()
            .expect("fallback cache poisoned")
            .get(&key)
        {
            return inst.clone();
        }
        let inst = self.builder.build(scope, name, kind, unit);
        self.map
            .write()
            .expect("fallback cache poisoned")
            .entry(key)
            .or_insert(inst)
            .clone()
    }
}

/// Record `observations` against an already-resolved instrument.
///
/// The OTel SDK's `add`/`record` calls are internally synchronized, so no
/// external locking is required here — this is the lock-free hot path.
pub(crate) fn record_observations(
    instrument: &CachedInstrument,
    observations: impl IntoIterator<Item = Observation>,
    attributes: &[KeyValue],
) {
    match instrument {
        CachedInstrument::Counter(c) => {
            for obs in observations {
                let v = match obs {
                    Observation::Unsigned(v) => v,
                    // Counters are non-negative; clamp at 0 rather than
                    // emitting a panic for an out-of-spec observation.
                    Observation::Floating(v) => v.max(0.0) as u64,
                    Observation::Repeated { total, .. } => total.max(0.0) as u64,
                    _ => continue,
                };
                c.add(v, attributes);
            }
        }
        CachedInstrument::UpDownCounter(c) => {
            for obs in observations {
                let v = match obs {
                    Observation::Unsigned(v) => v as i64,
                    Observation::Floating(v) => v as i64,
                    Observation::Repeated { total, .. } => total as i64,
                    _ => continue,
                };
                c.add(v, attributes);
            }
        }
        CachedInstrument::Histogram(h) => {
            for obs in observations {
                let v = match obs {
                    Observation::Unsigned(v) => v as f64,
                    Observation::Floating(v) => v,
                    // Repeated has already collapsed the distribution to
                    // (total, occurrences); we can't recover individual
                    // samples. Record the mean once — bucketing is lossy
                    // but count and sum stay sensible. Users that need
                    // faithful distributions should keep raw `Floating`
                    // observations and avoid pre-summing.
                    Observation::Repeated { total, occurrences } if occurrences > 0 => {
                        total / occurrences as f64
                    }
                    _ => continue,
                };
                h.record(v, attributes);
            }
        }
        CachedInstrument::Gauge(g) => {
            for obs in observations {
                let v = match obs {
                    Observation::Unsigned(v) => v as f64,
                    Observation::Floating(v) => v,
                    Observation::Repeated { total, occurrences } if occurrences > 0 => {
                        total / occurrences as f64
                    }
                    _ => continue,
                };
                g.record(v, attributes);
            }
        }
    }
}

/// Map a `metrique` [`Unit`] to the UCUM-flavored string the OTEL semantic
/// conventions expect on the wire (e.g. `ms`, `By`, `%`, `1` for dimensionless).
pub(crate) fn unit_to_otel(unit: Unit) -> &'static str {
    match unit {
        Unit::None | Unit::Count => "1",
        Unit::Percent => "%",
        Unit::Second(NegativeScale::Micro) => "us",
        Unit::Second(NegativeScale::Milli) => "ms",
        Unit::Second(NegativeScale::One) => "s",
        Unit::Byte(scale) => match scale {
            PositiveScale::One => "By",
            PositiveScale::Kilo => "KBy",
            PositiveScale::Mega => "MBy",
            PositiveScale::Giga => "GBy",
            PositiveScale::Tera => "TBy",
            _ => "By",
        },
        Unit::BytePerSecond(scale) => match scale {
            PositiveScale::One => "By/s",
            PositiveScale::Kilo => "KBy/s",
            PositiveScale::Mega => "MBy/s",
            PositiveScale::Giga => "GBy/s",
            PositiveScale::Tera => "TBy/s",
            _ => "By/s",
        },
        Unit::Bit(scale) => match scale {
            PositiveScale::One => "bit",
            PositiveScale::Kilo => "Kbit",
            PositiveScale::Mega => "Mbit",
            PositiveScale::Giga => "Gbit",
            PositiveScale::Tera => "Tbit",
            _ => "bit",
        },
        Unit::BitPerSecond(scale) => match scale {
            PositiveScale::One => "bit/s",
            PositiveScale::Kilo => "Kbit/s",
            PositiveScale::Mega => "Mbit/s",
            PositiveScale::Giga => "Gbit/s",
            PositiveScale::Tera => "Tbit/s",
            _ => "bit/s",
        },
        Unit::Custom(s) => s,
        // `Unit` is `#[non_exhaustive]`; fall back to dimensionless for
        // unknown future variants rather than panicking.
        _ => "1",
    }
}
