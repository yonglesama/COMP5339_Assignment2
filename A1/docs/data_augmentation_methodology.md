# Data Augmentation Methodology (Member C)

## Scope and external source

The augmentation targets the spatially joined DC subset produced by Member B:
419 charger rows representing 414 unique `station_id` locations. Its
`sa4_region` field comes from the ABS join completed during cleaning. Coverage is measured primarily by
unique location because the assignment asks for DC fast charging locations. A
second charger-row coverage measure is reported as a stricter check.

External data comes from OpenStreetMap through the read-only Overpass API. The
Python query retrieves every `amenity=charging_station` node, way or relation in a
loose NSW bounding box. The complete JSON response, a flattened CSV copy and a
metadata file containing the exact query, endpoint, timestamps, licence and
attribution are saved under `data/raw/augmentation/`. Ordinary reruns use this
cache; `--refresh` deliberately replaces it with a current snapshot.

API used: [Overpass API](https://wiki.openstreetmap.org/wiki/Overpass_API).
Supporting references: [OSM charging-station tagging](https://wiki.openstreetmap.org/wiki/Tag:amenity%3Dcharging_station)
and [OpenStreetMap copyright and ODbL](https://www.openstreetmap.org/copyright).
Attribution: © OpenStreetMap contributors.

## Matching strategy

Member B's DC rows are collapsed to one row per `station_id`. The station
coordinate is the median of its charger coordinates; all operators present at a
multi-operator station remain eligible for matching. Candidate OSM objects are
searched within 100 m using Haversine distance.

Missing TfNSW station names remain null and are not replaced with generated
labels. Station names are retained only for review and do not decide a match;
matching relies on operator compatibility and coordinates.

The preferred match is the nearest candidate whose OSM `operator`, `brand` or
`network` agrees with a TfNSW operator after canonicalising common aliases such as
Evie/Evie Networks, Ampol/AmpCharge and BP/bp pulse. If no operator-compatible
candidate exists, a coordinate-only match is accepted only within 25 m and only
when the OSM object has no operator, brand or network tag. A nearby object with an
explicitly conflicting operator is not matched. The output records the method,
distance and confidence for every accepted match. `matching_review.csv` retains
all accepted and rejected decisions, including possible shared OSM objects.

## Added attributes and outputs

For matched locations, the pipeline derives three useful fields: plug types from
positive `socket:<type>` tags, charging bays from a positive integer `capacity`,
and pricing information from `charge` or `fee`. Missing values are not imputed and
external values never overwrite TfNSW fields.

`charger_enrichment.parquet` is a self-contained 17-column final dataset: all 419
rows and 14 cleaned/spatial columns from `dc_candidates.parquet`, followed by the three
new fields. Matching evidence and provenance stay in the raw cache, metadata and
`matching_review.csv`. `augmentation_coverage.csv` records the 50% checks and
counts for the three added fields.

For the cached OpenStreetMap snapshot dated 2026-09-20, 244 of 414 locations were
matched. Of these, 214 locations contained at least one of the three final new
attributes, giving 51.69% location coverage and meeting the 50% target. They cover
218 of 419 charger rows (52.03%), so the stricter charger-row check is also met.
Four OSM objects are each shared by two nearby TfNSW station IDs. Even when each
shared OSM object is counted only once, 210 distinct augmented OSM objects remain
(50.72% of 414), so the target is not dependent on double-counting those cases.

## Limitations

OpenStreetMap is community-maintained, so tags may be incomplete, stale or use
different granularity from TfNSW. A single OSM station object can represent several
nearby physical chargers, while TfNSW may split the same site into multiple
`station_id` groups. Coordinate offsets can also arise from mapping an entrance,
parking area or charge-point centroid. The conservative operator and distance
rules reduce false matches, but all medium/low-confidence and shared-object cases
should remain visible for manual review. Exact pricing is often absent; when OSM
only supplies `fee=yes`, the pipeline reports that a fee is required without
inventing a rate.
