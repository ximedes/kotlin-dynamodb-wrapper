# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.5] - 2026-07-28

**Users of 0.7 should upgrade.** 0.7 shipped a defect in the expression DSL that can silently write
the wrong attribute. See *Fixed* below for what was affected and how to tell whether you were hit.

### Fixed

- Multiple expressions on a single request no longer share attribute name and value placeholders.

  Each expression slot was rendered by its own renderer, and every renderer restarts its placeholder
  counters at `#n1` / `:v1`. Because a request has only one `ExpressionAttributeNames` map and one
  `ExpressionAttributeValues` map, the second expression's placeholders overwrote the first's.

  Affected combinations, all of them introduced in 0.7:

  - `updateItem`/transactional `update` with both `update { }` and `condition { }` — **silent data
    corruption.** The request stayed well-formed, so DynamoDB accepted it and wrote the attribute
    named by the condition instead of the one named by the update. For example
    `update { set("name", "NewName") }` with `condition { "status" eq "ACTIVE" }` executed as
    `SET status = "ACTIVE"`, and the write to `name` was lost. This includes the common optimistic
    locking pattern of an update guarded by a version or status check.
  - `query` with both `keyCondition { }` and `filter { }` — the key condition was rewritten to
    reference the filter's attributes, which normally fails fast with a `ValidationException`
    because those attributes are not key attributes.

  Requests using only one expression slot were never affected, nor were manually written expression
  strings. Code that never combined two expression slots on one request is unaffected.

- Exceptions thrown while awaiting an asynchronous request are logged.

### Changed

- Repeated `attributeNames(...)` calls on `GetItemRequestBuilder` and `KeysAndAttributesBuilder`
  accumulate rather than discarding earlier entries, which is how the other builders already behaved.
- A manually supplied expression placeholder that collides with a DSL-generated one (`#n1`, `#n2`,
  … and `:v1`, `:v2`, …) now throws `IllegalArgumentException` instead of silently overwriting it.
- An attribute referenced by two expressions on the same request now shares a single name
  placeholder.
- The `kotlinx-coroutines-jdk8` dependency is replaced by `kotlinx-coroutines-core`. The `jdk8`
  module was folded into `core` upstream in coroutines 1.7.0 and is now a deprecated shim. No API
  change for consumers.
- Dependency updates: AWS SDK for Java DynamoDB `2.49.4`, kotlinx-coroutines `1.11.0`,
  JUnit `6.1.2` (test scope only). Maven plugin versions updated and the compiler and jar plugin
  versions are now pinned.

### Added

- `takeAll` now reads number sets and binary sets: `Int`, `Long`, `Double` and `ByteArray` are
  supported alongside `String`. Previously it threw for anything but `String`.

### Removed

- Unused `UpdateExpressionSet` class.

## [0.7] - 2026-01-06

Superseded by 0.7.5; see the advisory above before using this version.

### Added

- Type-safe condition expression DSL, available as `condition`, `filter` and `keyCondition` on the
  request builders. Supports the comparison operators (`eq`, `ne`, `lt`, `le`, `gt`, `ge`),
  `between`, `isIn`, `attributeExists`, `attributeNotExists`, `attributeType`, `beginsWith`,
  `contains`, `size`, the logical operators (`and`, `or`, `not`), and nested attribute paths with
  dot notation and array indices.
- Type-safe update expression DSL, available as `update`. Supports `SET`, `REMOVE`, `ADD` and
  `DELETE`, plus `increment`, `decrement`, `ifNotExists`, `listAppend` and `listPrepend`.
- `Scan` operation, synchronous and asynchronous.
- `returnValues` option on `DeleteItemRequestBuilder`.

### Fixed

- Corrected the SCM and project URLs in the published POM.

## [0.6.1]

See the commit history for changes up to and including `0.6.1`.
