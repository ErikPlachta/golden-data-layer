# PE / Real Assets: Industry-Standard Data Hierarchy Research

**Date:** 2026-02-08
**Purpose:** Document industry findings on how private equity and real asset investment firms organize data, to validate and inform golden data layer architecture decisions.

---

## Industry-Standard Hierarchy

```
Management Company (GP)
  └→ Fund (vintage year, strategy, committed capital)
       └→ Portfolio (collection of investments within a fund)
            └→ Investment / Deal (capital deployed into a specific target)
                 └→ Portfolio Company / Entity (legal entity invested in)
                      └→ Asset (physical or financial asset owned by entity)
                           └→ Security (tradeable instrument representing claim on asset)
```

---

## Key Findings

### 1. Fund is the Primary Organizational Unit

In PE/real assets, a fund has a vintage year, defined strategy, committed capital, and lifecycle (fundraising → investing → harvesting → exit). Not "portfolio group" — the fund concept is the canonical organizing principle across the industry.

### 2. LP/GP Relationship is Structural

Limited Partners commit capital to Funds managed by General Partners. The GP/management company layer is the top of the hierarchy, not an afterthought.

### 3. Ownership is Layered and Fractional

A Fund may own 60% of an Entity through an SPV. That Entity may own 3 Assets. Each Asset may have multiple Securities across the capital structure (equity, debt, mezzanine, derivative). Ownership % exists at **multiple levels**, not just entity→asset.

### 4. Securities are the Instrument Layer

A security represents a specific financial claim — equity stake, senior secured debt, mezzanine, derivative. One asset can have multiple securities across the capital structure.

### 5. Public Market Identifiers Live at Security Level

BankLoanID / CUSIP / ISIN are how internal positions connect to external market data (WSO, Bloomberg, etc.).

### 6. Security Master is its Own Discipline

Industry standard is a dedicated security master system — the authoritative record of what securities exist, their types, identifiers, and current status. Gets assembled from multiple inputs (internal deal records + external market data).

### 7. Entity-Asset Coupling

Asset sourcing is typically part of entity management in most firms. The entity management platform tracks "Entity X owns Assets A, B, C with ownership splits Y%." Lifecycle management is tightly coupled — acquisitions, dispositions, revaluations happen at entity level and cascade to assets.

---

## Industry Platforms Referenced

| Platform | Focus |
|---|---|
| CEPRES | PE portfolio analytics, cash flow benchmarking |
| Chronograph | LP portfolio monitoring, fund-level reporting |
| eFront (BlackRock) | Full lifecycle PE/RE fund management |
| Allvue | GP fund accounting, portfolio management |
| PitchBook / Preqin | Market data, deal sourcing, fund benchmarks |
| MyFO | Family office / multi-asset portfolio tracking |

---

## Mapping to Our Architecture

| Industry Term | Our Equivalent | Source System |
|---|---|---|
| Management Company / GP | `investment_team_dimension` | enterprise_data |
| Fund | `portfolio_group_dimension` | enterprise_data |
| Portfolio | `portfolio_dimension` | enterprise_data |
| Portfolio Company / Entity | `entity_dimension` | Source_Entity_Management |
| Asset | `asset_dimension` | Source_Asset_Management |
| Security | `security_dimension` | Source_Security_Management |

---

## Relationship Cardinality (Confirmed)

```
investment_team (GP)  ──1:N──→  portfolio_group (Fund)
portfolio_group       ──1:N──→  portfolio
portfolio             ──N:M──→  entity        (via ownership bridge with %)
entity                ──1:N──→  asset
asset                 ──1:N──→  security
security              ──N:1──→  entity        (many securities → 1 entity)
```

**Security bridges Team, Asset, and Entity.** It is the instrument-level grain that connects the organizational hierarchy (team/fund/portfolio) to the investment hierarchy (entity/asset).

**Two fact tables:**
- `position_fact` — summarized positions (portfolio × entity × security × date)
- `position_transactions_fact` — detailed daily transactions (by security_id)

---

## Architecture Gaps Identified (Pre-Revision)

1. **No explicit ownership % at Fund→Entity level** — requires bridge table with `ownership_pct`
2. **Asset sourcing was unassigned** — now addressed by Source_Asset_Management
3. **Security master was not a separate system** — now addressed by Source_Security_Management (composite: internal records + WSO feeds)

---

## 6-Source-System Model (Adopted)

```
enterprise_data               → investment_team, portfolio_group, portfolio
Source_Entity_Management       → entity, entity→asset ownership (with %)
Source_Asset_Management        → asset master data, asset types, valuations
Source_Security_Management     → security master (composite: internal + WSO)
Source_Transaction_Management  → transaction records (daily, by security_id)
Source_Wall_Street_Online      → public market security data, pricing
```

WSO feeds into Source_Security_Management as an input. The security master is the authoritative composite — it reconciles internal deal-originated securities with external market identifiers.
