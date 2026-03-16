# ioBroker.go-e-gemini-adapter

Custom ioBroker adapter for go-e Gemini chargers with deterministic control logic and clear object IDs.

## Charging modes

- `1 = PV only`
- `2 = PV only (go-e = priority)`
- `3 = grid mode`

All modes are selectable via `control.mode`.

## Core control states

- `control.allowCharging` (global master switch)
- `control.simulationMode` (dry-run, no write commands)
- `control.mode`
- `control.gridManual.currentA`
- `control.gridManual.phaseMode` (`0 auto / 1 single / 2 three`)
- `control.minCurrentA`
- `control.maxCurrentA`
- `control.targetSocEnabled`
- `control.targetSocPercent`

## Required input datapoints (positive-only model)

Configure these in instance settings. Use separate positive values, no signed direction datapoints.

- `gridExportObjectId` -> current grid feed-in [W]
- `gridImportObjectId` -> current grid import [W]
- `pvPowerObjectId` -> current PV power [W]
- `houseConsumptionObjectId` -> current house consumption [W]
- `homeBatteryChargeObjectId` -> home battery charging power [W]
- `homeBatteryDischargeObjectId` -> home battery discharging power [W]
- `carSocObjectId` -> optional car SoC [%]

## Formulas

### Mode 1: PV only

`availablePowerW = gridExportW - gridImportW - reservePowerW`

### Mode 2: PV only (go-e = priority)

`nonEvHouseConsumptionW = houseConsumptionW - chargerPowerW`

`availablePowerW = pvPowerW - nonEvHouseConsumptionW + homeBatteryChargeW - homeBatteryDischargeW - reservePowerW`

### Mode 3: grid mode

No PV formula; manual current + phase mode are applied.

## Implemented safety/stability features

- Start/stop delays (`startDelaySec`, `stopDelaySec`)
- Phase-switch hysteresis + hold time (`phaseSwitchUpThresholdW`, `phaseSwitchHysteresisW`, `phaseSwitchMinHoldSec`)
- Stale-input protection (`maxInputAgeSec`)
  - In PV modes charging is blocked if required inputs are too old/missing.
- Optional max grid import limiter (`maxGridImportW`)
  - `-1` disables this limiter.
- Simulation mode (global dry-run)
  - Available as default config and runtime state.

## API support

- HTTP API v1 + v2
- MQTT status + command topics

### HTTP

- read: `/status` (+ optional `/api/status`)
- write v1 keys: `/mqtt?payload=key=value`
- write v2 phase mode: `/api/set?psm=...`

### MQTT

- status: `<prefix>/<serial>/status`
- command: `<prefix>/<serial>/cmd/req`

(default prefix: `go-eCharger`)

## Diagnostic states

- `status.decision`
- `diagnostics.lastError`
- `diagnostics.inputsStale`
- `diagnostics.staleInputList`
- `diagnostics.oldestInputAgeSec`

