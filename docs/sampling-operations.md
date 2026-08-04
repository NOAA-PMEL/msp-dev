# EnvOps Sampling Architecture

The EnvOps Sampling Architecture is a highly decoupled, distributed edge-to-cloud control system designed for autonomous environmental data acquisition. It is divided into two primary subsystems: the **Sampling System** (Data Engine) and **Sampling Operations** (State Machine).

## 1. Sampling System (The Data Engine)
The `sampling-system` acts as a high-performance MQTT bridge and data calculator.
* **Ingestion & Caching:** It listens to raw MQTT telemetry from hardware devices and buffers it in memory.
* **Variable Mapping:** It uses GitOps-defined `PlatformVariableMaps` to map raw, proprietary hardware variables (e.g., `Ta`) into standardized, semantic variables (e.g., `air_temperature`).
* **Two-Tier Time Bucketing:** It aligns jittery, asynchronous MQTT data into strict time buckets (e.g., 1-second intervals). It uses a Two-Tier execution model:
  * **Tier 1 (Direct):** Averages/aggregates raw hardware data and publishes it.
  * **Tier 2 (Derived):** Waits for Tier 1 to finish, then executes Python-based mathematical models (e.g., calculating True Wind Speed from Relative Wind and Platform Speed) using the freshly aggregated data.

## 2. Sampling Operations (The State Machine)
The operations suite is a declarative, hierarchical state machine distributed across edge nodes. It is broken into four microservices:
1. **Sampling Conditions:** The lowest level. It evaluates raw telemetry against predefined bounds (e.g., `LimitMinMax`, `LatLonRegionLocation`). Example: *Is the wind speed < 100 m/s?*
2. **Sampling States:** The stabilization layer. It acts as a time-gated watchdog. Example: *Has the wind speed been < 100 m/s continuously for the last 10 seconds?* If yes, the state becomes "Stabilized".
3. **Sampling Modes:** The active logic layer. It requires specific `SamplingStates` to be stabilized before executing a list of physical `SamplingActions` (e.g., toggling a 24V relay or adjusting a flow controller setpoint).
4. **System Modes:** The orchestration layer (`startup`, `normal`, `shutdown`). It commands which `SamplingModes` should be running. A designated "Primary Controller" node dictates the System Mode, and all replica nodes in the fleet automatically align their states to follow it.