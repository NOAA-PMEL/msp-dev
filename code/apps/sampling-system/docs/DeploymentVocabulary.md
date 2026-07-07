# envds Deployment Vocabulary & ERDDAP Partitioning Guide

## Overview
To ensure seamless integration between the GitOps edge architecture and the Shore Server (ERDDAP / Datastore), it is critical to use a strict, controlled vocabulary for deployments. Qualitative or vague descriptors (e.g., "Autonomous_Observatory", "Vessel Transit") break the automated data partitioning logic.

By adhering to this three-tier taxonomy (`deployment_type`, `deployment_subtype`, and `trajectory_partition`), the Datastore can automatically slice continuous data streams into optimized NetCDF files without requiring complex geofences, manual intervention, or constant GitOps updates.

---

## 1. Deployment Type (`deployment_type`)
The highest-level classification describing the physical nature or vehicle of the deployment.

**Allowed Values (Examples):**
* `Ship`: Commercial vessels of opportunity (SOOP), research vessels, or ferries.
* `Station`: Fixed ground-based observatories or mobile containers acting as fixed stations (e.g., ALVAN container).
* `UAS`: Uncrewed Aerial Systems.
* `Mooring`: Oceanographic buoys and moorings.
* `Land-based Intercomparison`: Pre-deployment testing and sensor validation (e.g., PMEL Rooftop).
* `Hardware Integration`: Internal payload building and testing (e.g., assembling payloads inside enclosures).

---

## 2. Deployment Subtype (`deployment_subtype`)
Describes the operational mode and sampling cadence of the deployment. This field determines how the system manages the dataset lifecycle.

**Allowed Values:**
* `Continuous`: 24/7 autonomous monitoring. Designed for "ferrybox" operations where the physical location doesn't dictate the dataset boundaries. Data is chunked purely by time.
* `Transit`: A defined point-A to point-B physical trip (e.g., a specific ship Leg or cruise).
* `Event`: A discrete, human-initiated operation with a clear start and stop (e.g., a UAS flight, a CTD cast, a specific sampling run).

---

## 3. Trajectory Partition (`trajectory_partition`)
Instructs the Datastore on how to automatically slice the timeline into optimized ERDDAP trajectories. This completely eliminates the need for manual leg increments or geofencing.

**Allowed Values:**
* `monthly`: The Datastore automatically formats the trajectory ID using the current Year and Month. Ideal for continuous 1Hz ship data to maintain high ERDDAP performance.
* `annual`: The Datastore formats the trajectory ID using the current Year. Ideal for lower-bandwidth fixed stations.
* `none`: The timeline is not automatically sliced by time (typically used in conjunction with `Event` or `Transit` subtypes where the event itself bounds the data).

---

## Example: M/V Marjorie C Autonomous Deployment

**GitOps Configuration:**
```json
{
  "deployment_type": "Ship",
  "deployment_subtype": "Continuous",
  "trajectory_partition": "monthly"
}