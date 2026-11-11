# EtherConnectedBallisticFiles

> **Hypersonic Multi-Link Peer-to-Peer File Acceleration over Ethernet**  
> Native Desktop Application powered by **Tauri v2 + Rust**, built with a **Strict 3-Color Monochrome Palette** (`#000000`, `#1E1E1E`, `#FFFFFF`), **Zero Color Gradients**, and **Dual-Stage In-Flight File Payload Animations**.

---

## Strict 3-Color Design System & Visual Geometry

The application enforces a strict, uncompromising 3-color Swiss brutalist-minimalist architecture:

- **Black (`#000000`)**: Canvas backgrounds, window frame, inverted buttons, deep contrast backdrops.
- **Dark Grey (`#1E1E1E`)**: Card surfaces, modal dialogs, form controls, sidebar navigation.
- **White (`#FFFFFF`)**: Primary typography, 1px geometric borders, active badges, particle streams, in-flight file blocks.
- **Zero Color Gradients**: All `linear-gradient` and `radial-gradient` properties are completely eliminated. All elements, progress fills, telemetry graphs, and particle paths use solid flat geometry.

---

## Features & Kinetic Engine

- **Dual-Stage Transfer Animation**:
  - **In-Flight File Payload Corridor**: Watch the actual file (e.g. `Studio_Master_4K_Build.zip`) disassemble at the Sender Pod (`WORKSTATION-PRO`), travel as discrete numbered solid white blocks (`BLK #001`, `BLK #002`) across the dual 2.5 Gbps and 1.0 Gbps physical corridors, and reassemble in real time at the Receiver Pod (`DESKTOP-STUDIO`).
  - **Multi-Link Ballistic Particle Stream**: 60fps kinetic particle stream rendered on HTML5 canvas with solid white heads and solid trailing streaks (switchable between *Hyper-Rail*, *Coil Arc*, and *Quantum Scatter* modes).
  - **Live Speed Telemetry**: Real-time throughput plotted with a crisp solid 2px white line on a dark grey grid with Current, Average, and Peak bandwidth meters.
  - **24-Cell Dynamic Chunk Matrix**: Adaptive 16 MB chunk grid featuring instant link failover redistribution when a physical cable is unplugged.
- **Tauri v2 + Rust Desktop Engine**:
  - Frameless custom drag-titlebar with native caption controls (Minimize, Maximize, Close).
  - Native hardware network adapter enumeration using `sysinfo`.
  - Bit-perfect SHA-256 integrity validation engine powered by `sha2`.
  - Windows System Tray minimization with background transfer tracking.
- **Interactive Workflows & Simulator**:
  - 3-step Send Files Wizard with local file browsing.
  - Cryptographic device pairing modal with 6-digit PIN and Ed25519 identity verification.
  - Incoming transfer approval dialog with 3 distinct scenario simulations.
  - 12 empty & error state presets (unplugged cables, firewall blocks, insufficient disk, corrupted hashes).

---

## 5-Phase Architecture Roadmap

1. **Phase 1: Foundation & Tauri v2 Scaffold** — Cargo workspace, Tauri v2 configuration, window parameters, and dependencies (`sysinfo`, `sha2`, `tokio`).
2. **Phase 2: Strict 3-Color UI Architecture (Zero Gradients)** — 3-color CSS token system, gradient eradication, and 5 core view routers (Home, Devices, Transfers, History, Settings).
3. **Phase 3: Dual-Stage Transfer Animation** — Visual in-flight file payload corridor, ballistic particle accelerator, solid telemetry chart, and adaptive chunk matrix.
4. **Phase 4: Rust Networking & Cryptographic Integrity Backend** — Physical NIC probing, multi-link scheduler, and native SHA-256 validation.
5. **Phase 5: Native Window Controls, System Tray, Edge Cases & Packaging** — Frameless drag handling, system tray flyout, 12-state simulation engine, and desktop packaging.

---

## Running the Application

### Option A: Tauri Desktop App (Recommended)
```powershell
# In PowerShell:
$env:CARGO_TARGET_DIR = "$env:USERPROFILE\.cargo-target\ecbf"
npx @tauri-apps/cli dev
```

### Option B: Standalone Web Prototype
Open `ui/index.html` or `index.html` directly in any web browser:
```powershell
Start-Process .\ui\index.html
```

---

## License

MIT License. Designed and developed with pride.
