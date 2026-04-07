# HOWTO: AI Server GPU Monitoring on Apple Silicon

Monitoring GPU utilization on Apple Silicon (M1/M2/M3/M4 Ultra etc.) when
running local LLM inference servers like Ollama.

## Quick comparison of existing tools

| Tool | Language | Sudo? | Apple Silicon | Install | Stars |
|------|----------|-------|---------------|---------|-------|
| **macmon** | Rust | No | M1-M5 | `brew install macmon` | ~1.4k |
| **mactop** | Go | Yes | M1-M4 | `brew install mactop` | ~2.3k |
| **asitop** | Python | Yes | M1-M2 | `pip install asitop` | ~4.5k |
| **nvtop** | C | No | limited M1/M2 | `brew install nvtop` | ~10k |

### Recommendation: macmon

`macmon` is the best current option for M3 Ultra.  No root required, full
metrics, multiple output modes.

**Metrics exposed:** GPU utilization %, GPU frequency (MHz), GPU power (W),
GPU temperature, CPU utilization/frequency/power, ANE power, RAM/swap usage.

#### Interactive TUI

Installation: `brew install macmon`

Running it:

```bash
macmon
```

#### JSON pipe (for scripting / dashboards)

```bash
# One JSON object per line, every 1 second
macmon pipe -s 1 -i 1000
```

Example output:

```json
{
  "gpu_usage": [461, 0.021],
  "gpu_power": 0.017,
  "temp": {"gpu_temp_avg": 38.2},
  "memory": {"ram_total": 549755813888, "ram_usage": 42949672960},
  "cpu_power": {"total": 0.85}
}
```

`gpu_usage` is `[frequency_mhz, utilization_fraction]`.

#### Prometheus metrics server

```bash
macmon serve --port 9090
```

Exposes `/metrics` endpoint for scraping with Prometheus/Grafana.

---

## Low-level macOS APIs for GPU metrics

If you need to build custom tooling, macOS exposes GPU data through
these interfaces.

### ioreg (no sudo)

The IOKit `AGXAccelerator` driver exposes real-time GPU statistics:

```bash
ioreg -r -c AGXAccelerator -d 1
```

Available fields:

| Field | Description |
|-------|-------------|
| `Device Utilization %` | Overall GPU utilization (0-100) |
| `Renderer Utilization %` | Render pipeline utilization |
| `Tiler Utilization %` | Tiler pipeline utilization |
| `In use system memory` | GPU memory actively in use (bytes) |
| `Alloc system memory` | Total GPU memory allocated (bytes) |

Python example:

```python
import subprocess
import re

def get_gpu_stats() -> dict[str, int]:
    out = subprocess.run(
        ["ioreg", "-r", "-c", "AGXAccelerator", "-d", "1"],
        capture_output=True, text=True,
    ).stdout
    stats: dict[str, int] = {}
    for key in [
        "Device Utilization %",
        "Renderer Utilization %",
        "Tiler Utilization %",
        "In use system memory",
        "Alloc system memory",
    ]:
        m = re.search(rf'"{re.escape(key)}"\s*=\s*(\d+)', out)
        if m:
            stats[key] = int(m.group(1))
    return stats
```

**Limitations:** no frequency, no power, no temperature.

### powermetrics (requires sudo)

The `gpu_power` sampler provides frequency, utilization residency, and
estimated power draw:

```bash
sudo powermetrics -n 1 -i 1000 --samplers gpu_power
```

Machine-readable plist output:

```bash
sudo powermetrics -n 1 -i 1000 --samplers gpu_power -f plist
```

Python example:

```python
import plistlib
import subprocess

def get_gpu_power_metrics() -> dict:
    result = subprocess.run(
        [
            "sudo", "powermetrics",
            "-n", "1", "-i", "1000",
            "--samplers", "gpu_power",
            "-f", "plist",
        ],
        capture_output=True,
    )
    # powermetrics emits NUL-separated plist chunks
    data = plistlib.loads(result.stdout.split(b"\x00")[0])
    return data.get("gpu", {})
```

**Provides:** GPU active residency (%), frequency (MHz), estimated power (W).
**Limitations:** needs root, no memory usage.

### libIOReport.dylib (no sudo, advanced)

The private `IOReport` framework is what `macmon` and Apple's Activity
Monitor use internally.  Accessible via `ctypes`:

```python
import ctypes

lib = ctypes.CDLL("/usr/lib/libIOReport.dylib")
# Key functions: IOReportCopyChannelsInGroup, IOReportCreateSubscription,
# IOReportCreateSamples, IOReportCreateSamplesDelta,
# IOReportSimpleGetIntegerValue, IOReportStateGetResidency, etc.
```

This gives GPU frequency, power, utilization, and temperature without
sudo, but requires writing a CoreFoundation/IOReport ctypes wrapper.
See the `macmon` and `socpowerbud` source code for reference
implementations.

### sysctl (not useful for GPU)

`sysctl` only exposes IOGPU memory management tunables, not runtime
utilization.  Use `ioreg` or `powermetrics` instead.

### system_profiler (static info only)

```bash
system_profiler SPDisplaysDataType
```

Returns chipset model, core count, Metal support version.  No runtime
metrics.

---

## Monitoring Ollama specifically

### Ollama built-in metrics

Ollama exposes `/api/ps` to list loaded models and their resource usage:

```bash
curl -s http://localhost:11434/api/ps | python -m json.tool
```

### Combining GPU + Ollama monitoring

A practical monitoring loop for an Ollama server on Apple Silicon:

```bash
# Terminal 1: GPU metrics (pick one)
macmon
# or: watch -n1 'ioreg -r -c AGXAccelerator -d 1 | grep "Utilization\|memory"'

# Terminal 2: Ollama model status
watch -n5 'curl -s http://localhost:11434/api/ps'
```

For JSON-based dashboards, `macmon pipe` combined with Ollama API polling
gives a complete picture without needing sudo.

---

## Hardware reference: Mac Studio M3 Ultra

| Spec | Value |
|------|-------|
| GPU cores | 60 or 80 (depends on config) |
| Unified memory | up to 512 GB (shared CPU/GPU) |
| Memory bandwidth | 819.2 GB/s |
| Neural Engine | 32-core ANE |
| Metal version | Metal 3 |

Apple Silicon uses **unified memory** -- there is no separate "VRAM".
The `Alloc system memory` and `In use system memory` fields from `ioreg`
reflect the GPU's share of the unified memory pool.
