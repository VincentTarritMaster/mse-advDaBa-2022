import subprocess
import matplotlib.pyplot as plt
import numpy as np

# -----------------------------
# Helper: convert HH:MM:SS → seconds
# -----------------------------
def time_to_seconds(t):
    h, m, s = map(int, t.split(":"))
    return h * 3600 + m * 60 + s

# -----------------------------
# Get logs from kubectl
# -----------------------------
cmd = [
    "kubectl", "logs", "importer-c8wmt",
    "-n", "tarrit-adv-daba-26"
]

process = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)

times = []
totals = []
elapsed = []

start_parsing = False

for line in process.stdout:
    line = line.strip()

    if "Constraints ensured." in line:
        start_parsing = True
        continue

    if not start_parsing or not line:
        continue

    parts = line.split(",")

    if len(parts) < 4:
        continue

    try:
        totals.append(int(parts[1]))                     # column 2
        times.append(int(parts[2]))                      # column 3
        elapsed.append(time_to_seconds(parts[3]))        # column 4
    except ValueError:
        continue

# -----------------------------
# Plot
# -----------------------------
fig, ax1 = plt.subplots(figsize=(12, 6))

x = np.arange(len(times))

# Main curve
ax1.plot(times, label="Batch time")

z = np.polyfit(x, times, 10)
trend = np.poly1d(z)
ax1.plot(x, trend(x), linestyle='--', linewidth=2, label="Trend")

ax1.set_xlabel("Batch index")
ax1.set_ylabel("Time (seconds)")
ax1.set_title("Batch processing time over time")
ax1.grid(True)
ax1.legend()

# -----------------------------
# Top axis 1 → processed items
# -----------------------------
ax2 = ax1.twiny()
ax2.set_xlim(ax1.get_xlim())

if len(times) > 0:
    tick_idx = np.linspace(0, len(times) - 1, 6, dtype=int)

    ax1.set_xticks(tick_idx)
    ax2.set_xticks(tick_idx)

    ax2.set_xticklabels([f"{totals[i]:,}" for i in tick_idx])

ax2.set_xlabel("Processed items")

# -----------------------------
# Top axis 2 → elapsed time
# -----------------------------
ax3 = ax1.twiny()

# Move above the first top axis
ax3.spines["top"].set_position(("outward", 50))

ax3.set_xlim(ax1.get_xlim())
ax3.set_xticks(tick_idx)

def format_time(seconds):
    m = seconds // 60
    s = seconds % 60
    return f"{m}:{s:02d}"

ax3.set_xticklabels([format_time(elapsed[i]) for i in tick_idx])
ax3.set_xlabel("Elapsed time")

# Improve spacing
ax2.tick_params(axis='x', pad=5)
ax3.tick_params(axis='x', pad=25)

plt.tight_layout()
plt.show()