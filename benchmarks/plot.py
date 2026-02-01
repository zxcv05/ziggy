#!/usr/bin/env python3
import matplotlib.pyplot as plt
import numpy as np

# Data from 100-run benchmark
metrics = ['Throughput\n(M ops/s)', 'p50\n(µs)', 'p99\n(µs)', 'p99.9\n(µs)', 'p99.99\n(µs)', 'p99.999\n(µs)']
ziggy = [6.36, 60.0, 92.2, 155.7, 195.1, 853.1]
crossbeam = [6.51, 46.2, 88.8, 153.9, 205.1, 551.3]

x = np.arange(len(metrics))
width = 0.35

fig, ax = plt.subplots(figsize=(12, 6))
bars1 = ax.bar(x - width/2, ziggy, width, label='Ziggy', color='#4a90d9')
bars2 = ax.bar(x + width/2, crossbeam, width, label='Crossbeam', color='#d94a4a')

ax.set_ylabel('Value')
ax.set_title('Ziggy vs Crossbeam (100 runs, 10P/10C)')
ax.set_xticks(x)
ax.set_xticklabels(metrics)
ax.legend()

# Add value labels on bars
def add_labels(bars):
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'{height:.1f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=8)

add_labels(bars1)
add_labels(bars2)

# Use log scale for y-axis since values vary widely
ax.set_yscale('log')
ax.set_ylabel('Value (log scale)')

plt.tight_layout()
plt.savefig('comparison.png', dpi=150)
print("Saved comparison.png")
