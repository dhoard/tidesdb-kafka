#!/usr/bin/env python3
"""
Benchmark Visualization Script
Generates charts from TidesDB vs RocksDB benchmark CSV files
"""

import warnings
warnings.filterwarnings('ignore', category=RuntimeWarning)

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pathlib import Path

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

def load_benchmark_data(csv_file):
    """Load benchmark data from CSV file"""
    if not os.path.exists(csv_file):
        print(f"Warning: {csv_file} not found")
        return None
    return pd.read_csv(csv_file)

def plot_performance_comparison(df, title, output_file):
    """Create a bar chart comparing TidesDB vs RocksDB"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = range(len(df))
    width = 0.35
    
    bars1 = ax.bar([i - width/2 for i in x], df['TidesDB_ms'], width, 
                   label='TidesDB', color='#2E86AB', alpha=0.8)
    bars2 = ax.bar([i + width/2 for i in x], df['RocksDB_ms'], width,
                   label='RocksDB', color='#A23B72', alpha=0.8)
    
    ax.set_xlabel('Number of Operations', fontsize=12, fontweight='bold')
    ax.set_ylabel('Time (milliseconds)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(df['Size'])
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height)}',
                   ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()

def plot_speedup_chart(df, title, output_file):
    """Create a line chart showing TidesDB speedup over RocksDB"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(df['Size'], df['Speedup'], marker='o', linewidth=2.5, 
            markersize=8, color='#F18F01', label='Speedup Factor')
    ax.axhline(y=1.0, color='red', linestyle='--', linewidth=1.5, 
               alpha=0.7, label='Break-even (1.0x)')
    
    ax.set_xlabel('Number of Operations', fontsize=12, fontweight='bold')
    ax.set_ylabel('Speedup (RocksDB time / TidesDB time)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    
    # Add value labels
    for i, (size, speedup) in enumerate(zip(df['Size'], df['Speedup'])):
        ax.text(size, speedup, f'{speedup:.2f}x', 
               ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()

def plot_throughput_comparison(df, title, output_file):
    """Create throughput comparison (ops/sec)"""
    df['TidesDB_ops_sec'] = (df['Size'] / df['TidesDB_ms']) * 1000
    df['RocksDB_ops_sec'] = (df['Size'] / df['RocksDB_ms']) * 1000
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = range(len(df))
    width = 0.35
    
    bars1 = ax.bar([i - width/2 for i in x], df['TidesDB_ops_sec'], width,
                   label='TidesDB', color='#2E86AB', alpha=0.8)
    bars2 = ax.bar([i + width/2 for i in x], df['RocksDB_ops_sec'], width,
                   label='RocksDB', color='#A23B72', alpha=0.8)
    
    ax.set_xlabel('Number of Operations', fontsize=12, fontweight='bold')
    ax.set_ylabel('Throughput (ops/sec)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(df['Size'])
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    # Add value labels (skip infinite or NaN values)
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height > 0 and height != float('inf') and not pd.isna(height):
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height):,}',
                       ha='center', va='bottom', fontsize=8, rotation=0)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()

def create_summary_table(benchmarks_dir, output_file):
    """Create a summary table of all benchmarks"""
    summaries = []
    
    for csv_file in sorted(Path(benchmarks_dir).glob('*.csv')):
        df = pd.read_csv(csv_file)
        if len(df) == 0:
            continue
        # Only include standard benchmark CSVs that have Benchmark + Speedup columns
        if 'Benchmark' not in df.columns or 'Speedup' not in df.columns:
            continue
        # Replace inf speedup with a large finite number for averaging
        speedups = df['Speedup'].replace([float('inf'), float('-inf')], float('nan')).dropna()
        if len(speedups) == 0:
            continue
        benchmark_name = df['Benchmark'].iloc[0]
        avg_speedup = speedups.mean()
        max_speedup = speedups.max()
        min_speedup = speedups.min()
        
        summaries.append({
            'Benchmark': benchmark_name,
            'Avg Speedup': f'{avg_speedup:.2f}x',
            'Min Speedup': f'{min_speedup:.2f}x',
            'Max Speedup': f'{max_speedup:.2f}x',
            'Winner': 'TidesDB' if avg_speedup > 1.0 else 'RocksDB'
        })
    
    summary_df = pd.DataFrame(summaries)
    
    # Create table visualization
    fig, ax = plt.subplots(figsize=(12, len(summaries) * 0.5 + 1))
    ax.axis('tight')
    ax.axis('off')
    
    table = ax.table(cellText=summary_df.values,
                     colLabels=summary_df.columns,
                     cellLoc='center',
                     loc='center',
                     colColours=['#2E86AB'] * len(summary_df.columns))
    
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)
    
    # Style header
    for i in range(len(summary_df.columns)):
        table[(0, i)].set_facecolor('#2E86AB')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    # Color winner column
    for i in range(len(summaries)):
        if summaries[i]['Winner'] == 'TidesDB':
            table[(i+1, 4)].set_facecolor('#C8E6C9')
        else:
            table[(i+1, 4)].set_facecolor('#FFCDD2')
    
    plt.title('Benchmark Summary: TidesDB vs RocksDB', 
             fontsize=14, fontweight='bold', pad=20)
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()
    
    return summary_df

def find_latest_csv(benchmarks_dir, base_name):
    """Find the latest timestamped CSV file for a given base name"""
    import glob
    # Look for timestamped files first (e.g., sequential_writes_20260126_221500.csv)
    pattern = os.path.join(benchmarks_dir, f'{base_name}_*.csv')
    files = glob.glob(pattern)
    if files:
        # Return the most recent one (sorted by name, timestamp makes it chronological)
        return sorted(files)[-1]
    # Fall back to non-timestamped file
    fallback = os.path.join(benchmarks_dir, f'{base_name}.csv')
    if os.path.exists(fallback):
        return fallback
    return None

def extract_timestamp(csv_path):
    """Extract timestamp from CSV filename if present"""
    import re
    match = re.search(r'_(\d{8}_\d{6})\.csv$', csv_path)
    if match:
        return match.group(1)
    return None

def format_large_number(n):
    """Format large numbers with K/M suffix"""
    if n >= 1000000:
        return f'{n/1000000:.0f}M' if n % 1000000 == 0 else f'{n/1000000:.1f}M'
    elif n >= 1000:
        return f'{n/1000:.0f}K' if n % 1000 == 0 else f'{n/1000:.1f}K'
    return str(n)

def plot_large_datasets_with_error_bars(df, title, output_file):
    """Create bar chart with error bars for large dataset benchmarks"""
    fig, ax = plt.subplots(figsize=(16, 8))
    
    x = range(len(df))
    width = 0.35
    
    bars1 = ax.bar([i - width/2 for i in x], df['TidesDB_avg_ms'], width,
                   yerr=df['TidesDB_stddev'], capsize=5,
                   label='TidesDB', color='#2E86AB', alpha=0.8)
    bars2 = ax.bar([i + width/2 for i in x], df['RocksDB_avg_ms'], width,
                   yerr=df['RocksDB_stddev'], capsize=5,
                   label='RocksDB', color='#A23B72', alpha=0.8)
    
    ax.set_xlabel('Number of Keys', fontsize=12, fontweight='bold')
    ax.set_ylabel('Time (milliseconds)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([format_large_number(s) for s in df['Size']], rotation=45, ha='right')
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    # Use log scale if values vary greatly
    max_val = max(df['TidesDB_avg_ms'].max(), df['RocksDB_avg_ms'].max())
    min_val = min(df['TidesDB_avg_ms'].min(), df['RocksDB_avg_ms'].min())
    if max_val > 0 and min_val > 0 and max_val / min_val > 100:
        ax.set_yscale('log')
        ax.set_ylabel('Time (milliseconds, log scale)', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()

def plot_large_datasets_throughput(df, title, output_file):
    """Create throughput chart for large datasets showing ops/sec"""
    fig, ax = plt.subplots(figsize=(16, 8))
    
    # Calculate throughput (ops/sec)
    df['TidesDB_ops_sec'] = (df['Size'] / df['TidesDB_avg_ms']) * 1000
    df['RocksDB_ops_sec'] = (df['Size'] / df['RocksDB_avg_ms']) * 1000
    
    x = range(len(df))
    width = 0.35
    
    bars1 = ax.bar([i - width/2 for i in x], df['TidesDB_ops_sec'], width,
                   label='TidesDB', color='#2E86AB', alpha=0.8)
    bars2 = ax.bar([i + width/2 for i in x], df['RocksDB_ops_sec'], width,
                   label='RocksDB', color='#A23B72', alpha=0.8)
    
    ax.set_xlabel('Dataset Size', fontsize=12, fontweight='bold')
    ax.set_ylabel('Throughput (ops/sec)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([format_large_number(s) for s in df['Size']], rotation=45, ha='right')
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    # Format y-axis with K/M suffix
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_large_number(int(x))))
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()

def plot_concurrent_throughput(df, title, output_file):
    """Create line chart for concurrent access throughput"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(df['Threads'], df['TidesDB_ops_sec'], marker='o', linewidth=2.5,
            markersize=10, color='#2E86AB', label='TidesDB')
    ax.plot(df['Threads'], df['RocksDB_ops_sec'], marker='s', linewidth=2.5,
            markersize=10, color='#A23B72', label='RocksDB')
    
    ax.set_xlabel('Number of Threads', fontsize=12, fontweight='bold')
    ax.set_ylabel('Throughput (ops/sec)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    
    # Add value labels
    for i, row in df.iterrows():
        ax.annotate(f'{int(row["TidesDB_ops_sec"]):,}', 
                   (row['Threads'], row['TidesDB_ops_sec']),
                   textcoords="offset points", xytext=(0,10), ha='center', fontsize=9)
        ax.annotate(f'{int(row["RocksDB_ops_sec"]):,}',
                   (row['Threads'], row['RocksDB_ops_sec']),
                   textcoords="offset points", xytext=(0,-15), ha='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()

def plot_concurrent_scalability(df, title, output_file):
    """Create scalability chart showing speedup vs threads"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Calculate speedup relative to single thread
    tides_base = df[df['Threads'] == 1]['TidesDB_ops_sec'].values[0]
    rocks_base = df[df['Threads'] == 1]['RocksDB_ops_sec'].values[0]
    
    df['TidesDB_scalability'] = df['TidesDB_ops_sec'] / tides_base
    df['RocksDB_scalability'] = df['RocksDB_ops_sec'] / rocks_base
    
    ax.plot(df['Threads'], df['TidesDB_scalability'], marker='o', linewidth=2.5,
            markersize=10, color='#2E86AB', label='TidesDB')
    ax.plot(df['Threads'], df['RocksDB_scalability'], marker='s', linewidth=2.5,
            markersize=10, color='#A23B72', label='RocksDB')
    ax.plot(df['Threads'], df['Threads'], linestyle='--', color='gray', 
            alpha=0.5, label='Linear scaling')
    
    ax.set_xlabel('Number of Threads', fontsize=12, fontweight='bold')
    ax.set_ylabel('Scalability (relative to 1 thread)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()

def plot_compaction_pressure(df, title, output_file):
    """Create line chart showing performance under compaction pressure"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    tides_df = df[df['Store'] == 'TidesDB']
    rocks_df = df[df['Store'] == 'RocksDB']
    
    # Write performance
    ax1.plot(tides_df['TotalKeys'], tides_df['WriteTime_ms'], marker='o', linewidth=2.5,
             markersize=10, color='#2E86AB', label='TidesDB')
    ax1.plot(rocks_df['TotalKeys'], rocks_df['WriteTime_ms'], marker='s', linewidth=2.5,
             markersize=10, color='#A23B72', label='RocksDB')
    ax1.set_xlabel('Total Keys in Store', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Write Time (ms)', fontsize=12, fontweight='bold')
    ax1.set_title('Write Performance Under Compaction Pressure', fontsize=12, fontweight='bold')
    ax1.legend(fontsize=11)
    ax1.grid(True, alpha=0.3)
    ax1.ticklabel_format(style='plain', axis='x')
    
    # Read performance
    ax2.plot(tides_df['TotalKeys'], tides_df['ReadTime_ms'], marker='o', linewidth=2.5,
             markersize=10, color='#2E86AB', label='TidesDB')
    ax2.plot(rocks_df['TotalKeys'], rocks_df['ReadTime_ms'], marker='s', linewidth=2.5,
             markersize=10, color='#A23B72', label='RocksDB')
    ax2.set_xlabel('Total Keys in Store', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Read Time (ms)', fontsize=12, fontweight='bold')
    ax2.set_title('Read Performance Under Compaction Pressure', fontsize=12, fontweight='bold')
    ax2.legend(fontsize=11)
    ax2.grid(True, alpha=0.3)
    ax2.ticklabel_format(style='plain', axis='x')
    
    plt.suptitle(title, fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()

def plot_memory_comparison(df, title, output_file):
    """Create bar chart comparing memory usage"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = range(len(df))
    width = 0.35
    
    # Convert bytes to MB
    tides_mem_mb = df['TidesDB_mem_bytes'] / (1024 * 1024)
    rocks_mem_mb = df['RocksDB_mem_bytes'] / (1024 * 1024)
    
    bars1 = ax.bar([i - width/2 for i in x], tides_mem_mb, width,
                   label='TidesDB', color='#2E86AB', alpha=0.8)
    bars2 = ax.bar([i + width/2 for i in x], rocks_mem_mb, width,
                   label='RocksDB', color='#A23B72', alpha=0.8)
    
    ax.set_xlabel('Number of Keys', fontsize=12, fontweight='bold')
    ax.set_ylabel('Memory Usage (MB)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([f'{s:,}' for s in df['Size']])
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    # Add value labels
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height > 0 and not pd.isna(height):
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.1f}',
                       ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()

def plot_metrics_combined(df, title, output_file):
    """Create combined metrics chart with write/read times and memory"""
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    
    x = range(len(df))
    width = 0.35
    
    # Write time
    axes[0].bar([i - width/2 for i in x], df['TidesDB_write_ms'], width,
                label='TidesDB', color='#2E86AB', alpha=0.8)
    axes[0].bar([i + width/2 for i in x], df['RocksDB_write_ms'], width,
                label='RocksDB', color='#A23B72', alpha=0.8)
    axes[0].set_xlabel('Keys', fontsize=11, fontweight='bold')
    axes[0].set_ylabel('Write Time (ms)', fontsize=11, fontweight='bold')
    axes[0].set_title('Write Performance', fontsize=12, fontweight='bold')
    axes[0].set_xticks(x)
    axes[0].set_xticklabels([f'{s//1000}K' for s in df['Size']])
    axes[0].legend(fontsize=10)
    axes[0].grid(axis='y', alpha=0.3)
    
    # Read time
    axes[1].bar([i - width/2 for i in x], df['TidesDB_read_ms'], width,
                label='TidesDB', color='#2E86AB', alpha=0.8)
    axes[1].bar([i + width/2 for i in x], df['RocksDB_read_ms'], width,
                label='RocksDB', color='#A23B72', alpha=0.8)
    axes[1].set_xlabel('Keys', fontsize=11, fontweight='bold')
    axes[1].set_ylabel('Read Time (ms)', fontsize=11, fontweight='bold')
    axes[1].set_title('Read Performance', fontsize=12, fontweight='bold')
    axes[1].set_xticks(x)
    axes[1].set_xticklabels([f'{s//1000}K' for s in df['Size']])
    axes[1].legend(fontsize=10)
    axes[1].grid(axis='y', alpha=0.3)
    
    # Memory usage
    tides_mem_mb = df['TidesDB_mem_bytes'] / (1024 * 1024)
    rocks_mem_mb = df['RocksDB_mem_bytes'] / (1024 * 1024)
    axes[2].bar([i - width/2 for i in x], tides_mem_mb, width,
                label='TidesDB', color='#2E86AB', alpha=0.8)
    axes[2].bar([i + width/2 for i in x], rocks_mem_mb, width,
                label='RocksDB', color='#A23B72', alpha=0.8)
    axes[2].set_xlabel('Keys', fontsize=11, fontweight='bold')
    axes[2].set_ylabel('Memory (MB)', fontsize=11, fontweight='bold')
    axes[2].set_title('Memory Usage', fontsize=12, fontweight='bold')
    axes[2].set_xticks(x)
    axes[2].set_xticklabels([f'{s//1000}K' for s in df['Size']])
    axes[2].legend(fontsize=10)
    axes[2].grid(axis='y', alpha=0.3)
    
    plt.suptitle(title, fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()

def plot_latency_percentiles(df, title, output_file):
    """Create grouped bar chart of latency percentiles per operation type.
    
    CSV columns: OpType, SampleOps,
      TidesDB_p50_ns .. TidesDB_max_ns, RocksDB_p50_ns .. RocksDB_max_ns
    """
    percentiles = ['p50', 'p90', 'p95', 'p99', 'p999', 'max']
    pctl_labels = ['p50', 'p90', 'p95', 'p99', 'p99.9', 'max']
    n_ops = len(df)
    
    fig, axes = plt.subplots(1, n_ops, figsize=(7 * n_ops, 7), squeeze=False)
    axes = axes[0]
    
    for idx, (_, row) in enumerate(df.iterrows()):
        ax = axes[idx]
        op = row['OpType']
        
        tides_vals = [row[f'TidesDB_{p}_ns'] / 1000.0 for p in percentiles]  # ns -> us
        rocks_vals = [row[f'RocksDB_{p}_ns'] / 1000.0 for p in percentiles]
        
        x = range(len(percentiles))
        width = 0.35
        
        bars1 = ax.bar([i - width/2 for i in x], tides_vals, width,
                       label='TidesDB', color='#2E86AB', alpha=0.8)
        bars2 = ax.bar([i + width/2 for i in x], rocks_vals, width,
                       label='RocksDB', color='#A23B72', alpha=0.8)
        
        ax.set_xlabel('Percentile', fontsize=11, fontweight='bold')
        ax.set_ylabel('Latency (\u00b5s)', fontsize=11, fontweight='bold')
        ax.set_title(f'{op.capitalize()} Latency ({row["SampleOps"]:,} ops)',
                     fontsize=12, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(pctl_labels)
        ax.legend(fontsize=10)
        ax.grid(axis='y', alpha=0.3)
        
        # Use log scale if tail latencies are much larger than median
        if max(tides_vals[-1], rocks_vals[-1]) > 10 * max(max(tides_vals[0], 1), max(rocks_vals[0], 1)):
            ax.set_yscale('log')
            ax.set_ylabel('Latency (\u00b5s, log scale)', fontsize=11, fontweight='bold')
        
        # Value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                h = bar.get_height()
                if h > 0 and not pd.isna(h):
                    label = f'{h:.1f}' if h < 100 else f'{h:.0f}'
                    ax.text(bar.get_x() + bar.get_width()/2., h, label,
                           ha='center', va='bottom', fontsize=7, rotation=45)
    
    plt.suptitle(title, fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()


def plot_latency_percentile_ratio(df, title, output_file):
    """Create line chart showing RocksDB/TidesDB latency ratio per percentile.
    
    Values > 1 mean TidesDB is faster; values < 1 mean RocksDB is faster.
    """
    percentiles = ['p50', 'p90', 'p95', 'p99', 'p999', 'max']
    pctl_labels = ['p50', 'p90', 'p95', 'p99', 'p99.9', 'max']
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    colors = {'write': '#2E86AB', 'read': '#A23B72', 'mixed': '#F18F01'}
    markers = {'write': 'o', 'read': 's', 'mixed': 'D'}
    
    for _, row in df.iterrows():
        op = row['OpType']
        ratios = []
        for p in percentiles:
            tides = row[f'TidesDB_{p}_ns']
            rocks = row[f'RocksDB_{p}_ns']
            ratio = rocks / tides if tides > 0 else float('nan')
            ratios.append(ratio)
        
        ax.plot(pctl_labels, ratios, marker=markers.get(op, 'o'), linewidth=2.5,
                markersize=10, color=colors.get(op, '#333333'), label=op.capitalize())
    
    ax.axhline(y=1.0, color='red', linestyle='--', linewidth=1.5, alpha=0.7,
               label='Break-even (1.0x)')
    
    ax.set_xlabel('Percentile', fontsize=12, fontweight='bold')
    ax.set_ylabel('Latency Ratio (RocksDB / TidesDB)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    
    # Annotate values
    for _, row in df.iterrows():
        op = row['OpType']
        for i, p in enumerate(percentiles):
            tides = row[f'TidesDB_{p}_ns']
            rocks = row[f'RocksDB_{p}_ns']
            ratio = rocks / tides if tides > 0 else float('nan')
            if not pd.isna(ratio):
                ax.text(i, ratio, f'{ratio:.2f}x', ha='center', va='bottom',
                       fontsize=8, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()


def generate_all_visualizations(benchmarks_dir='.', output_dir='charts'):
    """Generate all visualization charts"""
    
    benchmarks = {
        'sequential_writes': 'Sequential Write Performance',
        'random_writes': 'Random Write Performance',
        'sequential_reads': 'Sequential Read Performance',
        'random_reads': 'Random Read Performance',
        'mixed_workload': 'Mixed Workload Performance',
        'range_scans': 'Range Scan Performance',
        'bulk_writes': 'Bulk Write Performance (putAll)',
        'updates': 'Update Performance',
        'large_values': 'Large Value Performance',
        'iteration': 'Full Iteration Performance',
        'deletes': 'Delete Performance'
    }
    
    # Extended benchmarks
    extended_benchmarks = {
        'large_datasets': 'Large Dataset Performance (with warmup)',
        'concurrent_access': 'Concurrent Access Performance',
        'compaction_pressure': 'Compaction Pressure Test',
        'metrics': 'Performance with Memory/CPU Metrics',
        'latency_percentiles': 'Latency Percentile Distribution'
    }
    
    print("Generating benchmark visualizations...\n")
    
    # Find the timestamp from the first available CSV
    timestamp = None
    all_benchmark_names = list(benchmarks.keys()) + list(extended_benchmarks.keys())
    for base_name in all_benchmark_names:
        csv_path = find_latest_csv(benchmarks_dir, base_name)
        if csv_path:
            timestamp = extract_timestamp(csv_path)
            if timestamp:
                break
    
    # Create timestamped output directory if we have a timestamp
    if timestamp:
        output_dir = f'charts_{timestamp}'
    os.makedirs(output_dir, exist_ok=True)
    
    for base_name, title in benchmarks.items():
        csv_path = find_latest_csv(benchmarks_dir, base_name)
        if csv_path is None:
            print(f"Warning: {base_name}.csv not found")
            continue
            
        df = load_benchmark_data(csv_path)
        
        if df is not None:
            # Use timestamp in output filenames if available
            suffix = f'_{timestamp}' if timestamp else ''
            
            # Performance comparison
            plot_performance_comparison(
                df, 
                f'{title} - Time Comparison',
                os.path.join(output_dir, f'{base_name}_comparison{suffix}.png')
            )
            
            # Speedup chart
            plot_speedup_chart(
                df,
                f'{title} - TidesDB Speedup',
                os.path.join(output_dir, f'{base_name}_speedup{suffix}.png')
            )
            
            # Throughput comparison
            plot_throughput_comparison(
                df,
                f'{title} - Throughput Comparison',
                os.path.join(output_dir, f'{base_name}_throughput{suffix}.png')
            )
    
    # Generate extended benchmark visualizations
    print("\nGenerating extended benchmark visualizations...")
    suffix = f'_{timestamp}' if timestamp else ''
    
    # Large datasets with error bars
    csv_path = find_latest_csv(benchmarks_dir, 'large_datasets')
    if csv_path:
        df = load_benchmark_data(csv_path)
        if df is not None:
            plot_large_datasets_with_error_bars(
                df,
                'Large Dataset Performance (up to 25M keys, with warmup)',
                os.path.join(output_dir, f'large_datasets_comparison{suffix}.png')
            )
            # Throughput chart for large datasets
            plot_large_datasets_throughput(
                df.copy(),
                'Large Dataset Throughput (ops/sec)',
                os.path.join(output_dir, f'large_datasets_throughput{suffix}.png')
            )
            # Also create speedup chart for large datasets
            if 'Speedup' in df.columns:
                plot_speedup_chart(
                    df.rename(columns={'TidesDB_avg_ms': 'TidesDB_ms', 'RocksDB_avg_ms': 'RocksDB_ms'}),
                    'Large Dataset - TidesDB Speedup (up to 25M keys)',
                    os.path.join(output_dir, f'large_datasets_speedup{suffix}.png')
                )
    
    # Concurrent access
    csv_path = find_latest_csv(benchmarks_dir, 'concurrent_access')
    if csv_path:
        df = load_benchmark_data(csv_path)
        if df is not None:
            plot_concurrent_throughput(
                df,
                'Concurrent Access - Throughput by Thread Count',
                os.path.join(output_dir, f'concurrent_throughput{suffix}.png')
            )
            plot_concurrent_scalability(
                df.copy(),
                'Concurrent Access - Scalability',
                os.path.join(output_dir, f'concurrent_scalability{suffix}.png')
            )
    
    # Compaction pressure
    csv_path = find_latest_csv(benchmarks_dir, 'compaction_pressure')
    if csv_path:
        df = load_benchmark_data(csv_path)
        if df is not None:
            plot_compaction_pressure(
                df,
                'Performance Under Compaction Pressure',
                os.path.join(output_dir, f'compaction_pressure{suffix}.png')
            )
    
    # Metrics (memory/CPU)
    csv_path = find_latest_csv(benchmarks_dir, 'metrics')
    if csv_path:
        df = load_benchmark_data(csv_path)
        if df is not None:
            plot_memory_comparison(
                df,
                'Memory Usage Comparison',
                os.path.join(output_dir, f'memory_comparison{suffix}.png')
            )
            plot_metrics_combined(
                df,
                'Combined Performance Metrics',
                os.path.join(output_dir, f'metrics_combined{suffix}.png')
            )
    
    # Latency percentiles
    csv_path = find_latest_csv(benchmarks_dir, 'latency_percentiles')
    if csv_path:
        df = load_benchmark_data(csv_path)
        if df is not None:
            plot_latency_percentiles(
                df,
                'Latency Percentile Distribution (TidesDB vs RocksDB)',
                os.path.join(output_dir, f'latency_percentiles{suffix}.png')
            )
            plot_latency_percentile_ratio(
                df,
                'Latency Ratio by Percentile (RocksDB / TidesDB)',
                os.path.join(output_dir, f'latency_ratio{suffix}.png')
            )
    
    # Create summary table
    print("\nGenerating summary table...")
    create_summary_table(benchmarks_dir, os.path.join(output_dir, f'summary_table{suffix}.png'))
    
    print(f"\n✓ All visualizations created in: {output_dir}")
    print("\nBenchmark Analysis Complete!")
    print("=" * 60)
    print("Charts generated:")
    print("  - Performance comparisons (time)")
    print("  - Speedup charts")
    print("  - Throughput comparisons")
    print("  - Large dataset charts (with error bars)")
    print("  - Concurrent access charts (throughput & scalability)")
    print("  - Compaction pressure charts")
    print("  - Memory/CPU metrics charts")
    print("  - Latency percentile charts")
    print("  - Summary table")
    print("=" * 60)

if __name__ == '__main__':
    generate_all_visualizations()
