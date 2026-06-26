package com.benchmark.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class MemoryHtmlReporter {
    private static final Logger log = LoggerFactory.getLogger(MemoryHtmlReporter.class);
    private static final DateTimeFormatter FILE_FMT    = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    private static final DateTimeFormatter DISPLAY_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String[] PALETTE = {"#22c55e", "#3b82f6", "#f97316", "#ef4444", "#a855f7"};

    public Path write(List<MemoryResult> results, List<String> labels, int[] productCounts,
                      Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        LocalDateTime now = LocalDateTime.now();
        Path file = outputDir.resolve("memory-" + now.format(FILE_FMT) + ".html");
        Files.writeString(file, buildHtml(results, labels, productCounts, now));
        log.info("Memory report → file://{}", file.toAbsolutePath());
        return file;
    }

    private String buildHtml(List<MemoryResult> results, List<String> labels,
                              int[] productCounts, LocalDateTime ts) {
        // X-axis category labels: "1k", "10k", "100k", "500k"
        StringBuilder pcLabels = new StringBuilder();
        for (int j = 0; j < productCounts.length; j++) {
            if (j > 0) pcLabels.append(",");
            pcLabels.append("'").append(MemoryBenchmarkRunner.formatCount(productCounts[j])).append("'");
        }

        // One dataset per strategy for each chart
        StringBuilder rssDatasets     = new StringBuilder();
        StringBuilder rocksdbDatasets = new StringBuilder();
        StringBuilder tableRows       = new StringBuilder();

        for (int i = 0; i < labels.size(); i++) {
            String label = labels.get(i);
            String color = PALETTE[i % PALETTE.length];

            StringBuilder rssData     = new StringBuilder();
            StringBuilder rocksdbData = new StringBuilder();

            for (int j = 0; j < productCounts.length; j++) {
                if (j > 0) { rssData.append(","); rocksdbData.append(","); }
                MemoryResult r = MemoryBenchmarkRunner.find(results, label, productCounts[j]);
                rssData.append(r != null ? r.stats().rssMb() : 0);
                rocksdbData.append(r != null ? r.stats().rocksdbLiveDataMb() : 0);
            }

            if (i > 0) { rssDatasets.append(","); rocksdbDatasets.append(","); }
            String ds = "{label:'%s',borderColor:'%s',backgroundColor:'%s',pointBackgroundColor:'%s'," +
                        "tension:0.3,fill:false,data:[%s]}";
            rssDatasets.append(String.format(ds, label, color, color, color, rssData));
            rocksdbDatasets.append(String.format(ds, label, color, color, color, rocksdbData));

            // Table rows — one row per (strategy, productCount)
            for (int j = 0; j < productCounts.length; j++) {
                MemoryResult r = MemoryBenchmarkRunner.find(results, label, productCounts[j]);
                if (r == null) continue;
                MemoryStats s = r.stats();
                tableRows.append(String.format(
                    "<tr><td>%s</td><td>%s</td><td>%d</td><td>%d</td><td>%d</td><td>%d</td></tr>",
                    label,
                    MemoryBenchmarkRunner.formatCount(productCounts[j]),
                    s.heapUsedMb(), s.rocksdbBlockCacheMb(), s.rocksdbLiveDataMb(), s.rssMb()
                ));
            }
        }

        return TEMPLATE
            .replace("{{TIMESTAMP}}",        ts.format(DISPLAY_FMT))
            .replace("{{TIMESTAMP_FILE}}",   ts.format(FILE_FMT))
            .replace("{{PC_LABELS}}",        pcLabels)
            .replace("{{RSS_DATASETS}}",     rssDatasets)
            .replace("{{ROCKSDB_DATASETS}}", rocksdbDatasets)
            .replace("{{TABLE_ROWS}}",       tableRows);
    }

    private static final String TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Memory Footprint Benchmark</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 960px; margin: 48px auto; padding: 0 24px; color: #1e293b; }
    h1 { font-size: 1.4rem; font-weight: 700; margin: 0 0 6px; }
    .meta { color: #64748b; font-size: 0.85rem; margin: 0 0 36px; }
    .section { margin-bottom: 48px; }
    .section-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; }
    .section-header h2 { font-size: 0.85rem; font-weight: 600; color: #475569; text-transform: uppercase; letter-spacing: .05em; margin: 0; }
    .chart-wrap { position: relative; height: 400px; background: #fff; }
    button.export {
      font: 0.8rem/1 system-ui, sans-serif; color: #3b82f6; background: #eff6ff;
      border: 1px solid #bfdbfe; border-radius: 6px; padding: 6px 14px;
      cursor: pointer; transition: background .15s;
    }
    button.export:hover { background: #dbeafe; }
    table { width: 100%; border-collapse: collapse; font-size: 0.9rem; }
    th, td { padding: 10px 16px; text-align: right; border-bottom: 1px solid #e2e8f0; font-variant-numeric: tabular-nums; }
    th:first-child, td:first-child,
    th:nth-child(2), td:nth-child(2) { text-align: left; font-variant-numeric: normal; }
    th { background: #f8fafc; font-weight: 600; color: #475569; font-size: 0.8rem; letter-spacing: .04em; text-transform: uppercase; }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #f8fafc; }
    .note { margin-top: 32px; padding: 16px 20px; background: #f8fafc; border-left: 3px solid #cbd5e1; border-radius: 4px; font-size: 0.85rem; color: #475569; line-height: 1.6; }
    .note strong { color: #1e293b; }
  </style>
</head>
<body>
  <h1>Memory Footprint Benchmark</h1>
  <p class="meta">{{TIMESTAMP}} &nbsp;·&nbsp; per-strategy memory at rest after state store fully loaded</p>

  <div class="section">
    <div class="section-header">
      <h2>Process RSS (MB) vs dataset size</h2>
      <button class="export" onclick="exportPng('rss')">Download PNG</button>
    </div>
    <div class="chart-wrap">
      <canvas id="rssChart"></canvas>
    </div>
  </div>

  <div class="section">
    <div class="section-header">
      <h2>RocksDB live data (MB) vs dataset size — state store on-disk footprint</h2>
      <button class="export" onclick="exportPng('rocksdb')">Download PNG</button>
    </div>
    <div class="chart-wrap">
      <canvas id="rocksdbChart"></canvas>
    </div>
  </div>

  <table>
    <thead>
      <tr>
        <th>Strategy</th>
        <th>Dataset</th>
        <th>Heap used (MB)</th>
        <th>RocksDB block cache (MB)</th>
        <th>RocksDB live data (MB)</th>
        <th>RSS (MB)</th>
      </tr>
    </thead>
    <tbody>{{TABLE_ROWS}}</tbody>
  </table>

  <div class="note">
    <strong>How to read this:</strong>
    <em>KTable-Co</em> and <em>KTable</em> hold only one partition slice of the reference dataset per instance — memory grows as <em>dataset ÷ partition count</em>.
    <em>GlobalKTable</em> holds the full dataset on every instance regardless of parallelism — its curve diverges sharply at large dataset sizes.
    <em>Mongo-Batch</em> and <em>Mongo-Sync</em> hold no reference data in-process (RocksDB = 0); their RSS reflects baseline JVM + MongoDB driver overhead only.
    <br><br>
    RSS is measured via <code>ps -o rss=</code> after a forced GC and a 2-second settle period.
    RocksDB live-data-size is read from Kafka Streams metrics (requires <code>METRICS_RECORDING_LEVEL=DEBUG</code>).
  </div>

  <script>
    const lineOpts = {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'top' },
        tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y + ' MB' } }
      },
      scales: {
        x: { title: { display: true, text: 'Reference dataset size (products)' } },
        y: { beginAtZero: true, title: { display: true, text: 'Memory (MB)' } }
      }
    };

    const rssChart = new Chart(document.getElementById('rssChart'), {
      type: 'line',
      data: { labels: [{{PC_LABELS}}], datasets: [{{RSS_DATASETS}}] },
      options: lineOpts
    });

    const rocksdbChart = new Chart(document.getElementById('rocksdbChart'), {
      type: 'line',
      data: { labels: [{{PC_LABELS}}], datasets: [{{ROCKSDB_DATASETS}}] },
      options: lineOpts
    });

    function exportPng(which) {
      const canvas = document.getElementById(which === 'rss' ? 'rssChart' : 'rocksdbChart');
      const out = document.createElement('canvas');
      out.width = canvas.width; out.height = canvas.height;
      const ctx = out.getContext('2d');
      ctx.fillStyle = '#ffffff';
      ctx.fillRect(0, 0, out.width, out.height);
      ctx.drawImage(canvas, 0, 0);
      const a = document.createElement('a');
      a.href = out.toDataURL('image/png');
      a.download = 'memory-{{TIMESTAMP_FILE}}-' + which + '.png';
      a.click();
    }
  </script>
</body>
</html>
""";
}
