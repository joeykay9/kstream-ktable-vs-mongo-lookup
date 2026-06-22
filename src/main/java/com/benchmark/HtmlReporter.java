package com.benchmark;

import com.benchmark.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class HtmlReporter {
    private static final Logger log = LoggerFactory.getLogger(HtmlReporter.class);
    private static final DateTimeFormatter FILE_FMT    = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    private static final DateTimeFormatter DISPLAY_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String[] PALETTE = {"#22c55e", "#3b82f6", "#f97316", "#ef4444", "#a855f7", "#14b8a6"};

    public Path write(List<BenchmarkCollector.Stats> headlineStats,
                      List<BenchmarkCollector.RateRun> rateRuns,
                      Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        LocalDateTime now = LocalDateTime.now();
        Path file = outputDir.resolve("benchmark-" + now.format(FILE_FMT) + ".html");
        Files.writeString(file, buildHtml(headlineStats, rateRuns, now));
        log.info("HTML report → file://{}", file.toAbsolutePath());
        return file;
    }

    private String buildHtml(List<BenchmarkCollector.Stats> headline,
                             List<BenchmarkCollector.RateRun> rateRuns,
                             LocalDateTime ts) {
        // ── bar chart: latency percentiles at max rate ───────────────────────
        StringBuilder barDatasets  = new StringBuilder();
        StringBuilder tableRows    = new StringBuilder();
        for (int i = 0; i < headline.size(); i++) {
            BenchmarkCollector.Stats s = headline.get(i);
            String color = PALETTE[i % PALETTE.length];
            if (i > 0) barDatasets.append(",");
            barDatasets.append(String.format(
                "{label:'%s',backgroundColor:'%s',data:[%.1f,%.1f,%.1f,%.1f,%.1f]}",
                s.label(), color, s.min(), s.mean(), s.p50(), s.p99(), s.p999()));
            tableRows.append(String.format(
                "<tr><td>%s</td><td>%.0f</td><td>%.0f</td><td>%.0f</td><td>%.0f</td><td>%.0f</td><td>%d</td></tr>",
                s.label(), s.min(), s.mean(), s.p50(), s.p99(), s.p999(), s.count()));
        }

        // ── line chart: p99 latency vs target rate ───────────────────────────
        StringBuilder rateLabels   = new StringBuilder();
        StringBuilder lineDatasets = new StringBuilder();
        for (int r = 0; r < rateRuns.size(); r++) {
            if (r > 0) rateLabels.append(",");
            rateLabels.append(rateRuns.get(r).ratePerSecond());
        }
        int numStrategies = headline.size();
        for (int i = 0; i < numStrategies; i++) {
            String label = rateRuns.get(0).statsList().get(i).label();
            String color = PALETTE[i % PALETTE.length];
            StringBuilder data = new StringBuilder();
            for (int r = 0; r < rateRuns.size(); r++) {
                if (r > 0) data.append(",");
                data.append(String.format("%.1f", rateRuns.get(r).statsList().get(i).p99()));
            }
            if (i > 0) lineDatasets.append(",");
            lineDatasets.append(String.format(
                "{label:'%s',borderColor:'%s',backgroundColor:'%s',pointBackgroundColor:'%s'," +
                "tension:0.3,fill:false,data:[%s]}",
                label, color, color, color, data));
        }

        int headlineRate = rateRuns.get(rateRuns.size() - 1).ratePerSecond();

        return TEMPLATE
            .replace("{{TIMESTAMP}}",      ts.format(DISPLAY_FMT))
            .replace("{{TIMESTAMP_FILE}}", ts.format(FILE_FMT))
            .replace("{{ORDER_COUNT}}",    String.valueOf(AppConfig.THROUGHPUT_ORDER_COUNT))
            .replace("{{PRODUCT_COUNT}}",  String.valueOf(AppConfig.PRODUCT_COUNT))
            .replace("{{WINDOW_MS}}",      String.valueOf(AppConfig.WINDOW_MS))
            .replace("{{HEADLINE_RATE}}",  String.valueOf(headlineRate))
            .replace("{{BAR_DATASETS}}",   barDatasets)
            .replace("{{TABLE_ROWS}}",     tableRows)
            .replace("{{RATE_LABELS}}",    rateLabels)
            .replace("{{LINE_DATASETS}}",  lineDatasets);
    }

    private static final String TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Benchmark Results</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 960px; margin: 48px auto; padding: 0 24px; color: #1e293b; }
    h1 { font-size: 1.4rem; font-weight: 700; margin: 0 0 6px; }
    .meta { color: #64748b; font-size: 0.85rem; margin: 0 0 36px; }
    .section { margin-bottom: 48px; }
    .section-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; }
    .section-header h2 { font-size: 0.85rem; font-weight: 600; color: #475569; text-transform: uppercase; letter-spacing: .05em; margin: 0; }
    .chart-wrap { position: relative; height: 420px; background: #fff; }
    button.export {
      font: 0.8rem/1 system-ui, sans-serif; color: #3b82f6; background: #eff6ff;
      border: 1px solid #bfdbfe; border-radius: 6px; padding: 6px 14px;
      cursor: pointer; transition: background .15s;
    }
    button.export:hover { background: #dbeafe; }
    table { width: 100%; border-collapse: collapse; font-size: 0.9rem; }
    th, td { padding: 10px 16px; text-align: right; border-bottom: 1px solid #e2e8f0; font-variant-numeric: tabular-nums; }
    th:first-child, td:first-child { text-align: left; font-variant-numeric: normal; }
    th { background: #f8fafc; font-weight: 600; color: #475569; font-size: 0.8rem; letter-spacing: .04em; text-transform: uppercase; }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #f8fafc; }
  </style>
</head>
<body>
  <h1>Enrichment Latency Benchmark</h1>
  <p class="meta">{{TIMESTAMP}} &nbsp;·&nbsp; {{ORDER_COUNT}} orders per rate &nbsp;·&nbsp; {{PRODUCT_COUNT}} products &nbsp;·&nbsp; Mongo-Batch window: {{WINDOW_MS}}ms</p>

  <div class="section">
    <div class="section-header">
      <h2>Latency distribution at {{HEADLINE_RATE}} rec/s (ms)</h2>
      <button class="export" onclick="exportPng('bar')">Download PNG</button>
    </div>
    <div class="chart-wrap">
      <canvas id="barChart"></canvas>
    </div>
  </div>

  <div class="section">
    <div class="section-header">
      <h2>p99 latency vs throughput rate (ms)</h2>
      <button class="export" onclick="exportPng('line')">Download PNG</button>
    </div>
    <div class="chart-wrap">
      <canvas id="lineChart"></canvas>
    </div>
  </div>

  <table>
    <thead>
      <tr><th>Strategy</th><th>min</th><th>mean</th><th>p50</th><th>p99</th><th>p99.9</th><th>n</th></tr>
    </thead>
    <tbody>
      {{TABLE_ROWS}}
    </tbody>
  </table>

  <script>
    const barChart = new Chart(document.getElementById('barChart'), {
      type: 'bar',
      data: {
        labels: ['min', 'mean', 'p50', 'p99', 'p99.9'],
        datasets: [{{BAR_DATASETS}}]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top' },
          tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(1) + ' ms' } }
        },
        scales: { y: { beginAtZero: true, title: { display: true, text: 'Latency (ms)' } } }
      }
    });

    const lineChart = new Chart(document.getElementById('lineChart'), {
      type: 'line',
      data: {
        labels: [{{RATE_LABELS}}],
        datasets: [{{LINE_DATASETS}}]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top' },
          tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(0) + ' ms p99' } }
        },
        scales: {
          x: { title: { display: true, text: 'Target rate (rec/s)' } },
          y: { beginAtZero: true, title: { display: true, text: 'p99 latency (ms)' } }
        }
      }
    });

    function exportPng(which) {
      const canvas = document.getElementById(which === 'bar' ? 'barChart' : 'lineChart');
      const out = document.createElement('canvas');
      out.width = canvas.width; out.height = canvas.height;
      const ctx = out.getContext('2d');
      ctx.fillStyle = '#ffffff';
      ctx.fillRect(0, 0, out.width, out.height);
      ctx.drawImage(canvas, 0, 0);
      const a = document.createElement('a');
      a.href = out.toDataURL('image/png');
      a.download = 'benchmark-{{TIMESTAMP_FILE}}-' + which + '.png';
      a.click();
    }
  </script>
</body>
</html>
""";
}
