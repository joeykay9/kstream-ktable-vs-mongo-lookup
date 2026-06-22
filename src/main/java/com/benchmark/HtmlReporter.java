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

    public Path write(List<BenchmarkCollector.Stats> statsList, Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        LocalDateTime now = LocalDateTime.now();
        Path file = outputDir.resolve("benchmark-" + now.format(FILE_FMT) + ".html");
        Files.writeString(file, buildHtml(statsList, now));
        log.info("HTML report → file://{}", file.toAbsolutePath());
        return file;
    }

    private String buildHtml(List<BenchmarkCollector.Stats> stats, LocalDateTime ts) {
        StringBuilder datasets  = new StringBuilder();
        StringBuilder tableRows = new StringBuilder();

        for (int i = 0; i < stats.size(); i++) {
            BenchmarkCollector.Stats s = stats.get(i);
            String color = PALETTE[i % PALETTE.length];
            if (i > 0) datasets.append(",");
            datasets.append(String.format(
                "{label:'%s',backgroundColor:'%s',data:[%.1f,%.1f,%.1f,%.1f,%.1f]}",
                s.label(), color, s.min(), s.mean(), s.p50(), s.p99(), s.p999()
            ));
            tableRows.append(String.format(
                "<tr><td>%s</td><td>%.0f</td><td>%.0f</td><td>%.0f</td><td>%.0f</td><td>%.0f</td><td>%d</td></tr>",
                s.label(), s.min(), s.mean(), s.p50(), s.p99(), s.p999(), s.count()
            ));
        }

        return TEMPLATE
            .replace("{{TIMESTAMP}}",     ts.format(DISPLAY_FMT))
            .replace("{{ORDER_COUNT}}",   String.valueOf(AppConfig.ORDER_COUNT))
            .replace("{{PRODUCT_COUNT}}", String.valueOf(AppConfig.PRODUCT_COUNT))
            .replace("{{WINDOW_MS}}",     String.valueOf(AppConfig.WINDOW_MS))
            .replace("{{DATASETS}}",      datasets)
            .replace("{{TABLE_ROWS}}",    tableRows);
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
    .chart-wrap { position: relative; height: 420px; margin-bottom: 40px; }
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
  <p class="meta">{{TIMESTAMP}} &nbsp;·&nbsp; {{ORDER_COUNT}} orders &nbsp;·&nbsp; {{PRODUCT_COUNT}} products &nbsp;·&nbsp; Mongo-Batch window: {{WINDOW_MS}}ms</p>
  <div class="chart-wrap">
    <canvas id="chart"></canvas>
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
    new Chart(document.getElementById('chart'), {
      type: 'bar',
      data: {
        labels: ['min', 'mean', 'p50', 'p99', 'p99.9'],
        datasets: [{{DATASETS}}]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top' },
          tooltip: {
            callbacks: {
              label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(1) + ' ms'
            }
          }
        },
        scales: {
          y: {
            beginAtZero: true,
            title: { display: true, text: 'Latency (ms)' }
          }
        }
      }
    });
  </script>
</body>
</html>
""";
}
