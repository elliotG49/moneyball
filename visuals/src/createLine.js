#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');
const echarts = require('echarts');
const { createCanvas } = require('canvas');

function loadLineConfig(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  return yaml.load(raw);
}

function createLineChart(config) {
  const title = config.title || 'Line Chart';
  const subtitle = config.subtitle || '';

  // e.g. config.xAxis.categories => the x-axis points
  const xAxis = config.xAxis || {};
  const xCategories = xAxis.categories || [];

  // Each dataset => one line
  const datasets = config.datasets || [];

  

  // Build ECharts series
  const series = datasets.map(ds => ({
    name: ds.label,
    type: 'line',
    data: ds.data,
    // Add endLabel for each line
    endLabel: {
      show: false,
      formatter: (params) => {
        // params.seriesName => e.g. "Manchester City"
        // params.value => the final numeric y-value
        return `${params.value}`;
      },
      color: '#FFF',
      fontSize: 8
    }
  }));

  const option = {
    backgroundColor: 'transparent',
    title: {
      show: false,
      text: title,
      subtext: subtitle,
      left: 'center'
    },
    tooltip: {
      trigger: 'axis'
    },
    legend: {
      show: true
    },
    xAxis: {
      type: 'category',
      data: xCategories,
      name: xAxis.label || ''
    },
    yAxis: {
      type: 'value',
      min: 1500
    },
    series
  };
  const dpi = 2
  const width = 800 * dpi;
  const height = 600 * dpi;
  const canvas = createCanvas(width, height);
  const chart = echarts.init(canvas);
  chart.setOption(option);

  const buffer = canvas.toBuffer('image/png');
  fs.writeFileSync('line-chart.png', buffer);
  console.log('Line chart saved as line-chart.png');
}

// Main
(function main() {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.error('Usage: node createLineChart.js <path/to/config.yaml>');
    process.exit(1);
  }
  const configPath = path.resolve(args[0]);
  const config = loadLineConfig(configPath);
  createLineChart(config);
})();
