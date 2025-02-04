#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');
const echarts = require('echarts');
const { createCanvas } = require('canvas');

function loadPieConfig(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  return yaml.load(raw);
}

function createPieChart(config) {
  const title = config.title || 'Pie Chart';
  const subtitle = config.subtitle || '';

  // data => array of { label, value }
  const data = config.data || [];

  // Transform to ECharts style => { name, value }
  const seriesData = data.map(d => ({
    name: d.label,
    value: d.value
  }));

  const option = {
    backgroundColor: 'transparent',
    title: {
      text: title,
      subtext: subtitle,
      left: 'center'
    },
    tooltip: {
      trigger: 'item'
    },
    legend: {
      // put at bottom or top, personal preference
      bottom: 20
    },
    series: [
      {
        type: 'pie',
        data: seriesData,
        radius: '50%' // or "40%", "70%", etc.
      }
    ]
  };

  // Node-canvas + ECharts
  const width = 800;
  const height = 600;
  const canvas = createCanvas(width, height);
  const chart = echarts.init(canvas);
  chart.setOption(option);

  const buffer = canvas.toBuffer('image/png');
  fs.writeFileSync('pie-chart.png', buffer);
  console.log('Pie chart saved as pie-chart.png');
}

// Main
(function main() {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.error('Usage: node createPieChart.js <path/to/config.yaml>');
    process.exit(1);
  }
  const configPath = path.resolve(args[0]);
  const config = loadPieConfig(configPath);
  createPieChart(config);
})();
