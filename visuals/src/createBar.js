#!/usr/bin/env node

/**
 * Usage:
 *   node createBarChart.js path/to/config.yaml
 */

const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');
const echarts = require('echarts');
const { createCanvas } = require('canvas');

function loadBarConfig(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  return yaml.load(raw);
}

function createBarChart(config) {
  const title = config.title || 'Bar Chart';
  const subtitle = config.subtitle || '';

  // Axis labels
  const xAxisLabel = config.xAxisLabel || '';
  const yAxisLabel = config.yAxisLabel || '';

  // Categories and values
  const categories = config.categories || [];
  const values = config.values || [];

  // The color for the bar (all bars share this color)
  // e.g. "rgba(54,162,235,0.8)" or "#FF6384"
  const barColor = config.barColor || '#36A2EB'; // fallback

  // Build the ECharts option
  const option = {
    backgroundColor: 'transparent',
    title: {
      text: title,
      subtext: subtitle,
      left: 'center'
    },
    tooltip: {},
    xAxis: {
      type: 'category',
      name: xAxisLabel,
      data: categories
    },
    yAxis: {
      type: 'value',
      name: yAxisLabel
    },
    series: [
        {
          type: 'bar',
          data: values.map((val, i) => ({
            value: val,
            itemStyle: {
              color: (config.barColors && config.barColors[i]) || '#36A2EB'
            }
          }))
        }
      ]
  };

  // Render with node-canvas
  const width = 800;
  const height = 600;
  const canvas = createCanvas(width, height);
  const chart = echarts.init(canvas);
  chart.setOption(option);

  // Save PNG
  const buffer = canvas.toBuffer('image/png');
  fs.writeFileSync('bar-chart.png', buffer);
  console.log('Bar chart saved as bar-chart.png');
}

// Main
(function main() {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.error('Usage: node createBarChart.js <path/to/config.yaml>');
    process.exit(1);
  }
  const configPath = path.resolve(args[0]);
  const config = loadBarConfig(configPath);
  createBarChart(config);
})();
