#!/usr/bin/env node

/**
 * Usage:
 *   node createStackedBarChart.js path/to/stacked-bar.yaml
 *
 * This script reads a "two-segment stacked bar" YAML config (highest vs. rest)
 * and generates a horizontal stacked bar chart as a PNG.
 * Each team has its own color, with lower opacity for the "highest" segment.
 */

const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');
const echarts = require('echarts');
const { createCanvas } = require('canvas');

/**
 * Convert a "#RRGGBB" hex color to "rgba(r,g,b,<alpha>)".
 * If the input is already "rgba(...)", you can expand
 * this function to parse it or handle it differently.
 */
function applyAlpha(hex, alpha = 1) {
  // Remove leading '#' if present
  const cleanHex = hex.replace(/^#/, '');
  // Parse the R, G, B
  const r = parseInt(cleanHex.substring(0, 2), 16);
  const g = parseInt(cleanHex.substring(2, 4), 16);
  const b = parseInt(cleanHex.substring(4, 6), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

function loadConfig(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  return yaml.load(raw);
}

function createStackedBarChart(config) {
  const title = config.title || 'Stacked Bar Chart';
  const subtitle = config.subtitle || '';

  // X-axis label, Y-axis label
  const xAxisLabel = config.xAxisLabel || 'Goals';
  const yAxisLabel = config.yAxisLabel || 'Teams';

  // teams => array of { name, highest, rest, color }
  const teams = config.teams || [];

  // Extract just the team names for the y-axis categories
  const teamNames = teams.map(t => t.name);

  // Build "Highest" data array, using a lower opacity color
  const highestData = teams.map(t => ({
    value: t.highest || 0,
    itemStyle: {
      color: applyAlpha(t.color || '#999', 0.4) // e.g. 40% opacity
    }
  }));

  // Build "Rest" data array, using full opacity
  const restData = teams.map(t => ({
    value: t.rest || 0,
    itemStyle: {
      color: applyAlpha(t.color || '#999', 1.0) // 100% opacity
    }
  }));

  // Two series, stacked on each other
  const series = [
    {
      name: 'Highest Scorer',
      type: 'bar',
      stack: 'goals',  // same stack key
      data: highestData
    },
    {
      name: 'Rest',
      type: 'bar',
      stack: 'goals',
      data: restData
    }
  ];

  const option = {
    backgroundColor: 'transparent',
    title: {
        show: false,
      text: title,
      subtext: subtitle,
      left: 'center'
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' }
    },
    legend: {
        show: false,
      data: ['Highest Scorer', 'Rest'],
      top: 40
    },
    // For horizontal bars:
    xAxis: {
      type: 'value',
      name: xAxisLabel
    },
    yAxis: {
      type: 'category',
      name: yAxisLabel,
      data: teamNames
    },
    series
  };

  // Render chart in Node using node-canvas
  const dpi = 2
  const width = 1600 * dpi;
  const height = 1080 * dpi;
  const canvas = createCanvas(width, height);
  const chart = echarts.init(canvas);
  chart.setOption(option);

  // Save as PNG
  const buffer = canvas.toBuffer('image/png');
  fs.writeFileSync('stacked-bar-chart.png', buffer);
  console.log('Stacked bar chart saved as stacked-bar-chart.png');
}

// MAIN
(function main() {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.error('Usage: node createStackedBarChart.js <path/to/config.yaml>');
    process.exit(1);
  }

  const configPath = path.resolve(args[0]);
  const config = loadConfig(configPath);
  createStackedBarChart(config);
})();
