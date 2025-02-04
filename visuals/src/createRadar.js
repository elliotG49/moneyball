#!/usr/bin/env node

/**
 * Usage:
 *   node createRadarChart.js path/to/config.yaml
 *
 * This script loads a manual YAML config for a Radar chart
 * and produces a PNG image using ECharts + canvas.
 */

const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');
const echarts = require('echarts');
const { createCanvas } = require('canvas');

function loadRadarConfig(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  const config = yaml.load(raw);
  return config;
}

function createRadarChart(config) {
  // 1) Extract data from config
  const title = config.title || 'Radar Chart';
  const subtitle = config.subtitle || '';

  // "indicators" is an array of { name, min, max }
  const indicators = config.indicators || [];

  // "datasets" is an array of objects, each: 
  // { label, values: [...], lineColor: 'rgba(...)', fillColor: 'rgba(...)' }
  // where lineColor = outline color, fillColor = inside color.

  // 2) Build ECharts option for a radar
  const option = {
    backgroundColor: 'transparent', // PNG with no background color

    title: {
      show: false
    },

    tooltip: {},

    legend: {
      show: false
    },

    radar: {
      indicator: indicators,
      radius: '65%',
      shape: config.radarShape || 'circle',   // or 'polygon'
      splitNumber: 5,

      // Dark gray background for the rings
      splitArea: {
        show: true,
        areaStyle: {
          // An array alternates ring colors. You can also do just one color.
          color: config.ringColors || ['#2b2b2b', '#3a3a3a']
        }
      },
      // The lines dividing the rings
      splitLine: {
        lineStyle: {
          // Subtle lines, you could adjust to match dark style
          color: 'rgba(255,255,255,0.2)',
          width: 1.5, // Slightly thicker for clarity
          
        }
      },
      // The line from center to edges
      axisLine: {
        lineStyle: {
          color: 'rgba(255,255,255,0.3)'
        }
      },
      // The label at the outer edge of each axis
      name: {
        show: true,
        color: '#fff',    // axis label color
        fontSize: 20
      }
    },

    series: [
      {
        type: 'radar',
        // Show all vertices and label each with numeric value
        showAllSymbol: true,
        symbolSize: 10,

        label: {
          show: true,
          // '{c}' = just the numeric value for that dimension
          formatter: '{c}',
          color: '#fff',
          fontSize: 20
        },

        // Map each dataset to ECharts format
        data: config.datasets.map(ds => {
          return {
            name: ds.label,
            value: ds.values,
            lineStyle: {
              color: ds.lineColor || 'rgba(54,162,235,1)',
              width: 2
            },
            areaStyle: {
              color: ds.fillColor || 'rgba(54,162,235,0.3)'
            },
            itemStyle: {
              // The color of the vertex symbol
              color: ds.lineColor || 'rgba(54,162,235,1)'
            }
          };
        })
      }
    ]
  };

  // 3) Render using node-canvas + ECharts
  const dpi = 2
  const width = 1600 * dpi;
  const height = 1080 * dpi;
  const canvas = createCanvas(width, height);
  const chart = echarts.init(canvas);
  chart.setOption(option);

  // 4) Save PNG
  const buffer = canvas.toBuffer('image/png', { compressionLevel: 3 });
  fs.writeFileSync('radar-chart.png', buffer);
  console.log('Radar chart saved as radar-chart.png');
}

// Main
(function main() {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.error('Usage: node createRadarChart.js <path/to/config.yaml>');
    process.exit(1);
  }
  const configPath = path.resolve(args[0]);
  const config = loadRadarConfig(configPath);
  // Optionally verify config.type === 'radar'
  createRadarChart(config);
})();
