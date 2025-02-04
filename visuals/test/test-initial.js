// ============================================================================
// 1. REQUIRED LIBRARIES & MONGODB SETUP
// ============================================================================
const { MongoClient } = require('mongodb');
const fs = require('fs');
const { ChartJSNodeCanvas } = require('chartjs-node-canvas');

// MongoDB connection details
const uri = "mongodb://localhost:27017"; // Adjust URI as needed
const databaseName = "footballDB";
const collectionName = "matches";

// Chart dimensions in px
const width = 1920;
const height = 1080;

// Initialize ChartJSNodeCanvas
const chartJSNodeCanvas = new ChartJSNodeCanvas({ width, height });

// ============================================================================
// 2. FETCH DATA FROM MONGODB (REMOVED ELO, JUST CORNERS, SHOTS, YELLOW, GOALS)
// ============================================================================
async function fetchData() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(databaseName);
    const collection = db.collection(collectionName);

    const matches = await collection.find(
      {
        competition_id: 9660,
        homeID: 149,
        awayID: 93,
      },
      {
        projection: {
          team_a_corners: 1,
          team_b_corners: 1,
          team_a_shotsOnTarget: 1,
          team_b_shotsOnTarget: 1,
          team_a_yellow_cards: 1,
          team_b_yellow_cards: 1,
          homeGoalCount: 1,
          awayGoalCount: 1
        },
      }
    ).toArray();

    if (matches.length === 0) {
      console.error("No matches found with the specified filter");
      return null;
    }

    // Just take the first match for demonstration
    const match = matches[0];

    return {
      homeTeam: {
        corners: match.team_a_corners || 0,
        shotsOnTarget: match.team_a_shotsOnTarget || 0,
        yellowCards: match.team_a_yellow_cards || 0,
        goals: match.homeGoalCount || 0
      },
      awayTeam: {
        corners: match.team_b_corners || 0,
        shotsOnTarget: match.team_b_shotsOnTarget || 0,
        yellowCards: match.team_b_yellow_cards || 0,
        goals: match.awayGoalCount || 0
      }
    };
  } catch (err) {
    console.error("Error fetching data", err);
  } finally {
    await client.close();
  }
}

// ============================================================================
// 3. CREATE A RADAR CHART WITH A "KITCHEN SINK" OF CONFIG OPTIONS
// ============================================================================
async function createChart(data) {
  if (!data) {
    console.error("No data available to create the chart");
    return;
  }

  // Dynamic "suggestedMax" based on your data
  const suggestedMax = Math.max(
    data.homeTeam.corners,
    data.awayTeam.corners,
    data.homeTeam.shotsOnTarget,
    data.awayTeam.shotsOnTarget,
    data.homeTeam.yellowCards,
    data.awayTeam.yellowCards,
    data.homeTeam.goals,
    data.awayTeam.goals
  ) + 2; // Add padding so the largest stat isn't at the very edge

  // Here's your "kitchen sink" radar chart config:
  const chartConfig = {
    type: 'radar',
    data: {
      labels: ['Corners', 'Shots on Target', 'Yellow Cards', 'Goals'],
      datasets: [
        {
          label: 'Home Team',
          data: [
            data.homeTeam.corners,
            data.homeTeam.shotsOnTarget,
            data.homeTeam.yellowCards,
            data.homeTeam.goals
          ],
          // ----- COLORS & FILL -----
          backgroundColor: 'rgba(54, 162, 235, 0.2)', // fill color under the line
          borderColor: 'rgba(54, 162, 235, 1)',      // line color
          borderWidth: 2,
          // ----- POINT STYLES -----
          pointBackgroundColor: '#fff',              // inner point color
          pointBorderColor: 'rgba(54, 162, 235, 1)', // point outline
          pointBorderWidth: 2,
          pointRadius: 5,
          // pointStyle can be: 'circle', 'rect', 'rectRounded', 'triangle', 'star', etc.
          pointStyle: 'circle'
        },
        {
          label: 'Away Team',
          data: [
            data.awayTeam.corners,
            data.awayTeam.shotsOnTarget,
            data.awayTeam.yellowCards,
            data.awayTeam.goals
          ],
          backgroundColor: 'rgba(255, 99, 132, 0.2)',
          borderColor: 'rgba(255, 99, 132, 1)',
          borderWidth: 2,
          pointBackgroundColor: '#fff',
          pointBorderColor: 'rgba(255, 99, 132, 1)',
          pointBorderWidth: 2,
          pointRadius: 5,
          pointStyle: 'triangle'
        },
      ],
    },
    options: {
      // ----- BASIC LAYOUT & RESPONSIVENESS -----
      responsive: true,
      // If you're rendering on the server, "maintainAspectRatio" may or may not matter.
      maintainAspectRatio: true,
      // aspectRatio is used when "maintainAspectRatio" is true
      aspectRatio: 1, // makes the canvas a 1:1 square

      // ----- PLUGINS -----
      plugins: {
        // Chart Title
        title: {
          display: true,
          text: 'Match Stats Radar 2',
          color: '#000',
          font: {
            size: 24,
            weight: 'bold'
          },
          padding: {
            top: 10,
            bottom: 20
          }
        },
        // Chart Subtitle
        subtitle: {
          display: true,
          text: 'Home Team vs. Away Team',
          color: '#666',
          font: {
            size: 16,
            style: 'italic'
          },
          padding: {
            bottom: 10
          }
        },
        // Legend (the dataset labels)
        legend: {
          display: true,
          position: 'top', // 'top', 'left', 'bottom', 'right'
          labels: {
            color: '#333', // color for legend text
            boxWidth: 20,  // width of the colored box
            padding: 10,
            font: {
              size: 14
            }
          }
        },
        // Tooltips
        tooltip: {
          enabled: true, // set to false to disable tooltips
          backgroundColor: 'rgba(252, 252, 252, 1)',
          titleColor: '#fff',
          bodyColor: '#fff',
          borderColor: '#aaa',
          borderWidth: 1,
          cornerRadius: 4,
          callbacks: {
            // Customize the text in the tooltip
            label: function(context) {
              const label = context.dataset.label || '';
              const value = context.parsed.r; // "r" is the radial value
              return `${label}: ${value}`;
            }
          }
        }
      },

      // ----- RADAR SCALES (scales.r) -----
      scales: {
        r: {
          // The minimum value for your radial scale
          suggestedMin: 0,
          // The maximum value for your radial scale
          suggestedMax: suggestedMax,

          // Start angle: where "label[0]" is placed in degrees
          // e.g., 0 means corners is at the top
          startAngle: 0, // 0°, 90°, 180°, etc.

          // "ticks" are the numeric rings that show 0, 1, 2, etc.
          ticks: {
            display: true,      // set to false to hide the numeric scale
            color: '#333',      // color of numbers
            backdropColor: '#fff',
            // stepSize: 1,     // force a step of 1
            font: {
              size: 12,
              weight: 'normal'
            }
          },
          // "pointLabels" are the axis labels (Corners, Shots, etc.)
          pointLabels: {
            display: true,      // set to false to hide axis labels
            color: '#111',
            font: {
              size: 14,
              weight: 'bold'
            },
            // padding: 10,     // spacing around the label text
          },
          // Grid lines that go from the center to each label
          angleLines: {
            display: true,
            color: 'rgba(0, 0, 0, 0.2)'  // lines from center outwards
          },
          // Circular grid lines that run around the chart
          grid: {
            display: true,
            color: 'rgba(0, 0, 0, 0.1)'
          }
        }
      },

      // ----- ANIMATION -----
      animation: {
        duration: 2000,           // in ms
        easing: 'easeOutBounce'   // e.g. 'linear', 'easeInQuad', etc.
      },

      // ----- LAYOUT PADDING -----
      layout: {
        padding: {
          top: 10,
          right: 10,
          bottom: 10,
          left: 10
        }
      }
    }
  };

  // ----- RENDER TO BUFFER & SAVE -----
  const imageBuffer = await chartJSNodeCanvas.renderToBuffer(chartConfig);
  fs.writeFileSync('/root/project-barnard/social/charts/test/radar-chart.png', imageBuffer);
  console.log('Radar chart saved as radar-chart.png');
}

// ============================================================================
// 4. MAIN SCRIPT FLOW
// ============================================================================
(async () => {
  const data = await fetchData();
  console.log("Fetched data:", data);
  await createChart(data);
})();
